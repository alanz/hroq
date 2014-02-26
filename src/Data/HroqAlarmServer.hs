{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE DeriveDataTypeable   #-}
{-# LANGUAGE DeriveGeneric        #-}
module Data.HroqAlarmServer
  (
  -- * Starting the server
    hroq_alarm_server
  , hroq_alarm_server_closure

  -- * API
  , check
  , triggers

  -- * Types

  -- * Debug


  -- * Remote Table
  , Data.HroqAlarmServer.__remoteTable

  ) where


import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Serializable()
import Control.Exception hiding (try,catch)
import Data.Binary
import Data.Hroq
import Data.HroqGroups
import Data.HroqLogger
import Data.HroqStatsGatherer
import Data.Time.Calendar
import Data.Time.Clock
import Data.Typeable (Typeable)
import GHC.Generics
import System.Environment
import Text.Regex.Base
import Text.Regex.TDFA
import qualified Data.Map as Map
import qualified Data.Set as Set

--------------------------------------------------------------------------------
-- Types                                                                      --
--------------------------------------------------------------------------------

data AlarmTrigger = AT TriggerType QName TriggerParams
  deriving (Typeable, Generic, Eq, Show)
instance Binary AlarmTrigger where

data TriggerType = TriggerQueue
  deriving (Typeable, Generic, Eq, Show)
instance Binary TriggerType where

data TriggerParams = TriggerSize Integer Integer
                   | TriggerStatic Integer NominalDiffTime
  deriving (Typeable, Generic, Eq, Show)
instance Binary TriggerParams where

data Alarms = A (Map.Map String Integer)
  deriving  (Typeable, Generic, Eq, Show)
instance Binary Alarms where

-- Call and Cast request types.

data Check = Check
  deriving (Typeable, Generic, Eq, Show)
instance Binary Check where

data Triggers = Triggers
  deriving (Typeable, Generic, Eq, Show)
instance Binary Triggers where

-- ---------------------------------------------------------------------

data State = ST { stLastCheck :: UTCTime
                , stStatsHistory :: StatsDict
                , stCallbackFun :: Alarms -> Process ()
                , stTriggers :: [AlarmTrigger]
                }

-- | For each queue, keep track of /* last size, */ dequeueCount and timestamp
-- type StatsDict = Map.Map QName (Integer,Integer,UTCTime)
type StatsDict = Map.Map QName (Integer,UTCTime)

-- | appinfo,qname, qsize,dequeueCount
type Internal = (String,QName,Integer,Integer)

noopFun _ = return ()

emptyState :: State
emptyState = ST (UTCTime (fromGregorian 1900 0 0) 0) Map.empty noopFun []

-- ---------------------------------------------------------------------

-- -define(MONITOR_INTERVAL_MS, eroq_util:app_param(alarm_monitor_interval_ms, 30000)).
mONITOR_INTERVAL_MS :: Delay
mONITOR_INTERVAL_MS = Delay $ milliSeconds 30000

{-
-define(EXAMPLE_QUEUE_ALARM_CONFIG, [

               {"MAIN_QUEUE_STATIC",   ["MAIN"], {include, ""},  [{static,     60}]}, %Queue can't be static for longer than 1 minutes
               {"CONS_DLQ",            ["DLQ"],  {include, ""},  [{max_size,   0}]}

               ]).

-}
-- TODO: look at getting this via dyre http://hackage.haskell.org/package/dyre-0.8.2/docs/Config-Dyre.html
eXAMPLE_QUEUE_ALARM_CONFIG =
  [
    QAC "MAIN_QUEUE_STATIC" (Set.fromList ["MAIN"]) (Include "") [(P Static  60)]
  , QAC "MAIN_QUEUE_STATIC" (Set.fromList ["DLQ"])  (Include "") [(P MaxSize 0)]
  ]

data QueueAlarmConfig = QAC { acName :: String
                            , acQTypes :: Set.Set String
                            , acExclusion :: Constraint
                            , acParams :: [Param]
                            }

data Constraint = Include String
                | Exclude String

data Param = P ParamType Integer

data ParamType = Static | MaxSize
               deriving (Eq,Show)

-- ---------------------------------------------------------------------

hroq_alarm_server :: Process ()
hroq_alarm_server = start_alarm_server

--------------------------------------------------------------------------------
-- API                                                                        --
--------------------------------------------------------------------------------

-- -------------------------------------
{-
-spec check() -> ok.
check() ->
    gen_server:call(?MODULE, check, infinity).
-}

check :: Process Delay
check = do
  pid <- getServerPid
  call pid Check

-- -------------------------------------
{-
-spec triggers() -> {ok, alarm_trigger_list()}.
triggers() ->
    gen_server:call(?MODULE, triggers, infinity).
-}
triggers :: Process [AlarmTrigger]
triggers = do
  pid <- getServerPid
  call pid Triggers


-- ---------------------------------------------------------------------

getServerPid :: Process ProcessId
getServerPid = do
  mpid <- whereis hroqAlarmSrverProcessName
  case mpid of
    Just pid -> return pid
    Nothing -> do
      logm "HroqAlarmServer:getServerPid failed"
      error "HroqAlarmServer:blow up"

hroqAlarmSrverProcessName :: String
hroqAlarmSrverProcessName = "HroqAlarmServer"

-- -------------------------------------
{-
-spec start_link(alarm_callback_fun()) -> {ok, pid()}.
start_link(CallbackFun) when is_function(CallbackFun,1) ->
    ?info(starting),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [CallbackFun], []).


init([CallbackFun]) ->
    State = #eroq_alarm_server_state {
        callback_fun = CallbackFun
    },
    {ok, State, ?MONITOR_INTERVAL_MS}.
-}

start_alarm_server :: Process ()
start_alarm_server = do
  self <- getSelfPid
  register hroqAlarmSrverProcessName self
  serve 0 initFunc serverDefinition
  where initFunc :: InitHandler Integer State
        initFunc i = do
          logm $ "HroqAlarmServer:start.initFunc"
          return $ InitOk emptyState mONITOR_INTERVAL_MS

serverDefinition :: ProcessDefinition State
serverDefinition = defaultProcess {
     apiHandlers = [
          handleCall handleCheckCall
        , handleCall handleTriggersCall

        ]
    , infoHandlers =
        [
         -- handleInfo handleInfoProcessMonitorNotification
        ]
     -- , timeoutHandler = \_ _ -> do
     --       {logm "HroqStatsGatherer:timout exit"; stop $ ExitOther "timeout az"}
     , timeoutHandler = handleTimeout
     , shutdownHandler = \_ reason -> do
           { logm $ "HroqAlarmServer terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State

-- ---------------------------------------------------------------------
-- Implementation
-- ---------------------------------------------------------------------

-- ---------------------------------------------------------------------

{-
handle_call(check, _From, State) ->
    {NewState, SleepMs} = do_safe_alarm_processing(State, true),
    {reply, ok, NewState, SleepMs};
-}
handleCheckCall :: State -> Check -> Process (ProcessReply Delay State)
handleCheckCall st Check = do
    logm $ "handleCheckCall called with:" ++ (show (Check))

    (st',res) <- do_safe_alarm_processing st True

    reply res st'


-- ---------------------------------------------------------------------
{-
handle_call(triggers, _From, #eroq_alarm_server_state{triggers = Triggers} = State) ->
    {reply, {ok, Triggers}, State, 0};
-}
handleTriggersCall :: State -> Triggers -> Process (ProcessReply [AlarmTrigger] State)
handleTriggersCall st@(ST {stTriggers = trigs}) _ts = do
    logm $ "handleTriggerCall called with:" ++ (show (Check))
    reply trigs st

-- ---------------------------------------------------------------------
{-
handle_call(_, _From, State) ->
    {noreply, State, 0}.

handle_cast(_, State) ->
    {noreply, State, 0}.
-}

-- ---------------------------------------------------------------------
{-
handle_info(timeout, State) ->
    {NewState, SleepMs} = do_safe_alarm_processing(State, false),
    {noreply, NewState, SleepMs};
-}

-- timeoutAfter :: TimeInterval -> s -> Process (ProcessAction s)

handleTimeout :: TimeoutHandler State
handleTimeout st _currDelay = do
  (st',sleepMs) <- do_safe_alarm_processing st False
  timeoutAfter sleepMs st'

-- ---------------------------------------------------------------------
{-
handle_info(_, State) ->
    {noreply, State, 0}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, _State) ->
    ?info({terminate, Reason}).
-}

-- ---------------------------------------------------------------------
{-
do_safe_alarm_processing(State, Force) ->
    Now = now(),
    AgeMs = trunc(timer:now_diff(Now, State#eroq_alarm_server_state.last_check)/1000),
    MonitorIntervalMs = ?MONITOR_INTERVAL_MS,
    if ((Force == true) or (AgeMs >= MonitorIntervalMs)) ->
        NewState =
        case catch(do_alarm_processing(State)) of
        {ok, NS} ->
            NS#eroq_alarm_server_state{last_check = Now};
        Error ->
            ?warn({alarm_processing_error, Error}),
            State#eroq_alarm_server_state{last_check = Now}
        end,
        {NewState, MonitorIntervalMs};
    true ->
        {State, MonitorIntervalMs - AgeMs}
    end.
-}

do_safe_alarm_processing :: State -> Bool -> Process (State, Delay)
do_safe_alarm_processing st force = do
  logm $ "do_safe_alarm_processing:force=" ++ show force
  now <- liftIO $ getCurrentTime
  let ageMs = diffUTCTime now (stLastCheck st)
  if force == True || ageMs > delayToDiffTime mONITOR_INTERVAL_MS
    then do
      result <- do_alarm_processing st
      case result of
        Right ns -> return (ns { stLastCheck = now }, mONITOR_INTERVAL_MS)
        Left err -> do
          logm $ "HroqAlarmServer.do_safe_alarm_processing:got err:" ++ show err
          return (st,mONITOR_INTERVAL_MS)
    else return (st,mONITOR_INTERVAL_MS - diffTimeToDelay ageMs)

-- ---------------------------------------------------------------------
{-
do_alarm_processing(State)->

    QueueStats = traverse_get_queue_stats(eroq_groups:queues(), []),

    AlarmConfig =
    case application:get_env(mira_eroq, alarm_config) of
    {ok, Acfg} ->
        Acfg;
    _ ->
        ?EXAMPLE_QUEUE_ALARM_CONFIG
    end,

    StatsDict   = State#eroq_alarm_server_state.stats_history,
    Fun         = State#eroq_alarm_server_state.callback_fun,

    {NewStatsDict, AlarmList, AlarmTiggers} = traverse_process_all_queues(StatsDict, [], [], QueueStats, AlarmConfig),

    NewState = State#eroq_alarm_server_state {stats_history = NewStatsDict, triggers = AlarmTiggers},

    catch(Fun([AlarmList])),

    {ok, NewState}.
-}

do_alarm_processing :: State -> Process (Either String State)
do_alarm_processing st = do
  logm $ "do_alarm_processing entered"

  qs <- queues
  queueStats <- traverse_get_queue_stats qs []
  -- TODO: use something like dyre to look this up
  mAcfg <- liftIO $ lookupEnv "alarm_config"
  let alarmConfig = case mAcfg of
        Just cfg -> -- cfg
                   eXAMPLE_QUEUE_ALARM_CONFIG
        Nothing -> eXAMPLE_QUEUE_ALARM_CONFIG

  let statsDict = stStatsHistory st
      fun       = stCallbackFun st

  (newStatsDict,alarmList,alarmTriggers)
     <- traverse_process_all_queues statsDict (A Map.empty) [] queueStats alarmConfig

  let newState = st {stStatsHistory = newStatsDict, stTriggers = alarmTriggers }

  let handler :: SomeException -> Process ()
      handler e = do
        logm $ "HroqAlarmServer:fun handler got error:" ++ show e
        return ()

  catch (fun alarmList) handler

  return $ Right newState

-- ---------------------------------------------------------------------
{-
traverse_process_all_queues(StatsDict, AlarmList, AlarmTriggers,  [{AppInfo, QueueName, QueueSize, DeqCount} | T], AlarmConfig)->
    {NewStatsDict, NewAlarmList, NewAlarmTriggers} = determine_alarms(AppInfo, QueueName, QueueSize, DeqCount, AlarmConfig, StatsDict, AlarmList, AlarmTriggers),
    traverse_process_all_queues(NewStatsDict, NewAlarmList, NewAlarmTriggers, T, AlarmConfig);
traverse_process_all_queues(StatsDict, AlarmList, AlarmTriggers, [], _)->
    {StatsDict, AlarmList, AlarmTriggers}.
-}
traverse_process_all_queues
  :: StatsDict -- statsDict
  -> Alarms -- alarmList
  -> [AlarmTrigger] -- alarmTriggers
  -> [Internal]
  -> [QueueAlarmConfig]
  -> Process (StatsDict,Alarms,[AlarmTrigger])
traverse_process_all_queues statsDict alarmList alarmTriggers (h:t) alarmConfig = do
  let (appInfo,queueName,queueSize,deqCount) = h
  (newStatsDict,newAlarmList,newAlarmTriggers)
     <- determine_alarms appInfo queueName queueSize deqCount alarmConfig statsDict alarmList alarmTriggers
  traverse_process_all_queues newStatsDict newAlarmList newAlarmTriggers t alarmConfig
traverse_process_all_queues statsDict alarmList alarmTriggers [] _ = do
  return (statsDict,alarmList,alarmTriggers)

-- ---------------------------------------------------------------------
{-
traverse_get_queue_stats([QueueName | T], Acc0)->
    Acc1 =
    case catch(eroq_stats_gatherer:get_queue_stats(QueueName)) of
    {ok, {AppInfo, QueueSize, _, DeqCount}} ->
        [{AppInfo, atom_to_list(QueueName), QueueSize, DeqCount} | Acc0];
    _ ->
        Acc0
    end,
    traverse_get_queue_stats(T, Acc1);
traverse_get_queue_stats([], Acc)-> Acc.
-}

traverse_get_queue_stats :: [QName] -> [Internal] -> Process [Internal]
traverse_get_queue_stats (queueName:t) acc0 = do
  let handler :: SomeException -> Process [Internal]
      handler _ = return acc0

      getter :: QName -> Process [(String,QName,Integer,Integer)]
      getter q = do
        ReplyQStats (QStats appInfo queueSize _ deqCount) <- get_queue_stats q
        return $ (appInfo,queueName, queueSize, deqCount) : acc0

  acc1 <- catch (getter queueName) handler
  traverse_get_queue_stats t acc1
traverse_get_queue_stats [] acc = return acc

-- ---------------------------------------------------------------------
{-
get_alarm_param(ParamName, AlarmParams)->
    case lists:keysearch(ParamName, 1, AlarmParams) of
    {value, {_, Value}}->
        {ok, Value};
    _ ->
        error
    end.
-}

get_alarm_param :: ParamType -> [Param] -> Maybe Integer
get_alarm_param paramName alarmParams =
  case (filter (\(P pt _) -> pt == paramName) alarmParams) of
    [] -> Nothing
    (P _ v:_) -> Just v

-- ---------------------------------------------------------------------
{-
is_excluded(StringQueueName, {include, Regex})->
    case re:run(StringQueueName, Regex, [{capture, first}]) of
    {match, _} ->
        false;
    _ ->
        true
    end;
is_excluded(StringQueueName, {exclude, Regex})->
    case re:run(StringQueueName, Regex, [{capture, first}]) of
    {match, _} ->
        true;
    _ ->
        false
    end.
-}

-- TODO: compile the regex on startup
is_excluded :: QName -> Constraint -> Bool
is_excluded (QN stringQueueName) (Include regex) =
  not $ matchTest (mkr regex) stringQueueName
is_excluded (QN stringQueueName) (Exclude regex) =
        matchTest (mkr regex) stringQueueName

mkr :: String -> Regex
mkr reg = makeRegex reg

-- ---------------------------------------------------------------------
{-
get_alarm_count(AlarmName, AlarmList)->
    case lists:keysearch(AlarmName, 1, AlarmList) of
    {value, {_, Count}}->
        Count;
    _ ->
        0
    end.
-}

get_alarm_count :: String -> Alarms -> Integer
get_alarm_count alarmName (A alarmList)
  = Map.findWithDefault 0 alarmName alarmList

-- ---------------------------------------------------------------------
{-
inc_alarm_count(AlarmName, AlarmList)->
    Count = get_alarm_count(AlarmName, AlarmList),
    case lists:keyreplace(AlarmName, 1, AlarmList, {AlarmName, Count + 1}) of
    AlarmList ->
        [{AlarmName, Count + 1} | AlarmList];
    NewAlarmList ->
        NewAlarmList
    end.
-}

inc_alarm_count :: String -> Alarms -> Alarms
inc_alarm_count name (A alarms) =
  case Map.lookup name alarms of
    Just v  -> A $ Map.insert name (v+1) alarms
    Nothing -> A $ Map.insert name 1 alarms

-- ---------------------------------------------------------------------
{-
get_last_stats(StringQueueName, DequeueCount, StatsDict)->
    case dict:find({queue, StringQueueName}, StatsDict) of
    {ok, LastStats}->
        LastStats;
    _ ->
        {DequeueCount, now()}
    end.
-}

get_last_stats :: QName -> Integer -> StatsDict -> Process (Integer,UTCTime)
get_last_stats stringQueueName dequeueCount statsDict = do
  now <- liftIO $ getCurrentTime
  return $ Map.findWithDefault (dequeueCount,now) stringQueueName statsDict

-- ---------------------------------------------------------------------
{-
determine_alarms(QueueType, StringQueueName, QueueSize, DequeueCount, [{AlarmName, QueueTypeList, ExclusionRegex, AlarmParams} | T], StatsDict, AlarmList, AlarmTriggers)->

    QueueTypeMatch = lists:member(QueueType, QueueTypeList),

    {NewStatsDict, NewAlarmList, NewAlarmTriggers} =
    if QueueTypeMatch == true ->
        Excluded = is_excluded(StringQueueName, ExclusionRegex),
        if Excluded == false ->

            {MaxSizeAlarmList, MaxSizeAlarmTriggers} =
            case get_alarm_param(max_size, AlarmParams) of
            {ok, MaxSize} when (QueueSize > MaxSize) ->
                {inc_alarm_count(AlarmName, AlarmList), [{queue, StringQueueName, max_size, MaxSize, QueueSize}  | AlarmTriggers]};
            _ ->
                {AlarmList, AlarmTriggers}
            end,

            case get_alarm_param(static, AlarmParams) of
            {ok, MaxStaticTimeSecs} ->
                {LastDeqCount, LastChangeTimestamp} = get_last_stats(StringQueueName, DequeueCount, StatsDict),
                if ((LastDeqCount == DequeueCount) and (QueueSize > 0)) ->
                    QueueStaticAgeSecs = trunc(timer:now_diff(now(), LastChangeTimestamp) / 1000000),
                    {StaticAlarmList, StaticAlarmTriggers} =
                    if ((QueueSize > 0) and (LastDeqCount == DequeueCount) and (QueueStaticAgeSecs > MaxStaticTimeSecs)) ->
                        {inc_alarm_count(AlarmName, MaxSizeAlarmList), [{queue, StringQueueName, static, MaxStaticTimeSecs, QueueStaticAgeSecs} | MaxSizeAlarmTriggers]};
                    true ->
                        {MaxSizeAlarmList, MaxSizeAlarmTriggers}
                    end,
                    {dict:store({queue, StringQueueName}, {LastDeqCount, LastChangeTimestamp}, StatsDict), StaticAlarmList, StaticAlarmTriggers};
                true ->
                    {dict:store({queue, StringQueueName}, {DequeueCount, now()}, StatsDict), MaxSizeAlarmList, MaxSizeAlarmTriggers}
                end;
            _ ->
                {StatsDict, MaxSizeAlarmList, MaxSizeAlarmTriggers}
            end;

        true ->
            {StatsDict, AlarmList, AlarmTriggers}
        end;
    true ->
        {StatsDict, AlarmList, AlarmTriggers}
    end,

    determine_alarms(QueueType, StringQueueName, QueueSize, DequeueCount, T, NewStatsDict, NewAlarmList, NewAlarmTriggers);
determine_alarms(_, _, _, _, [], NewStatsDict, NewAlarmList, NewAlarmTriggers)->
    {NewStatsDict, NewAlarmList, NewAlarmTriggers}.
-}

determine_alarms
  :: String
  -> QName
  -> Integer
  -> Integer
  -> [QueueAlarmConfig]
  -> StatsDict
  -> Alarms
  -> [AlarmTrigger]
  -> Process (StatsDict, Alarms, [AlarmTrigger])
determine_alarms queueType stringQueueName queueSize dequeueCount (h:t) statsDict alarmList alarmTriggers = do
  let (QAC alarmName queueTypeList exclusionRegex alarmParams) = h

  --  QueueTypeMatch = lists:member(QueueType, QueueTypeList),
  let queueTypeMatch = Set.member queueType queueTypeList
  now <- liftIO getCurrentTime

  (newStatsDict,newAlarmList,newAlarmTriggers) <-
    if queueTypeMatch
      then do
        let excluded = is_excluded stringQueueName exclusionRegex
        if excluded == False
          then do

            (maxSizeAlarmList,maxSizeAlarmTriggers) <-
               case get_alarm_param MaxSize alarmParams of
                 Just maxSize -> do
                   if queueSize > maxSize
                     then return (inc_alarm_count alarmName alarmList,(AT TriggerQueue stringQueueName (TriggerSize maxSize queueSize)):alarmTriggers)
                     else return (alarmList,alarmTriggers)
                 Nothing -> return (alarmList,alarmTriggers)

            case get_alarm_param Static alarmParams of
              Just maxStaticTimeSecs -> do
                (lastDeqCount,lastChangeTimestamp) <- get_last_stats stringQueueName dequeueCount statsDict

                if (lastDeqCount == dequeueCount) && (queueSize > 0)
                  then do
                    let queueStaticAgeSecs = diffUTCTime now lastChangeTimestamp
                    (staticAlarmList,staticAlarmTriggers) <- do
                      if (queueSize > 0) && (lastDeqCount == dequeueCount) && (queueStaticAgeSecs > (timeIntervalToDiffTime $ seconds $ fromIntegral maxStaticTimeSecs)) 
                        then return (inc_alarm_count alarmName maxSizeAlarmList, (AT TriggerQueue stringQueueName (TriggerStatic maxStaticTimeSecs queueStaticAgeSecs)):maxSizeAlarmTriggers)
                        else return (maxSizeAlarmList,maxSizeAlarmTriggers)

                    return (Map.insert stringQueueName ({- lastQueueSize,-} lastDeqCount,lastChangeTimestamp) statsDict, staticAlarmList, staticAlarmTriggers)

                  else
                     return (Map.insert stringQueueName ({- queueSize, -}dequeueCount, now) statsDict, maxSizeAlarmList, maxSizeAlarmTriggers)

              Nothing ->
                    return (statsDict, maxSizeAlarmList, maxSizeAlarmTriggers)

          else return (statsDict,alarmList, alarmTriggers)
      else return (statsDict, alarmList,alarmTriggers)

  determine_alarms queueType stringQueueName queueSize dequeueCount t newStatsDict newAlarmList newAlarmTriggers

determine_alarms _  _  _  _  [] newStatsDict newAlarmList newAlarmTriggers
  = return (newStatsDict,newAlarmList,newAlarmTriggers)

-- ---------------------------------------------------------------------
{-
%EOF

-}

-- ---------------------------------------------------------------------

-- NOTE: the TH crap has to be a the end, as it can only see the stuff lexically before it in the file

$(remotable [ 'hroq_alarm_server
            ])

hroq_alarm_server_closure :: Closure (Process ())
hroq_alarm_server_closure = ($(mkStaticClosure 'hroq_alarm_server))


