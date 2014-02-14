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
  , __remoteTable

  ) where


import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Platform hiding (__remoteTable,monitor)
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Serializable()
import Data.Binary
import Data.Typeable (Typeable)
import qualified Data.Map as Map
import Data.Hroq
import Data.HroqLogger
import GHC.Generics


--------------------------------------------------------------------------------
-- Types                                                                      --
--------------------------------------------------------------------------------

data AlarmTrigger = AlarmTrigger
  deriving (Typeable, Generic, Eq, Show)
instance Binary AlarmTrigger where

-- Call and Cast request types.

data Check = Check
  deriving (Typeable, Generic, Eq, Show)
instance Binary Check where

data Triggers = Triggers
  deriving (Typeable, Generic, Eq, Show)
instance Binary Triggers where

-- ---------------------------------------------------------------------

data State = ST [AlarmTrigger]

emptyState :: State
emptyState = ST []

-- ---------------------------------------------------------------------

-- -define(MONITOR_INTERVAL_MS, eroq_util:app_param(alarm_monitor_interval_ms, 30000)).
mONITOR_INTERVAL_MS :: Delay
mONITOR_INTERVAL_MS = Delay $ milliSeconds 3000

{-
-define(EXAMPLE_QUEUE_ALARM_CONFIG, [

               {"MAIN_QUEUE_STATIC",   ["MAIN"], {include, ""},  [{static,     60}]}, %Queue can't be static for longer than 1 minutes
               {"CONS_DLQ",            ["DLQ"],  {include, ""},  [{max_size,   0}]}

               ]).

-}

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

check :: Process ()
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
  call pid Check


-- ---------------------------------------------------------------------

getServerPid :: Process ProcessId
getServerPid = do
  mpid <- whereis hroqAlarmSrverProcessName
  case mpid of
    Just pid -> return pid
    Nothing -> do
      logm "HroqAlarmServer:getServerPid failed"
      error "blow up"

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
           { logm $ "HroqStatsGatherer terminateHandler:" ++ (show reason) }
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
handleCheckCall :: State -> Check -> Process (ProcessReply () State)
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
handleTriggersCall st@(ST ts) Triggers = do
    logm $ "handleTriggerCall called with:" ++ (show (Check))
    reply ts st

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

-- handleTimeout :: State -> Process (ProcessAction State)
handleTimeout :: TimeoutHandler State
handleTimeout st currDelay = do
  (st',sleepMs) <- do_safe_alarm_processing st False
  timeoutAfter (milliSeconds 3000) st

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

do_safe_alarm_processing :: State -> Bool -> Process (State, ())
do_safe_alarm_processing st force = do
  logm $ "do_safe_alarm_processing undefined"
  error $ "do_safe_alarm_processing undefined"

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

traverse_process_all_queues(StatsDict, AlarmList, AlarmTriggers,  [{AppInfo, QueueName, QueueSize, DeqCount} | T], AlarmConfig)->
    {NewStatsDict, NewAlarmList, NewAlarmTriggers} = determine_alarms(AppInfo, QueueName, QueueSize, DeqCount, AlarmConfig, StatsDict, AlarmList, AlarmTriggers),
    traverse_process_all_queues(NewStatsDict, NewAlarmList, NewAlarmTriggers, T, AlarmConfig);
traverse_process_all_queues(StatsDict, AlarmList, AlarmTriggers, [], _)->
    {StatsDict, AlarmList, AlarmTriggers}.

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

get_alarm_param(ParamName, AlarmParams)->
    case lists:keysearch(ParamName, 1, AlarmParams) of
    {value, {_, Value}}->
        {ok, Value};
    _ ->
        error
    end.

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

get_alarm_count(AlarmName, AlarmList)->
    case lists:keysearch(AlarmName, 1, AlarmList) of
    {value, {_, Count}}->
        Count;
    _ ->
        0
    end.

inc_alarm_count(AlarmName, AlarmList)->
    Count = get_alarm_count(AlarmName, AlarmList),
    case lists:keyreplace(AlarmName, 1, AlarmList, {AlarmName, Count + 1}) of
    AlarmList ->
        [{AlarmName, Count + 1} | AlarmList];
    NewAlarmList ->
        NewAlarmList
    end.

get_last_stats(StringQueueName, DequeueCount, StatsDict)->
    case dict:find({queue, StringQueueName}, StatsDict) of
    {ok, LastStats}->
        LastStats;
    _ ->
        {DequeueCount, now()}
    end.

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

%EOF

-}

-- ---------------------------------------------------------------------

-- NOTE: the TH crap has to be a the end, as it can only see the stuff lexically before it in the file

$(remotable [ 'hroq_alarm_server
            ])

hroq_alarm_server_closure :: Closure (Process ())
hroq_alarm_server_closure = ($(mkStaticClosure 'hroq_alarm_server))


