{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE DeriveDataTypeable   #-}
{-# LANGUAGE DeriveGeneric        #-}
module Data.HroqStatsGatherer
  (
  -- * Starting the server
    hroq_stats_gatherer
  , hroq_stats_gatherer_closure
  , hroq_stats_gatherer_pid

  -- * API
  , publish_queue_stats
  , publish_consumer_stats
  , get_queue_stats
  , get_consumer_stats

  -- * Types
  , QStats(..)
  , GetQueueStatsReply (..)
  , GetConsumerStatsReply (..)

  -- * Debug
  , ping

  -- * Remote Table
  , __remoteTable

  )
  where

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

-- Call and Cast request types.

-- {AppInfo, NewTotalQueuedMsg, EnqueueCount, NewDequeueCount}
data QStats = QStats { qstatsAppInfo         :: String
                     , qstatsNewTotalQueued  :: Integer
                     , qstatsEnqueueCount    :: Integer
                     , qstatsNewDequeueCount :: Integer
                     }
  deriving (Typeable, Generic, Eq, Show)
instance Binary QStats where

-- Call operations
data GetQueueStats = GetQueueStats QName
  deriving (Typeable, Generic, Eq, Show)
instance Binary GetQueueStats where

data GetQueueStatsReply = ReplyQStats QStats | ReplyQStatsNotFound
  deriving (Typeable, Generic, Eq, Show)
instance Binary GetQueueStatsReply where

--

data GetConsumerStats = GetConsumerStats ConsumerName
  deriving (Typeable, Generic, Eq, Show)
instance Binary GetConsumerStats where

data GetConsumerStatsReply = ReplyCStats QStats | ReplyCStatsNotFound
  deriving (Typeable, Generic, Eq, Show)
instance Binary GetConsumerStatsReply where

-- Cast operations

data PublishQueueStats = PublishQueueStats QName QStats ProcessId
  deriving (Typeable, Generic, Eq, Show)
instance Binary PublishQueueStats where


data PublishConsumerStats = PublishConsumerStats ConsumerName QStats ProcessId
  deriving (Typeable, Generic, Eq, Show)
instance Binary PublishConsumerStats where


data Ping = Ping
  deriving (Typeable, Generic, Eq, Show)
instance Binary Ping where

-- ---------------------------------------------------------------------

-- -record(state,  {qdict = dict:new(), cdict = dict:new(), pdict = dict:new()}).
data State = ST { stQdict :: Map.Map QName        QStats
                , stCdict :: Map.Map ConsumerName QStats
                , stPdict :: Map.Map ProcessId    StatsType
                }
emptyState :: State
emptyState = ST Map.empty Map.empty Map.empty

data StatsType = StatsQueue QName | StatsConsumer ConsumerName

-- ---------------------------------------------------------------------

hroq_stats_gatherer :: Process ()
hroq_stats_gatherer = start_stats_gatherer

hroq_stats_gatherer_pid :: Process ProcessId
hroq_stats_gatherer_pid = getServerPid

--------------------------------------------------------------------------------
-- API                                                                        --
--------------------------------------------------------------------------------

-- -------------------------------------
{-
publish_queue_stats(QueueName, Stats)->
    gen_server:cast(?MODULE, {publish_queue_stats, QueueName, self(), Stats}).
-}
publish_queue_stats :: ProcessId -> QName -> QStats -> Process ()
publish_queue_stats pid qname stats = do
  -- pid <- getServerPid
  sid <- getSelfPid
  cast pid (PublishQueueStats qname stats sid)

-- -------------------------------------
{-
publish_consumer_stats(ConsName, Stats)->
    gen_server:cast(?MODULE, {publish_consumer_stats, ConsName, self(), Stats}).
-}
publish_consumer_stats :: ProcessId -> ConsumerName -> QStats -> Process ()
publish_consumer_stats pid cname stats = do
  -- pid <- getServerPid
  sid <- getSelfPid
  cast pid (PublishConsumerStats cname stats sid)

-- -------------------------------------
{-
get_queue_stats(QueueName) ->
    gen_server:call(?MODULE, {get_queue_stats, QueueName}, infinity).
-}
get_queue_stats :: ProcessId -> QName -> Process GetQueueStatsReply
get_queue_stats pid qname = do
  -- pid <- getServerPid
  call pid (GetQueueStats qname)

-- -------------------------------------
{-
get_consumer_stats(ConsName) ->
    gen_server:call(?MODULE, {get_consumer_stats, ConsName}, infinity).
-}
get_consumer_stats :: ProcessId -> ConsumerName -> Process GetConsumerStatsReply
get_consumer_stats pid cname = do
  -- pid <- getServerPid
  call pid (GetConsumerStats cname)

-- -------------------------------------

ping :: Process ()
ping = do
  pid <- getServerPid
  cast pid Ping


-- ---------------------------------------------------------------------

getServerPid :: Process ProcessId
getServerPid = do
  mpid <- whereis hroqStatsGathererProcessName
  case mpid of
    Just pid -> return pid
    Nothing -> do
      logm "HroqStatsGatherer:getServerPid failed"
      error "HroqStatsGatherer:blow up"

hroqStatsGathererProcessName :: String
hroqStatsGathererProcessName = "HroqStatsGatherer"

-- ---------------------------------------------------------------------
{-

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init_state() -> #state{}.

init(_) ->

    process_flag(trap_exit, true),

    {ok, init_state()}.

-}
start_stats_gatherer :: Process ()
start_stats_gatherer = do
  self <- getSelfPid
  register hroqStatsGathererProcessName self
  serve 0 initFunc serverDefinition
  where initFunc :: InitHandler Integer State
        initFunc i = do
          logm $ "HroqStatsGatherer:start.initFunc"
          return $ InitOk emptyState Infinity

serverDefinition :: ProcessDefinition State
serverDefinition = defaultProcess {
     apiHandlers = [
          handleCall handleGetQueueStatsCall
        , handleCall handleGetConsumerStatsCall

        , handleCast handlePublishQueueStatsCast
        , handleCast handlePublishConsumerStatsCast
        , handleCast (\s Ping -> do {logm $ "HroqStatsGatherer:ping"; continue s })

        ]
    , infoHandlers =
        [
         handleInfo handleInfoProcessMonitorNotification
        ]
     , timeoutHandler = \_ _ -> do
           {logm "HroqStatsGatherer:timout exit"; stop $ ExitOther "timeout az"}
     , shutdownHandler = \_ reason -> do
           { logm $ "HroqStatsGatherer terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State

-- ---------------------------------------------------------------------
-- Implementation
-- ---------------------------------------------------------------------

-- ---------------------------------------------------------------------

{-
handle_call({get_queue_stats, QueueName}, _From, #state{qdict = Qd} = State) ->

    Res =
    case dict:find(QueueName, Qd) of
    {ok, Stats} ->
        {ok, Stats};

    _ ->
        {error, not_found}
    end,

    {reply, Res, State};
-}

handleGetQueueStatsCall ::
      State -> GetQueueStats
   -> Process (ProcessReply GetQueueStatsReply State)
handleGetQueueStatsCall st@(ST {stQdict = qd}) (GetQueueStats qname) = do
    -- logm $ "HroqStatsGatherer.handleGetQueueStatsCall called with:" ++ (show (qname))

    let res = case Map.lookup qname qd of
          Just stats -> ReplyQStats stats
          Nothing -> ReplyQStatsNotFound

    reply res st

-- ---------------------------------------------------------------------

{-
handle_call({get_consumer_stats, ConsName}, _From, #state{cdict = Cd} = State) ->

    Res =
    case dict:find(ConsName, Cd) of
    {ok, Stats} ->
        {ok, Stats};

    _ ->
        {error, not_found}
    end,

    {reply, Res, State};
-}

handleGetConsumerStatsCall ::
       State -> GetConsumerStats
    -> Process (ProcessReply GetConsumerStatsReply State)
handleGetConsumerStatsCall st@(ST {stCdict = cd}) (GetConsumerStats cname) = do
    -- logm $ "HroqStatsGatherer.handleGetConsumerStatsCall called with:" ++ (show (cname))

    let res = case Map.lookup cname cd of
          Just stats -> ReplyCStats stats
          Nothing -> ReplyCStatsNotFound

    reply res st


-- ---------------------------------------------------------------------

{-
handle_call(Msg, _, State) ->
    ?warn({ handle_call, [Msg]}),
    {reply, error, State}.
-}

-- -------------------------------------------------------------------------------------------------

{-
handle_cast({publish_queue_stats, QueueName, Pid, Stats}, #state{qdict = Qd, pdict = Pd} = State) ->

    NQd = dict:store(QueueName, Stats, Qd),

    NPd =
    case dict:find(Pid, Pd) of
    {ok, _} ->
        Pd;

    _ ->
        erlang:monitor(process, Pid),

        dict:store(Pid, {queue, QueueName}, Pd)

    end,

    {noreply, State#state{qdict = NQd, pdict = NPd}};
-}

handlePublishQueueStatsCast :: State -> PublishQueueStats -> Process (ProcessAction State)
handlePublishQueueStatsCast st@(ST { stQdict = qd, stPdict = pd }) (PublishQueueStats q s pid) = do
    -- logm $ "HroqStatsGatherer.handlePublishQueueStatsCast called:"
    --       ++ (show (q,qstatsAppInfo s,qstatsNewTotalQueued s,qstatsEnqueueCount s,qstatsNewDequeueCount s))

    let qd' = Map.insert q s qd

    pd' <- case Map.lookup pid pd of
          Just _ -> return pd

          Nothing -> do
            logm $ "HroqStatsGatherer.handlePublishQueueStatsCast: creating monitor for:" ++ show pid
            _mref <- monitor pid
            return $ Map.insert pid (StatsQueue q) pd

    continue st {stQdict = qd', stPdict = pd'}

-- ---------------------------------------------------------------------

{-
handle_cast({publish_consumer_stats, ConsName, Pid, Stats}, #state{cdict = Cd, pdict = Pd} = State) ->

    NCd = dict:store(ConsName, Stats, Cd),

    NPd =
    case dict:find(Pid, Pd) of
    {ok, _} ->
        Pd;

    _ ->
        erlang:monitor(process, Pid),

        dict:store(Pid, {consumer, ConsName}, Pd)

    end,

    {noreply, State#state{cdict = NCd, pdict = NPd}};

-}

handlePublishConsumerStatsCast :: State -> PublishConsumerStats -> Process (ProcessAction State)
handlePublishConsumerStatsCast st@(ST {stCdict = cd, stPdict = pd}) (PublishConsumerStats c s pid) = do
    -- logm $ "HroqStatsGatherer.handlePublishConsumerStatsCast called with:" ++ (show (c,s))
    let cd' = Map.insert c s cd

    pd' <- case Map.lookup pid pd of
          Just _ -> return pd

          Nothing -> do
            _mref <- monitor pid
            return $ Map.insert pid (StatsConsumer c) pd

    continue st {stCdict = cd', stPdict = pd'}


-- ---------------------------------------------------------------------
{-
handle_cast(Msg, State) ->
    ?warn({ handle_cast, [Msg, State]}),
    {noreply, State}.
-}

-- ---------------------------------------------------------------------

{-
handle_info({'DOWN', _Ref, process, Pid, _Info}, #state{qdict = Qd, cdict = Cd, pdict = Pd} = State) ->

    case dict:find(Pid, Pd) of
    {ok, {consumer, C}} ->

        {noreply, State#state{cdict = dict:erase(C, Cd), pdict = dict:erase(Pid, Pd) }};

    {ok, {queue, Q}} ->

        {noreply, State#state{qdict = dict:erase(Q, Qd), pdict = dict:erase(Pid, Pd) }};

    _ ->
        {noreply, State}
    end;
-}

-- | If the stats origin disappears, erase the stats to prevent long
-- term memory leak
handleInfoProcessMonitorNotification :: State -> ProcessMonitorNotification -> Process (ProcessAction State)
handleInfoProcessMonitorNotification st n@(ProcessMonitorNotification _ref pid _reason) = do
  -- logm $ "HroqStatsGatherer.handleInfoProcessMonitorNotification called with: " ++ show n
  let qd = stQdict st
      cd = stCdict st
      pd = stPdict st
  case Map.lookup pid pd of
        Just (StatsConsumer c) -> continue st { stCdict = Map.delete c cd
                                              , stPdict = Map.delete pid pd }
        Just (StatsQueue q)    -> continue st { stQdict = Map.delete q qd
                                              , stPdict = Map.delete pid pd }
        Nothing                -> continue st

-- ---------------------------------------------------------------------

{-
handle_info(Info, State) ->

    ?warn({handle_info, [Info, State]}),

    {noreply, State}.

terminate(_, _) ->
    ok.


code_change(_,StateData,_)->
    {ok, StateData}.

%EOF
-}

-- ---------------------------------------------------------------------

-- NOTE: the TH crap has to be a the end, as it can only see the stuff lexically before it in the file

$(remotable [ 'hroq_stats_gatherer
            ])

hroq_stats_gatherer_closure :: Closure (Process ())
hroq_stats_gatherer_closure = ($(mkStaticClosure 'hroq_stats_gatherer))

