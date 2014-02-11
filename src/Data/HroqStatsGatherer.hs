{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE DeriveDataTypeable   #-}
{-# LANGUAGE DeriveGeneric        #-}
module Data.HroqStatsGatherer
  (
    publish_queue_stats
  , hroq_stats_gatherer
  , ping
  , hroq_stats_gatherer_closure
  , __remoteTable

  , QStats(..)
  )
  where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Platform hiding (__remoteTable)
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Serializable()
import Data.Binary
import Data.Typeable (Typeable)
import qualified Data.Map as Map
import Data.Hroq
import Data.HroqLogger
import GHC.Generics
import qualified System.Remote.Monitoring as EKG


--------------------------------------------------------------------------------
-- Types                                                                      --
--------------------------------------------------------------------------------

-- Call and Cast request types. Response types are unnecessary as the GenProcess
-- API uses the Async API, which in turn guarantees that an async handle can
-- /only/ give back a reply for that *specific* request through the use of an
-- anonymous middle-man (as the sender and reciever in our case).

data PublishQueueStats = PublishQueueStats QName QStats
  deriving (Typeable, Generic, Eq, Show)
instance Binary PublishQueueStats where

-- {AppInfo, NewTotalQueuedMsg, EnqueueCount, NewDequeueCount}
data QStats = QStats { qstatsAppInfo         :: String
                     , qstatsNewTotalQueued  :: Integer
                     , qstatsEnqueueCount    :: Integer
                     , qstatsNewDequeueCount :: Integer
                     }
  deriving (Typeable, Generic, Eq, Show)
instance Binary QStats where


data Ping = Ping
  deriving (Typeable, Generic, Eq, Show)
instance Binary Ping where

-- ---------------------------------------------------------------------

type State = Integer


-- ---------------------------------------------------------------------

hroq_stats_gatherer :: Process ()
hroq_stats_gatherer = do
  logm $ "hroq_stats_gatherer about to start"
  pid <- start_stats_gatherer
  logm $ "hroq_stats_gatherer started:pid=" ++ show pid


--------------------------------------------------------------------------------
-- API                                                                        --
--------------------------------------------------------------------------------

{-
-record(state,  {qdict = dict:new(), cdict = dict:new(), pdict = dict:new()}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

publish_queue_stats(QueueName, Stats)->
    gen_server:cast(?MODULE, {publish_queue_stats, QueueName, self(), Stats}).
-}

ping :: Process ()
ping = do
  pid <- getServerPid
  cast pid Ping

publish_queue_stats :: QName -> QStats -> Process ()
publish_queue_stats qname stats = do -- TODO: check that enqued/dequeued are correct interpretations
  pid <- getServerPid
  cast pid (PublishQueueStats qname stats)

{-
publish_consumer_stats(ConsName, Stats)->
    gen_server:cast(?MODULE, {publish_consumer_stats, ConsName, self(), Stats}).

get_queue_stats(QueueName) ->
    gen_server:call(?MODULE, {get_queue_stats, QueueName}, infinity).

get_consumer_stats(ConsName) ->
    gen_server:call(?MODULE, {get_consumer_stats, ConsName}, infinity).
-}

getServerPid :: Process ProcessId
getServerPid = do
  -- deliberately blow up if not registered
  Just pid <- whereis processName
  return pid

processName = "HroqStatsGatherer"

{-
init_state() -> #state{}.

init(_) ->

    process_flag(trap_exit, true),

    {ok, init_state()}.

-}
start_stats_gatherer :: Process ProcessId
start_stats_gatherer = do
  let server = serverDefinition
  sid <- spawnLocal $ serve 0 initFunc server >> return ()
  register processName sid
  return sid
  where initFunc :: InitHandler Integer State
        initFunc i = do
          logm $ "HroqStatsGatherer:start.initFunc"
          return $ InitOk i Infinity

-- ---------------------------------------------------------------------
-- Implementation
-- ---------------------------------------------------------------------

serverDefinition :: ProcessDefinition State
serverDefinition = defaultProcess {
     apiHandlers = [
        --  handleCall handleEnqueue
        --, handleCall handleReadOp

          handleCast handlePublishCast
        , handleCast (\s Ping -> do {logm $ "HroqStatsGatherer:ping"; continue s })

        ]
    , infoHandlers =
        [
        -- handleInfo_ (\(ProcessMonitorNotification _ _ r) -> logm $ show r >> continue_)
         handleInfo (\dict (ProcessMonitorNotification _ _ r) -> do {logm $ show r; continue dict })
        ]
     , timeoutHandler = \_ _ -> stop $ ExitOther "timeout az"
     , shutdownHandler = \_ reason -> do { logm $ "HroqStatsGatherer terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State


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

handle_call({get_consumer_stats, ConsName}, _From, #state{cdict = Cd} = State) ->

    Res =
    case dict:find(ConsName, Cd) of
    {ok, Stats} ->
        {ok, Stats};

    _ ->
        {error, not_found}
    end,

    {reply, Res, State};


handle_call(Msg, _, State) ->
    ?warn({ handle_call, [Msg]}),
    {reply, error, State}.

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

handlePublishCast :: State -> PublishQueueStats -> Process (ProcessAction State)
handlePublishCast state (PublishQueueStats q s) = do
    logm $ "handlePublishCast called with:" ++ (show (q,s))
    continue state

-- ---------------------------------------------------------------------
{-
handle_cast(Msg, State) ->
    ?warn({ handle_cast, [Msg, State]}),
    {noreply, State}.


handle_info({'DOWN', _Ref, process, Pid, _Info}, #state{qdict = Qd, cdict = Cd, pdict = Pd} = State) ->

    case dict:find(Pid, Pd) of
    {ok, {consumer, C}} ->
        
        {noreply, State#state{cdict = dict:erase(C, Cd), pdict = dict:erase(Pid, Pd) }};
        
    {ok, {queue, Q}} ->

        {noreply, State#state{qdict = dict:erase(Q, Qd), pdict = dict:erase(Pid, Pd) }};

    _ ->
        {noreply, State}
    end;

handle_info(Info, State) ->

    ?warn({handle_info, [Info, State]}),

    {noreply, State}.
    
terminate(_, _) ->
    ok.


code_change(_,StateData,_)->
    {ok, StateData}.

%EOF
-}

-- NOTE: the TH crap has to be a the end, as it can only see the stuff lexically before it in the file

$(remotable [ 'hroq_stats_gatherer
            ])

hroq_stats_gatherer_closure = ($(mkStaticClosure 'hroq_stats_gatherer))

