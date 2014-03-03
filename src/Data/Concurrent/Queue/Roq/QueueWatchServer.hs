{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE DeriveDataTypeable   #-}
{-# LANGUAGE DeriveGeneric        #-}
module Data.Concurrent.Queue.Roq.QueueWatchServer
  (
  -- * Starting the server
    hroq_queue_watch_server
  , hroq_queue_watch_server_closure
  -- * API

  , queueWatchNoOpCallbackClosure
  -- * Types
  -- , CallbackFun

  -- * Debug
  , ping
  , noopFun

  -- * Remote Table
  , Data.Concurrent.Queue.Roq.QueueWatchServer.__remoteTable

  ) where


import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Serializable()
import Control.Exception hiding (try,catch)
import Data.Binary
import Data.Concurrent.Queue.Roq.Logger
import Data.Concurrent.Queue.Roq.QueueWatch
import Data.Concurrent.Queue.Roq.StatsGatherer hiding (ping)
import Data.Typeable (Typeable)
import GHC.Generics
import System.Environment


--------------------------------------------------------------------------------
-- Types                                                                      --
--------------------------------------------------------------------------------

-- Call and Cast request types.

{-

-define(EXAMPLE_QUEUE_WATCH_CONFIG, [

                                    %TAG     APPINFO   METRIC
                                    {"MAIN", "^MAIN$", "size"},
                                    {"MENQ", "^MAIN$", "enq"},
                                    {"MDEQ", "^MAIN$", "deq"},
                                    {"DLQ",  "^DLQ$",  "size"}

                                    ]).
-}
-- TODO: look at getting this via dyre http://hackage.haskell.org/package/dyre-0.8.2/docs/Config-Dyre.html
eXAMPLE_QUEUE_WATCH_CONFIG :: [(String,String,Metric)]
eXAMPLE_QUEUE_WATCH_CONFIG =
  [
    -- TAG     APPINFO   METRIC
    ("MAIN", "^MAIN$", Size),
    ("MENQ", "^MAIN$", Enq),
    ("MDEQ", "^MAIN$", Deq),
    ("DLQ",  "^DLQ$",  Size)
  ]

{-
-define(MONITOR_INTERVAL_MS, 30000).
-}
mONITOR_INTERVAL_MS :: Delay
-- mONITOR_INTERVAL_MS = Delay $ milliSeconds 30000
mONITOR_INTERVAL_MS = Delay $ milliSeconds 9000


-- Call operations

-- Cast operations

data Ping = Ping
  deriving (Typeable, Generic, Eq, Show)
instance Binary Ping where

-- ---------------------------------------------------------------------

-- type CallbackFun = (String -> Process ())

data State = ST (String -> Process ()) ProcessId

emptyState :: ProcessId -> State
emptyState pid = ST noopFun pid

noopFun :: (String -> Process ())
noopFun str = do
  logm $ "HroqQueueWatchServer noopFun called with:[" ++ show str ++ "]"
  return ()

-- ---------------------------------------------------------------------

hroq_queue_watch_server :: Closure (String -> Process ()) -> Process ()
hroq_queue_watch_server = start_queue_watch_server

--------------------------------------------------------------------------------
-- API                                                                        --
--------------------------------------------------------------------------------


ping :: Process ()
ping = do
  pid <- getServerPid
  cast pid Ping


-- ---------------------------------------------------------------------

getServerPid :: Process ProcessId
getServerPid = do
  mpid <- whereis hroqQueueWatchServerProcessName
  case mpid of
    Just pid -> return pid
    Nothing -> do
      logm "HroqQueueWatchServer:getServerPid failed"
      error "HroqQueueWatchServer:blow up"

-- -------------------------------------

hroqQueueWatchServerProcessName :: String
hroqQueueWatchServerProcessName = "HroqQueueWatchServer"

-- ---------------------------------------------------------------------

{-
-spec start_link(queue_watch_callback_fun()) -> {ok, pid()}.
start_link(CallbackFun) when is_function(CallbackFun,1) ->
    ?info(starting),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [CallbackFun], []).

init([CallbackFun]) ->
    {ok, {CallbackFun}, ?MONITOR_INTERVAL_MS}.
-}
start_queue_watch_server :: Closure (String -> Process ()) -> Process ()
start_queue_watch_server callbackFun = do
  logm $ "HroqQueueWatchServer:start_queue_watch_server entered"
  fun <- unClosure callbackFun
  logm $ "HroqQueueWatchServer:start_queue_watch_server after unClosure"

  self <- getSelfPid
  register hroqQueueWatchServerProcessName self
  pid <- hroq_stats_gatherer_pid
  serve (fun,pid) initFunc serverDefinition
  where initFunc :: InitHandler (String -> Process (),ProcessId) State
        initFunc (fun,statsPid) = do
          logm $ "HroqQueueWatchServer:start.initFunc"
          return $ InitOk (ST fun statsPid) mONITOR_INTERVAL_MS

serverDefinition :: ProcessDefinition State
serverDefinition = defaultProcess {
     apiHandlers = [
        --  handleCall handleGetQueueStatsCall

        -- , handleCast handlePublishConsumerStatsCast
        handleCast (\s Ping -> do {logm $ "HroqQueueWatchServer:ping"; continue s })

        ]
    , infoHandlers =
        [
        -- handleInfo handleInfoProcessMonitorNotification
        ]
     , timeoutHandler = handleTimeout
     , shutdownHandler = \_ reason -> do
           { logm $ "HroqQueueWatchServer terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State

-- ---------------------------------------------------------------------
-- Implementation
-- ---------------------------------------------------------------------



-- ---------------------------------------------------------------------
{-
handle_call(_, _From, ServerState) ->
    {noreply, ServerState, ?MONITOR_INTERVAL_MS}.

handle_cast(_, ServerState) ->
    {noreply, ServerState, ?MONITOR_INTERVAL_MS}.
-}

-- ---------------------------------------------------------------------
{-
handle_info(timeout, {CallbackFun}) ->

    QwConfig =
    case application:get_env(mira_eroq, queue_watch_config) of
    {ok, QWcfg} ->
        QWcfg;
    _ ->
        ?EXAMPLE_QUEUE_WATCH_CONFIG
    end,

    case catch(eroq_queue_watch:queue_watch(QwConfig)) of
    {ok, QwString} ->
        catch(CallbackFun([QwString])),
        ok;
    What ->
        ?warn({queue_watch_fail, CallbackFun, What})
    end,

    {noreply, {CallbackFun}, ?MONITOR_INTERVAL_MS};
-}

handleTimeout :: TimeoutHandler State
handleTimeout st@(ST callbackFun statsPid) currDelay = do
  logt $ "HroqQueueWatchServer:handleTimeout entered"

  -- TODO: use something like dyre to look this up
  mQwConfig <- liftIO $ lookupEnv "queue_watch_config"
  let qwConfig = case mQwConfig of
        Just cfg -> -- cfg
                   eXAMPLE_QUEUE_WATCH_CONFIG
        Nothing -> eXAMPLE_QUEUE_WATCH_CONFIG

  let handler :: SomeException -> Process ()
      handler e = do
        logm $ "HroqQueueWatchServer:handler got error:" ++ show e
        return ()

      worker = do
        qwString <- queue_watch statsPid qwConfig
        callbackFun qwString

  catch (worker) handler

  logt $ "HroqQueueWatchServer:handleTimeout complete"

  timeoutAfter currDelay st

-- ---------------------------------------------------------------------

{-
handle_info(_, ServerState) ->
    {noreply, ServerState, ?MONITOR_INTERVAL_MS}.

code_change(_OldVsn, ServerState, _Extra) ->
    {ok, ServerState}.

terminate(Reason, _ServerState) ->
    ?info({terminate, Reason}).


%EOF



-}

-- ---------------------------------------------------------------------

-- NOTE: the TH crap has to be a the end, as it can only see the stuff lexically before it in the file


$(remotable [ 'hroq_queue_watch_server
            , 'noopFun
            ])

-- hroq_stats_gatherer_closure :: Closure (Process ())

-- hroq_queue_watch_server_closure :: (Closure (String -> Process ())) -> Closure (Process ())
hroq_queue_watch_server_closure :: (Closure ((String -> Process ()))) -> Closure (Process ())
hroq_queue_watch_server_closure callback = ( $(mkClosure 'hroq_queue_watch_server) callback)

queueWatchNoOpCallbackClosure :: Closure ((String -> Process ()))
queueWatchNoOpCallbackClosure = ( $(mkStaticClosure 'noopFun) )

-- EOF

