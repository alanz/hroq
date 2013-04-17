module Data.HroqConsumer
  (
    pause
  , resume

  , startConsumer
  )
  where

import Control.Concurrent
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Internal.Types (NodeId(nodeAddress))
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform hiding (__remoteTable)
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Static (staticLabel, staticClosure)
import Data.Binary
import Data.Hroq
import Data.HroqApp
import Data.HroqLogger
import qualified Data.HroqMnesia as HM
import Data.HroqQueue
import Data.Maybe
import Data.RefSerialize
import Data.Typeable hiding (cast)
import System.Environment (getArgs)
import qualified Data.ByteString.Lazy.Char8 as C8
import qualified Data.Map as Map

import qualified System.Remote.Monitoring as EKG

--------------------------------------------------------------------------------
-- API                                                                        --
--------------------------------------------------------------------------------
{-
pause(ConsumerName) when is_atom(ConsumerName) ->
    gen_server:cast(eroq_util:make_consumer_name(ConsumerName), pause).

resume(ConsumerName) when is_atom(ConsumerName)  ->
    gen_server:cast(eroq_util:make_consumer_name(ConsumerName), resume).

%% @doc Request consumer to pause processing.
%%
%%Wait for upto WaitForMs milliseconds.
%%
pause_wait(ConsumerName, WaitForMs) when is_atom(ConsumerName) and ((WaitForMs =:= infinity) or ((is_integer(WaitForMs)) and (WaitForMs > 0))) ->
    gen_server:call(eroq_util:make_consumer_name(ConsumerName), {pause, WaitForMs}, infinity).

%% @doc Request consumer to resume processing.
%%
%%Wait for upto WaitForMs milliseconds.
%%
resume_wait(ConsumerName, WaitForMs) when is_atom(ConsumerName) and ((WaitForMs =:= infinity) or ((is_integer(WaitForMs)) and (WaitForMs > 0))) ->
    gen_server:call(eroq_util:make_consumer_name(ConsumerName), {resume, WaitForMs}, infinity).

-}

pause :: ConsumerName -> Process ()
pause consumerName = cast (getPid consumerName) ConsumerPause

resume :: ConsumerName -> Process ()
resume consumerName = cast (getPid consumerName) ConsumerResume


-- ConsumerName, AppInfo, SrcQueue, DlqQueue, WorkerModule, WorkerFunc, WorkerParams, State, DoCleanup
startConsumer :: (ConsumerName,String,QName,QName,Closure(WorkerFunc),ConsumerState,Bool,EKG.Server) -> Process ProcessId
startConsumer initParams@(consumerName,_,_,_,_,_,_,_,_) = do
  let server = serverDefinition
  sid <- spawnLocal $ start initParams initFunc server >> return ()
  register (mkRegisteredConsumerName consumerName) sid
  return sid

-- ---------------------------------------------------------------------

getPid :: ConsumerName -> ProcessId
getPid consumerName = do
  -- deliberately blow up if not registered
  Just pid <- whereis (mkRegisteredConsumerName consumerName)
  return pid

mkRegisteredConsumerName :: ConsumerName -> String
mkRegisteredConsumerName (CN consumerName) = "HroqConsumer:" ++ consumerName

-- ---------------------------------------------------------------------

-- |Init callback
-- [ConsumerName, AppInfo, SrcQueue, DlqQueue, WorkerModule, WorkerFunc, WorkerParams, InfoModule, InfoFunc, State, DoCleanup]
-- initFunc 
initFunc (consumerName,appInfo,srcQueue,dlqQueue,worker,infoModule,infoFuncr,state,doCleanup,ekg) = do
    logm $ "HroqConsumer:initFunc starting"


    -- process_flag(trap_exit, true),

    -- CPid = self(),
    cpid <- getSelfPid

{-
    F = fun() ->
        worker_entry(ConsumerName, CPid, State, WorkerParams)
    end,
-}
    -- TODO: populate environment with the other required params
    f <- unClosure worker

    -- ok = mnesia:wait_for_tables([eroq_consumer_local_storage_table], infinity),
    HM.wait_for_tables [hroq_consumer_local_storage_table] Infinity

    -- Pid = spawn_link(F),
    pid <- spawnLinkLocal f

{-
    ConsumerState = #eroq_consumer_state    {
                                            consumer_name         = ConsumerName,
                                            app_info              = AppInfo,
                                            src_queue             = SrcQueue,
                                            dlq_queue             = DlqQueue,
                                            worker_module         = WorkerModule,
                                            worker_func           = WorkerFunc,
                                            worker_params         = WorkerParams,
                                            info_module           = InfoModule,
                                            info_func             = InfoFunc,
                                            do_cleanup            = DoCleanup,
                                            worker_pid            = Pid,
                                            state                 = State
                                            },
-}
    let s = ConsumerState
             { csConsumerName = consumerName
             , csAppInfo = appInfo
             , csSrcQueue = srcQueue
             , csDlqQueue = dlqQueue
             , csWorker   = worker
             , csInfoModule = "csInfoModule"
             , csInfoFunc   = "csInfoFunc"
             , csDoCleanup  = doCleanup
             , csWorkerPid  = pid
             , csState      = state
             }
    -- eroq_groups:join(ConsumerName, ?MODULE),


    logm $ "HroqConsumer:initFunc ending"

    return $ InitOk s Infinity

-- ---------------------------------------------------------------------

data ConsumerName = CN !String
                    deriving (Eq,Show)


data ConsumerPause  = ConsumerPause
data ConsumerResume = ConsumerResume


{-
    ConsumerState = #eroq_consumer_state    {
                                            consumer_name         = ConsumerName,
                                            app_info              = AppInfo,
                                            src_queue             = SrcQueue,
                                            dlq_queue             = DlqQueue,
                                            worker_module         = WorkerModule,
                                            worker_func           = WorkerFunc,
                                            worker_params         = WorkerParams,
                                            info_module           = InfoModule,
                                            info_func             = InfoFunc,
                                            do_cleanup            = DoCleanup,
                                            worker_pid            = Pid,
                                            state                 = State
                                            },
-}
data State = ConsumerState 
    { csConsumerName :: !String
    , csAppInfo      :: !String
    , csSrcQueue     :: !QName
    , csDlqQueue     :: !QName
    , csWorker       :: !(Closure(WorkerFunc))
    , csInfoModule   :: !String -- for now
    , csInfoFunc     :: !String -- for now
    , csDoCleanup    :: !Bool
    , csWorkerPid    :: !ProcessId
    , csState        :: !ConsumerState
    } deriving (Show)


data ConsumerState = ConsumerActive
                   | ConsumerPaused
                   deriving (Show,Eq)



--------------------------------------------------------------------------------
-- Implementation                                                             --
--------------------------------------------------------------------------------

serverDefinition :: ProcessDefinition State
serverDefinition = defaultProcess {
     apiHandlers = [
          handleCast handlePause
        , handleCast handleResume 
        ]
    , infoHandlers =
        [
        -- handleInfo_ (\(ProcessMonitorNotification _ _ r) -> logm $ show r >> continue_)
         handleInfo (\dict (ProcessMonitorNotification _ _ r) -> do {logm $ show r; continue dict })
        ]
     , timeoutHandler = \_ _ -> stop $ TerminateOther "timeout az"
     , terminateHandler = \_ reason -> do { logm $ "HroqConsumer terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State


{-
handle_cast(pause,  #eroq_consumer_state{worker_pid = Pid} = StateData) -> 
    Pid ! {'$eroq_consumer', pause},
    {noreply, StateData#eroq_consumer_state{state = paused}};

handle_cast(resume,  #eroq_consumer_state{worker_pid = Pid} = StateData) -> 
    Pid ! {'$eroq_consumer', resume},
    {noreply, StateData#eroq_consumer_state{state = active}};

-}

handlePause :: State -> ConsumerPause -> Process (ProcessAction State)
handlePause s ConsumerPause = do
    -- logm $ "enqueue called with:" ++ (show (q,v))
    logt $ "handlePause starting"
    -- s' <- enqueue_one_message q v s
    let s' = s
    logt $ "handlePause done"
    continue s'

handleResume :: State -> ConsumerResume -> Process (ProcessAction State)
handleResume s ConsumerResume = do
    -- logm $ "enqueue called with:" ++ (show (q,v))
    logt $ "handleResume starting"
    -- s' <- enqueue_one_message q v s
    let s' = s
    logt $ "handleResume done"
    continue s'



