{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Data.HroqConsumer
  (
    pause
  , resume

  , startConsumer
  , __remoteTable
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
import Control.Monad(when,replicateM,foldM,liftM3,liftM2,liftM)
import Data.Binary
import Data.Hroq
import Data.HroqApp
import Data.HroqLogger
import Data.HroqQueue
import Data.Maybe
import Data.RefSerialize
import Data.Time.Clock
import Data.Typeable hiding (cast)
import System.Environment (getArgs)
import qualified Data.ByteString.Lazy.Char8 as C8
import qualified Data.HroqMnesia as HM
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
pause consumerName = mycast consumerName ConsumerPause

resume :: ConsumerName -> Process ()
resume consumerName = mycast consumerName ConsumerResume


-- ConsumerName, AppInfo, SrcQueue, DlqQueue, WorkerModule, WorkerFunc, WorkerParams, State, DoCleanup
startConsumer :: (ConsumerName,String,QName,QName,ConsumerFunc,String,String,ConsumerState,Bool,EKG.Server) -> Process ProcessId
startConsumer initParams@(consumerName,_,_,_,_,_,_,_,_,_) = do
  let server = serverDefinition
  sid <- spawnLocal $ start initParams initFunc server >> return ()
  register (mkRegisteredConsumerName consumerName) sid
  return sid

-- ---------------------------------------------------------------------

getPid :: ConsumerName -> Process ProcessId
getPid consumerName = do
  -- deliberately blow up if not registered
  Just pid <- whereis (mkRegisteredConsumerName consumerName)
  return pid

mkRegisteredConsumerName :: ConsumerName -> String
mkRegisteredConsumerName (CN consumerName) = "HroqConsumer:" ++ consumerName

mycall ::
  (Typeable b, Typeable a, Binary b, Binary a)
  => ConsumerName -> a -> Process b
mycall consumerName op = do
  sid <- getPid consumerName
  call sid op

mycast ::
  (Typeable a, Binary a)
  => ConsumerName -> a -> Process ()
mycast consumerName op = do
  sid <- getPid consumerName
  cast sid op

-- ---------------------------------------------------------------------

-- |Init callback
-- [ConsumerName, AppInfo, SrcQueue, DlqQueue, WorkerModule, ConsumerFunc, WorkerParams, InfoModule, InfoFunc, State, DoCleanup]
initFunc :: InitHandler (ConsumerName,String,QName,QName,ConsumerFunc,String,String,ConsumerState,Bool,EKG.Server) State
initFunc (consumerName,appInfo,srcQueue,dlqQueue,worker,infoModule,infoFunc,state,doCleanup,ekg) = do
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
    let s = State
             { csConsumerName = consumerName
             , csAppInfo            = appInfo
             , csSrcQueue           = srcQueue
             , csDlqQueue           = dlqQueue
             , csWorker             = worker
             , csInfoModule         = "csInfoModule"
             , csInfoFunc           = "csInfoFunc"
             , csDoCleanup          = doCleanup
             , csWorkerPid          = pid
             , csState              = state
             , csSrcQueueEmptyCount = 0
             , csProcessedCount     = 0
             , csErrorCount         = 0
             }
    -- eroq_groups:join(ConsumerName, ?MODULE),


    logm $ "HroqConsumer:initFunc ending"

    return $ InitOk s Infinity

-- ---------------------------------------------------------------------
-- | Promote a function to a monad, scanning the monadic arguments from
-- left to right (cf. 'liftM2').
liftM13  :: (Monad m) => (a1 -> a2 -> a3 -> a4 -> a5 -> a6 -> a7 -> a8 -> a9 -> a10 -> a11 -> a12 -> a13 -> r) 
            -> m a1 -> m a2 -> m a3 -> m a4 -> m a5 -> m a6 -> m a7 -> m a8 -> m a9 -> m a10 -> m a11 -> m a12 -> m a13 -> m r
liftM13 f m1 m2 m3 m4 m5 m6 m7 m8 m9 m10 m11 m12 m13 = do
  { x1 <- m1; x2 <- m2; x3 <- m3; x4 <- m4; x5 <- m5; x6 <- m6;
    x7 <- m7; x8 <- m8; x9 <- m9; x10 <- m10; x11 <- m11; x12 <- m12; x13 <- m13;
  return (f x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13) }

-- ---------------------------------------------------------------------

data ConsumerPause  = ConsumerPause  deriving (Typeable)
data ConsumerResume = ConsumerResume deriving (Typeable)

instance Binary ConsumerPause where
  put _ = put 'P'
  get   = do (_c::Char) <- get
             return ConsumerPause

instance Binary ConsumerResume where
  put _ = put 'R'
  get   = do (_c::Char) <- get
             return ConsumerResume

data ConsumerUpdateCounters = ConsumerUpdateCounters !Integer !Integer !Integer
                              deriving (Typeable,Show)
xxxxx
-- ---------------------------------------------------------------------

type ConsumerFunc = Closure (Process (Either String ConsumerReply))

{-
instance Show ConsumerFunc where
  show _ = "ConsumerFunc"
-}

data ConsumerReply = ConsumerReplyOk
                   | ConsumerReplyOkTimeout !Delay
                   | ConsumerReplyOkNewParams ![String] !Delay
                   | ConsumerReplyRetry !Delay
                   | ConsumerReplyRetryNewParams ![String] !Delay
                   deriving (Typeable,Show)
instance Binary ConsumerReply where
  put (ConsumerReplyOk)                 = put 'K'
  put (ConsumerReplyOkTimeout d)        = put 'T' >> put d
  put (ConsumerReplyOkNewParams p d)    = put 'P' >> put p >> put d
  put (ConsumerReplyRetry d)            = put 'R' >> put d
  put (ConsumerReplyRetryNewParams p d) = put 'N' >> put p >> put d

  get = do
    sel <- get
    case sel of
      'K' -> return ConsumerReplyOk
      'T' -> liftM  ConsumerReplyOkTimeout get
      'P' -> liftM2 ConsumerReplyOkNewParams get get
      'R' -> liftM  ConsumerReplyRetry get
      'N' -> liftM2 ConsumerReplyRetryNewParams get get

-- ---------------------------------------------------------------------

data PauseWaitRsp = PauseWaitRsp String
                    deriving (Typeable,Show)

instance Binary PauseWaitRsp where
  put (PauseWaitRsp ref) = put 'P' >> put ref
  get = do
    sel <- get
    case sel of
      'P' -> liftM PauseWaitRsp get

data ResumeWaitRsp = ResumeWaitRsp String
                     deriving (Typeable,Show)

instance Binary ResumeWaitRsp where
  put (ResumeWaitRsp ref) = put 'R' >> put ref
  get = do
    sel <- get
    case sel of
      'R' -> liftM ResumeWaitRsp get

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
data State = State
    { csConsumerName       :: !ConsumerName
    , csAppInfo            :: !String
    , csSrcQueue           :: !QName
    , csDlqQueue           :: !QName
    , csWorker             :: !ConsumerFunc
    , csInfoModule         :: !String -- for now
    , csInfoFunc           :: !String -- for now
    , csDoCleanup          :: !Bool
    , csWorkerPid          :: !ProcessId
    , csState              :: !ConsumerState
    , csSrcQueueEmptyCount :: !Integer
    , csProcessedCount     :: !Integer
    , csErrorCount         :: !Integer
    } deriving (Show,Typeable)

instance Binary State where
  put (State a b c d e f g h i j k l m) =
       put a >> put b >> put c >> put d >> put e >> put f >>
       put g >> put h >> put i >> put j >> put k >> put l >> put m

  get = liftM13 State get get get get get get get get get get get get get


data ConsumerState = ConsumerActive
                   | ConsumerPaused
                   | ConsumerExit
                   deriving (Show,Eq,Typeable)
instance Binary ConsumerState where
  put ConsumerActive = put 'A'
  put ConsumerPaused = put 'P'
  put ConsumerExit   = put 'X'

  get = do
    sel <- get
    case sel of
      'A' -> return ConsumerActive
      'P' -> return ConsumerPaused
      'E' -> return ConsumerExit

--------------------------------------------------------------------------------
-- Implementation                                                             --
--------------------------------------------------------------------------------

serverDefinition :: ProcessDefinition State
serverDefinition = defaultProcess {
     apiHandlers = [
          handleCast handlePause
        , handleCast handleResume

        -- Internal worker-to-master comms
        , handleCase handleUpdateCounters
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
    logt $ "handlePause starting"
    -- s' <- enqueue_one_message q v s
    let s' = s
    logt $ "handlePause done"
    continue s'

handleResume :: State -> ConsumerResume -> Process (ProcessAction State)
handleResume s ConsumerResume = do
    logt $ "handleResume starting"
    -- s' <- enqueue_one_message q v s
    let s' = s
    logt $ "handleResume done"
    continue s'

handleUpdateCounters :: State -> ConsumerUpdateCounters -> Process (ProcessAction State)
handleUpdateCounters s ConsumerResume = do
    logt $ "handleResume starting"
    -- s' <- enqueue_one_message q v s
    let s' = s
    logt $ "handleResume done"
    continue s'

-- ---------------------------------------------------------------------

-- -record(wstate, {waiting = no, app_params = undefined}).

data AppParams = AP [String]
                 deriving (Typeable,Show)

instance Binary AppParams where
  put (AP v) = put 'P' >> put v
  get = do
    sel <- get
    case sel of
      'P' -> liftM AP get

data WorkerState = WorkerState
  { wsWaiting   :: Waiting
  , wsAppParams :: AppParams
  }

data Waiting = WaitNo
             | WaitQueue
             | WaitResume UTCTime NominalDiffTime
               -- ^when started, how long to wait
               -- NOTE: using Data.Time.Clock values here
             deriving (Show,Eq)

data ConsumerMsg = ConsumerMsgPause
                 | ConsumerMsgResume
                 | ConsumerParams AppParams
                 | ConsumerPauseWait ProcessId String
                 | ConsumerResumeWait ProcessId String
                 deriving (Typeable,Show)

instance Binary ConsumerMsg where
  put (ConsumerMsgPause)        = put 'P'
  put (ConsumerMsgResume)       = put 'R'
  put (ConsumerParams p)        = put 'A' >> put p
  put (ConsumerPauseWait p v)   = put 'W' >> put p >> put v
  put (ConsumerResumeWait p v)  = put 'E' >> put p >> put v

  get = do
    sel <- get
    case sel of
      'P' -> return ConsumerMsgPause
      'R' -> return ConsumerMsgResume
      'A' -> liftM  ConsumerParams get
      'W' -> liftM2 ConsumerPauseWait get get
      'E' -> liftM2 ConsumerResumeWait get get


data QueueMessage = QueueMessage
                     deriving (Typeable,Show)

instance Binary QueueMessage where
  put QueueMessage = put 'Q'
  get = do
    sel <- get
    case sel of
      'Q' -> return QueueMessage


-- |Message sent to Queue Worker to get its state
data GetState = GetState
                 deriving (Typeable,Show)
instance Binary GetState where
  put GetState = put 'G'
  get = do
    sel <- get
    case sel of
      'G' -> return GetState

-- ---------------------------------------------------------------------

-- process_local_storage(DlqQueue, CName, M, F, A)->
process_local_storage dlqQueue cName worker = do
  r <- HM.dirty_read_ls hroq_consumer_local_storage_table cName
  case r of
    Nothing -> return ReadOpReplyEmpty
    Just (CM _ msg _ _) -> do
      rr <- worker msg
      case rr of
        Right (ConsumerReplyOk) -> do
          -- ok = eroq_util:retry_dirty_delete(10, eroq_consumer_local_storage_table, CName);
          dirty_delete_ls hroq_consumer_local_storage_table cName
          return rr
        Right (ConsumerReplyOkTimeout timeoutVal) -> do
          dirty_delete_ls hroq_consumer_local_storage_table cName
          return rr
        Right (ConsumerReplyOkNewParams newWorkerParams timeoutVal) -> do
          dirty_delete_ls hroq_consumer_local_storage_table cName
          return rr
        Right (ConsumerReplyRetry timeoutVal) -> do
          return timeoutVal
        Right (ConsumerReplyRetryNewParams newWorkerParams timeoutVal) -> do
          return rr
        Left e -> do
          dlq_message msg dlqQueue e
          dirty_delete_ls hroq_consumer_local_storage_table cName
          return rr

-- ---------------------------------------------------------------------
-- acquire_and_store_msg(Key, Msg, {CName, SrcQ}) -> ok = eroq_util:retry_dirty_write(10, eroq_consumer_local_storage_table, #eroq_consumer_message{cid=CName, key = Key, msg = Msg, src_queue = SrcQ, timestamp = now()}).

acquire_and_store_msger :: (ConsumerName, QName) -> QEntry -> Process (Either String ())
acquire_and_store_msger (cName,srcQ) entry = do
  logm $ "acquire_and_store_msger:undefined" ++ (show (cName,srcQ,entry))
  ts <- getTimeStamp
  let msg = CM cName entry srcQ ts
  HM.dirty_write_ls hroq_consumer_local_storage_table msg
  return (Right ())

remotable [ 'acquire_and_store_msger
          -- , 'purger
          ]

acquire_and_store_msg :: (ConsumerName, QName) -> Closure (QEntry -> Process (Either String ()))
acquire_and_store_msg params = ( $(mkClosure 'acquire_and_store_msger) params)
-- acquire_and_store_msg = undefined

-- ---------------------------------------------------------------------

{-
worker_entry(CName, CPid, EntryState, AppParams) ->
    process_flag(trap_exit, true),
    worker_loop(CName, CPid, EntryState, #wstate{app_params = AppParams}).
-}

worker_entry :: String -> ProcessId -> ConsumerState -> WorkerState
            -> Process ()
worker_entry cName cPid entryState wState =
    -- process_flag(trap_exit, true),
    worker_loop cName cPid entryState wState


{-
worker_loop(CName, CPid, State, #wstate{waiting = Waiting} = WState) when State =/= exit ->
-}

worker_loop :: String -> ProcessId -> ConsumerState -> WorkerState
            -> Process ()
worker_loop _ _ ConsumerExit _ = do return ()
worker_loop cName cPid state wState = do
  let waiting = wsWaiting wState
{-
    RxTimeoutMs =
    if (State == active) ->
        case Waiting of
        no ->
            0;
        {resume, T0, SleepTimeMs} ->
            DiffMs = trunc(timer:now_diff(now(), T0)/1000),
            if (SleepTimeMs > DiffMs) ->
                SleepTimeMs - DiffMs;
            true ->
                0
            end;
        queue ->
            infinity
        end;
    true ->
        infinity
    end,
-}
  rxTimeoutMs <- case state of
    ConsumerActive -> case waiting of
         WaitNo -> return $ Delay $ milliSeconds 0
         WaitResume t0 sleepTimeMs -> do
           now <- liftIO getCurrentTime
           let diffMs = diffUTCTime now t0
           if sleepTimeMs > diffMs
                -- then return $ sleepTimeMs - diffMs
                then return $ Delay $ milliSeconds
                            $ round (((realToFrac sleepTimeMs) - (realToFrac diffMs)) * 1000)
                else return $ Delay $ milliSeconds 0
    _ -> return Infinity

{-
    {NewState, NewWState} =
    receive
    {'$eroq_consumer', What} ->
        case What of
        pause                   -> {paused, WState};
        resume                  -> {active, WState};
        {app_params, NewParams} -> {State, WState#wstate{app_params = NewParams}};
        {pause_wait, Pid, Ref}  ->
            Pid ! {pause_wait_rsp, Ref},
            {paused, WState};
        {resume_wait, Pid, Ref} ->
            N = worker_process_message(CPid, WState),
            Pid ! {resume_wait_rsp, Ref},
            {active, N}
        end;
    {'$eroq_queue', _SrcQueue, _QSize} ->
        case State of
        active ->
            if ((Waiting == queue) or (Waiting == no)) ->
                N = worker_process_message(CPid, WState),
                {active, N};
            true ->
                {active, WState}
            end;
        paused ->
            N =
            if (Waiting == queue) ->
                WState#wstate{waiting = no};
            true ->
                WState
            end,
            {paused,N}
        end;
    {'EXIT', CPid, shutdown} ->
        {exit, WState};
    Info ->
        {State, worker_handle_info(CPid, Info, WState)}

    after RxTimeoutMs ->
        if State == active ->
            {active, worker_process_message(CPid, WState)};
        true ->
            {State, WState}
        end
    end,
-}
  let handlers =
       [ match (handleConsumerMsg cPid state wState)
       , match (handleQueueMessage cPid state wState)
       ]

  (newState,newWState) <- case rxTimeoutMs of
    Infinity -> do
      receiveWait handlers
    Delay interval -> do
      mm <- receiveTimeout (asTimeout interval) handlers
      case mm of
        Nothing  -> do
          if state == ConsumerActive
            then do
              n <- worker_process_message cPid wState
              return (state,n)
            else return (state,wState)
        Just ret -> return ret

  worker_loop cName cPid newState newWState


{-
    {'$eroq_consumer', What} ->
        case What of
        pause                   -> {paused, WState};
        resume                  -> {active, WState};
        {app_params, NewParams} -> {State, WState#wstate{app_params = NewParams}};
        {pause_wait, Pid, Ref}  ->
            Pid ! {pause_wait_rsp, Ref},
            {paused, WState};
        {resume_wait, Pid, Ref} ->
            N = worker_process_message(CPid, WState),
            Pid ! {resume_wait_rsp, Ref},
            {active, N}
        end;
-}
handleConsumerMsg :: ProcessId -> ConsumerState -> WorkerState -> ConsumerMsg -> Process (ConsumerState,WorkerState)
handleConsumerMsg cPid state wState msg = do
  case msg of
    ConsumerMsgPause  -> return (ConsumerPaused,wState)
    ConsumerMsgResume -> return (ConsumerActive,wState)
    ConsumerParams p  -> return (state, wState {wsAppParams = p})
    ConsumerPauseWait pid ref -> do
      send pid (PauseWaitRsp ref)
      return (ConsumerPaused,wState)
    ConsumerResumeWait pid ref -> do
      n <- worker_process_message cPid wState
      send pid (ResumeWaitRsp ref)
      return (ConsumerActive,n)


{-
    {'$eroq_queue', _SrcQueue, _QSize} ->
        case State of
        active ->
            if ((Waiting == queue) or (Waiting == no)) ->
                N = worker_process_message(CPid, WState),
                {active, N};
            true ->
                {active, WState}
            end;
        paused ->
            N =
            if (Waiting == queue) ->
                WState#wstate{waiting = no};
            true ->
                WState
            end,
            {paused,N}
        end;
-}
handleQueueMessage :: ProcessId -> ConsumerState -> WorkerState -> QueueMessage -> Process (ConsumerState,WorkerState)
handleQueueMessage cPid state wState msg = do
  case state of
    ConsumerActive -> do
      if (wsWaiting wState == WaitQueue || wsWaiting wState == WaitNo)
        then do
          n <- worker_process_message cPid wState
          return (ConsumerActive,n)
        else return (ConsumerActive,wState)
    ConsumerPaused -> do
      let n = if (wsWaiting wState == WaitQueue)
                then wState { wsWaiting = WaitNo }
                else wState
      return (ConsumerPaused,n)

-- ---------------------------------------------------------------------

data Instruction = InstructionProcessLocal
                 | InstructionWaitEmpty
                 | InstructionSleepError
                 deriving (Show)

-- %State-based callback functions
-- worker_process_message(CPid, #wstate{app_params = WorkerParams} = WState) ->
worker_process_message :: ProcessId -> WorkerState -> Process WorkerState
worker_process_message cPid wState = do

  -- StateData = gen_server:call(CPid, get_state, infinity),
  stateData <- call cPid GetState :: Process State

{-
    CName                = StateData#eroq_consumer_state.consumer_name,
    SrcQueue             = StateData#eroq_consumer_state.src_queue,
    DlqQueue             = StateData#eroq_consumer_state.dlq_queue,

    WorkerModule         = StateData#eroq_consumer_state.worker_module,
    WorkerFunc           = StateData#eroq_consumer_state.worker_func,

    QueueEmptyCount      = StateData#eroq_consumer_state.src_queue_empty_count,
    ProcessedCount       = StateData#eroq_consumer_state.processed_count,
    ErrorCount           = StateData#eroq_consumer_state.error_count,
-}
  let cName           = csConsumerName       stateData
      srcQueue        = csSrcQueue           stateData
      dlqQueue        = csDlqQueue           stateData
      worker          = csWorker             stateData
      queueEmptyCount = csSrcQueueEmptyCount stateData
      processedCount  = csProcessedCount     stateData
      errorCount      = csErrorCount         stateData
{-
    FirstProcLocalStorage =
    case eroq_util:retry_dirty_read(10, eroq_consumer_local_storage_table, CName) of
    {ok, []} ->
        false;
    {ok, _} ->
        true
    end,
-}
  firstProcLocalStorage <- do
    r <- HM.dirty_read_ls hroq_consumer_local_storage_table cName
    case r of
      Nothing -> return True
      Just _  -> return False
  logm $ "HroqConsumer:firstProcLocalStorage=" ++ (show firstProcLocalStorage)
{-
    Instruction =
    if not FirstProcLocalStorage ->
        case catch(eroq_queue:dequeue(SrcQueue, ?MODULE, acquire_and_store_msg, {CName, SrcQueue}, self())) of
        ok ->
            process_local;
        {error, empty} ->
            wait_empty;
        AcqError ->
            ?warn({acquisition_error, StateData#eroq_consumer_state.consumer_name, AcqError}),
            sleep_error
        end;
    true ->
        process_local
    end,
-}
  instruction <-
    if (not firstProcLocalStorage)
       then do
          myPid <- getSelfPid
          rr <- dequeue srcQueue (acquire_and_store_msg (cName, srcQueue)) (Just myPid)
          case rr of
            ReadOpReplyOk      -> return InstructionProcessLocal
            ReadOpReplyEmpty   -> return InstructionWaitEmpty
            ReadOpReplyError e -> do
              logm $ "HRoqConsumer:acquisition_error" ++ (show (cName,srcQueue,e))
              return InstructionSleepError
       else return InstructionProcessLocal
{-
    case Instruction of
    process_local ->
        case catch(process_local_storage(DlqQueue, CName, WorkerModule, WorkerFunc, WorkerParams)) of
        ok ->
            gen_server:cast(CPid, {update_counters, QueueEmptyCount, ProcessedCount + 1, ErrorCount}),
            WState#wstate{waiting = no};
        {ok, TimeoutMs} ->
            gen_server:cast(CPid, {update_counters, QueueEmptyCount, ProcessedCount + 1, ErrorCount}),
            WState#wstate{waiting = {resume, now(), TimeoutMs}};
        {ok, NewWorkerParams, TimeoutMs} ->
            gen_server:cast(CPid, {update_counters, QueueEmptyCount, ProcessedCount + 1, ErrorCount}),
            WState#wstate{waiting = {resume, now(), TimeoutMs}, app_params = NewWorkerParams};
        {retry, TimeoutMs} ->
            WState#wstate{waiting = {resume, now(), TimeoutMs}};
        {retry, NewWorkerParams, TimeoutMs} ->
            WState#wstate{waiting = {resume, now(), TimeoutMs}, app_params = NewWorkerParams};
        _ProcError ->
            gen_server:cast(CPid, {update_counters, QueueEmptyCount, ProcessedCount, ErrorCount + 1}),
            WState#wstate{waiting = {resume, now(), ?PROCESSING_ERROR_DELAY_MS}}
        end;
    wait_empty ->
        gen_server:cast(CPid, {update_counters, QueueEmptyCount + 1, ProcessedCount, ErrorCount}),
        WState#wstate{waiting = queue};
    sleep_error ->
        gen_server:cast(CPid, {update_counters, QueueEmptyCount, ProcessedCount, ErrorCount + 1}),
        WState#wstate{waiting = {resume, now(), ?PROCESSING_ERROR_DELAY_MS}}
    end.
-}

  wState' <- case instruction of
    InstructionProcessLocal -> do
      rr <- process_local_storage dlqQueue cName worker
      case rr of
        Right (ConsumerReplyOk) -> do
            -- gen_server:cast(CPid, {update_counters, QueueEmptyCount, ProcessedCount + 1, ErrorCount}),
            cast cPid xxxx
            -- WState#wstate{waiting = no};
        Right (ConsumerReplyOkTimeout timeoutVal) -> do

        Right (ConsumerReplyOkNewParams newWorkerParams timeoutVal) -> do

        Right (ConsumerReplyRetry timeoutVal) -> do

        Right (ConsumerReplyRetryNewParams newWorkerParams timeoutVal) -> do

        Left e -> do

    InstructionWaitEmpty -> do
    InstructionSleepError -> do

  return wState



