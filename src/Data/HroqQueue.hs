{-# LANGUAGE DeriveDataTypeable   #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances    #-}
module Data.HroqQueue
  (
    enqueue
  , enqueueCast
  , dequeue
  , peek

  , startQueue

  , WorkerFunc
  , ReadOpReply(..)

  -- * debug
  , enqueue_one_message
  )
  where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform hiding (monitor)
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Exception as Exception
import Control.Monad(when,replicateM,foldM,liftM3,liftM4,liftM2,liftM)
import Data.Binary
import Data.DeriveTH
import Data.Hroq
import Data.HroqGroups
import Data.HroqLogger
import Data.HroqQueueMeta
import Data.HroqStatsGatherer
import Data.HroqUtil
import Data.List
import Data.Maybe
import Data.Typeable (Typeable)
-- import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import qualified Data.HroqMnesia as HM
import qualified Data.Map as Map
import qualified Data.Set as Set

import qualified System.Remote.Monitoring as EKG

--------------------------------------------------------------------------------
-- API                                                                        --
--------------------------------------------------------------------------------

-- |Add an item to a queue
enqueue :: QName -> QValue -> Process ()
enqueue q v = mycall q (Enqueue q v)

enqueueCast :: ProcessId -> QName -> QValue -> Process ()
enqueueCast sid q v = cast sid (Enqueue q v)

dequeue :: QName -> Closure QWorker -> Maybe ProcessId -> Process ReadOpReply
dequeue q w mp = mycall q (ReadOpDequeue q w mp)

peek :: QName -> Process ReadOpReply
peek q = mycall q (ReadOpPeek q)


-- TODO: erlang version starts a Q gen server per Q, uses the QName to
-- lookup in the global process dict where it is.
-- | Start a Queue server
startQueue :: (QName,String,CleanupFunc,EKG.Server) -> Process ProcessId
startQueue initParams@(qname,_,_,_) = do
  let server = serverDefinition
  sid <- spawnLocal $ serve initParams initFunc server >> return ()
  register (mkRegisteredQName qname) sid
  return sid

{-
% Interface exports
-export([
        start_link/3,
        enqueue/2,
        dequeue/4,
        dequeue/5,
        get_queue_size/1,
        get_stats/1,
        set_max_bucket_size/2,
        set_cleanup/2,
        peek/1,
        get_app_info/1,
        get_state/1,
        is_subscribed/2,
        unsubscribe/2,
        create_table/2

        ]).
-}

--------------------------------------------------------------------------------
-- Types                                                                      --
--------------------------------------------------------------------------------

-- Call and Cast request types. Response types are unnecessary as the GenProcess
-- API uses the Async API, which in turn guarantees that an async handle can
-- /only/ give back a reply for that *specific* request through the use of an
-- anonymous middle-man (as the sender and reciever in our case).


-- type QWorker = (QValue -> QValue)
type QWorker = WorkerFunc
instance Show QWorker where
   show _ = "QWorker"

-- operations to be exposed

data Enqueue = Enqueue !QName !QValue
               deriving (Typeable, Show)

data ReadOp = ReadOpDequeue !QName !(Closure (QWorker))  (Maybe ProcessId)
            | ReadOpPeek    !QName
               deriving (Typeable, Show)

data ReadOpReply = ReadOpReplyError !String
                 | ReadOpReplyEmpty
                 | ReadOpReplyOk
                 | ReadOpReplyMsg !QEntry
                 deriving (Typeable,Show)

instance Binary ReadOpReply where
  put (ReadOpReplyError e) = put 'R' >> put e
  put (ReadOpReplyEmpty)   = put 'E'
  put (ReadOpReplyOk)      = put 'K'
  put (ReadOpReplyMsg v)   = put 'M' >> put v

  get = do
    sel <- get
    case sel of
      'R' -> liftM ReadOpReplyError get
      'E' -> return ReadOpReplyEmpty
      'K' -> return ReadOpReplyOk
      'M' -> liftM ReadOpReplyMsg get


instance Binary Enqueue where
  put (Enqueue n v) = put n >> put v
  get = liftM2 Enqueue get get


instance Binary ReadOp where
  put (ReadOpDequeue n w mpid) = put 'D' >> put n >> put w >> put mpid
  put (ReadOpPeek n)           = put 'P' >> put n
  get = do
    sel <- get
    case sel of
      'D' -> liftM3 ReadOpDequeue get get get
      'P' -> liftM  ReadOpPeek get

type WorkerFunc = QEntry -> Process (Either String ())

type CleanupFunc = String

data State = QueueState
   { qsAppInfo            :: !String
   , qsCurrProcBucket     :: !TableName
   , qsCurrOverflowBucket :: !TableName
   , qsTotalQueueSize     :: !Integer
   , qsEnqueueCount       :: !Integer
   , qsDequeueCount       :: !Integer
   , qsMaxBucketSize      :: !Integer
   , qsQueueName          :: !QName
   , qsDoCleanup          :: CleanupFunc -- Should be a function eventually?
   , qsIndexList          :: ![QKey]
   , qsSubscriberPidDict  :: !(Map.Map ProcessId MonitorRef)
   , qsMnesiaSid          :: !ProcessId
   , qsEkg                :: !EKG.Server
   }

{-
    ServerState =#eroq_queue_state  {
                                    app_info             = AppInfo,
                                    curr_proc_bucket     = CurrProcBucket,
                                    curr_overflow_bucket = CurrOverflowBucket,
                                    total_queue_size     = QueueSize,
                                    enqueue_count        = 0,
                                    dequeue_count        = 0,
                                    max_bucket_size      = ?MAX_BUCKET_SIZE,
                                    queue_name           = QueueName,
                                    do_cleanup           = DoCleanup,
                                    index_list           = lists:sort(mnesia:dirty_all_keys(CurrProcBucket)),
                                    subscriber_pid_dict  = dict:new()
                                    },
-}

-- ---------------------------------------------------------------------


-- ---------------------------------------------------------------------


mkRegisteredQName :: QName -> String
mkRegisteredQName (QN qname) = "HroqQueue:" ++ qname

getSid :: QName -> Process ProcessId
getSid qname = do
  -- deliberately blow up if not registered
  Just pid <- whereis (mkRegisteredQName qname)
  return pid

mycall ::
  (Typeable b, Typeable a, Binary b, Binary a)
  => QName -> a -> Process b
mycall qname op = do
  sid <- getSid qname
  call sid op

-- -----------------------------------------------------------------------------

-- |Init callback
initFunc :: InitHandler (QName,String,CleanupFunc,EKG.Server) State
initFunc (queueName,appInfo,doCleanup,ekg) = do

    logm $ "HroqQueue:initFunc started"

    -- process_flag(trap_exit, true),

    HM.wait_for_tables [hroq_queue_meta_table] Infinity
    logm $ "HroqQueue:initFunc 1"

    -- Run through the control table & delete all empty buckets ...
    (queueSize,buckets) <- check_buckets queueName
    logm $ "HroqQueue:initFunc 2:(queueSize,buckets)=" ++ (show (queueSize,buckets))

    -- ok = mnesia:wait_for_tables(Buckets, infinity),
    HM.wait_for_tables buckets Infinity
    logm $ "HroqQueue:initFunc 3"

    let currProcBucket     = head buckets
        currOverflowBucket = last buckets
        -- NOTE: these two buckets may be the same

    if currProcBucket == currOverflowBucket
      then do
        HM.change_table_copy_type currProcBucket HM.DiscCopies
      else do
        HM.change_table_copy_type currProcBucket     HM.DiscCopies
        HM.change_table_copy_type currOverflowBucket HM.DiscCopies
    logm $ "HroqQueue:initFunc 4"


    allKeys <- HM.dirty_all_keys currProcBucket
    logm $ "HroqQueue:initFunc 5"

    mnesiaSid <- HM.getSid
    let s = QueueState
             { qsAppInfo            = appInfo
             , qsCurrProcBucket     = currProcBucket
             , qsCurrOverflowBucket = currOverflowBucket
             , qsTotalQueueSize     = queueSize
             , qsEnqueueCount       = 0
             , qsDequeueCount       = 0
             , qsMaxBucketSize      = maxBucketSizeConst
             , qsQueueName          = queueName
             , qsDoCleanup          = doCleanup
             , qsIndexList          = sort allKeys
             , qsSubscriberPidDict  = Map.empty
             , qsMnesiaSid          = mnesiaSid
             , qsEkg                = ekg
             }


    -- eroq_groups:join(QueueName, ?MODULE),
    join (NGQueue queueName)
    logm $ "HroqQueue:initFunc 6"

    -- catch(eroq_stats_gatherer:publish_queue_stats(QueueName, {AppInfo, QueueSize, 0, 0})),
    publish_queue_stats queueName (QStats appInfo queueSize 0 0)

    logm $ "HroqQueue:initFunc ending"

    return $ InitOk s Infinity


-- ---------------------------------------------------------------------
{-
check_buckets(QueueName) ->
    case eroq_queue_meta:all_buckets(QueueName) of
    {ok, [B]} ->
        case catch(mnesia:table_info(B, storage_type)) of
        disc_copies ->
            {mnesia:table_info(B, size), [B]};
        disc_only_copies ->
            {mnesia:table_info(B, size), [B]};
        _ ->
            %Table does not exist ....
            NB = make_next_bucket(QueueName),
            {0, [NB]}
        end;
    {ok, List} ->
        Size = traverse_check_buckets(QueueName, List, 0),
        case eroq_queue_meta:all_buckets(QueueName) of
        {ok, []} ->
            {0, [make_next_bucket(QueueName)]};
        {ok, CBuckets} ->
            {Size, CBuckets}
        end
    end.
-}
check_buckets :: QName -> Process (Integer,[TableName])
check_buckets queueName = do
  logm $ "check_buckets:" ++ (show queueName)
  mab <- meta_all_buckets queueName
  logm $ " check_buckets:mab=" ++ (show mab)
  case mab of
    [b] -> do
      logm $ "check_buckets: one only:" ++ (show b)
      HM.TIStorageType storage <- HM.table_info b HM.TableInfoStorageType
      case storage of
        HM.DiscCopies -> do
            (HM.TISize size) <- HM.table_info b HM.TableInfoSize
            return (size,[b])
        HM.DiscOnlyCopies -> do
            (HM.TISize size) <- HM.table_info b HM.TableInfoSize
            return (size,[b])
        HM.StorageNone -> do
            nb <- make_next_bucket queueName
            return (0,[nb])
    bs -> do
      logm $ "check_buckets: multi(or none):" ++ (show bs)
      size <- traverse_check_buckets queueName bs 0
      -- Must re-read, traverse_check_buckets may delete empty ones
      mab' <- meta_all_buckets queueName
      logm $ " check_buckets:mab'=" ++ (show mab')
      case mab' of
        [] -> do
          b <- make_next_bucket queueName
          return (0,[b])
        cbuckets -> do
          return (size,cbuckets)

-- ---------------------------------------------------------------------
{-
traverse_check_buckets(QueueName, [B | T], Size)->
    S = mnesia:table_info(B, size),
    if (S == 0) ->
        case mnesia:delete_table(B) of
        {atomic, ok} ->
            ok;
        {aborted,{no_exists,B}} ->
            ok
        end,
        {ok, _} = eroq_queue_meta:del_bucket(QueueName, B),
        traverse_check_buckets(QueueName, T, Size);
    true ->
        traverse_check_buckets(QueueName, T, Size + S)
    end;
traverse_check_buckets(_, [], Size)-> Size.
-}

traverse_check_buckets :: QName -> [TableName] -> Integer -> Process Integer
traverse_check_buckets _            [] size = return size
traverse_check_buckets queueName (b:t) size = do
  HM.TISize s <- HM.table_info b HM.TableInfoSize
  case s of
    0 -> do
          HM.delete_table b
          meta_del_bucket queueName b
          return ()
    _ -> return ()
  traverse_check_buckets queueName t (size + s)


--------------------------------------------------------------------------------
-- Implementation                                                             --
--------------------------------------------------------------------------------

serverDefinition :: ProcessDefinition State
serverDefinition = defaultProcess {
     apiHandlers = [
          handleCall handleEnqueue
        , handleCall handleReadOp

        , handleCast handleEnqueueCast
        ]
    , infoHandlers =
        [
        -- handleInfo_ (\(ProcessMonitorNotification _ _ r) -> logm $ show r >> continue_)
         handleInfo (\dict (ProcessMonitorNotification _ _ r) -> do {logm $ show r; continue dict })
        ]
     , timeoutHandler = \_ _ -> stop $ ExitOther "timeout az"
     , shutdownHandler = \_ reason -> do { logm $ "HroqQueue terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State

handleReadOp :: State -> ReadOp -> Process (ProcessReply ReadOpReply State)
handleReadOp s readOp = do
    -- {(logm "dequeuing") ; reply () (s) }
    logt $ "handleReadOp starting"
    (res,s') <- do_read_op readOp s
    logt $ "handleReadOp done"
    reply res s'


-- Note: the handlers match on type signature
handleEnqueue :: State -> Enqueue -> Process (ProcessReply () State)
handleEnqueue s (Enqueue q v) = do
    -- logm $ "enqueue called with:" ++ (show (q,v))
    logt $ "handleEnqueue starting"
    s' <- enqueue_one_message q v s

    -- Notify all waiting processes that a new message has arrived

    logm $ "handleEnqueue notifying subsribers:" ++ (show (qsSubscriberPidDict s))
    -- [SPid ! {'$eroq_queue', QueueName, NewServerState#eroq_queue_state.total_queue_size} || {SPid, _} <- dict:to_list(SubsPidDict)],
    -- mapM_ (\(pid,_ref) -> send pid (QueueMessage)) $ Map.toList $ qsSubscriberPidDict s
    mapM_ (\(pid,_ref) -> sendTo pid (QueueMessage)) $ Map.toList $ qsSubscriberPidDict s
    -- {reply, ok, NewServerState#eroq_queue_state{subscriber_pid_dict = dict:new()}};
    logt $ "handleEnqueue done"
    reply () $ s'  {qsSubscriberPidDict = Map.empty}

handleEnqueueCast :: State -> Enqueue -> Process (ProcessAction State)
handleEnqueueCast s (Enqueue q v) = do
    -- logm $ "enqueue called with:" ++ (show (q,v))
    logt $ "handleEnqueueCast starting"
    s' <- enqueue_one_message q v s
    logt $ "handleEnqueueCast done"
    continue s'

{-
handle_call({enqueue, QueueName, Message}, _From, ServerState) when is_record(Message, eroq_message)->
    {ok, NewServerState} = enqueue_one_message(QueueName, Message, ServerState),
    SubsPidDict = ServerState#eroq_queue_state.subscriber_pid_dict,
    [SPid ! {'$eroq_queue', QueueName, NewServerState#eroq_queue_state.total_queue_size} || {SPid, _} <- dict:to_list(SubsPidDict)],
    {reply, ok, NewServerState#eroq_queue_state{subscriber_pid_dict = dict:new()}};
handle_call({enqueue, QueueName, Message}, From, ServerState)->
    ERoqMsg = eroq_util:construct_bucket_message(Message),
    handle_call({enqueue, QueueName,ERoqMsg}, From, ServerState);
-}

-- ---------------------------------------------------------------------
-- Actual workers, not just handlers

enqueue_one_message :: QName -> QValue -> State -> Process State
enqueue_one_message queueName v s = do
  key <- generate_key
  -- logm $ "enqueue_one_message:key=" ++ (show key)
  let msgRecord = QE key v

  let appInfo        = qsAppInfo            s
      procBucket     = qsCurrProcBucket     s
      overflowBucket = qsCurrOverflowBucket s
      maxBucketSize  = qsMaxBucketSize      s
      indexList      = qsIndexList          s
      dequeueCount   = qsDequeueCount       s

  -- logm $ "enqueue_one_message:(procBucket,overflowBucket)=" ++ (show (procBucket,overflowBucket))

  HM.TISize bucketSize <- HM.table_info overflowBucket HM.TableInfoSize
  logm $ "enqueue_one_message:bucketSize=" ++ (show bucketSize)

  logt $ "enqueue_one_message 1"

  enqueueWorkBucket <- if (bucketSize >= maxBucketSize)
    then do
      logm $ "DEBUG: enq - create new bucket size(" ++ (show overflowBucket) ++ ")=" ++ (show bucketSize)
      make_next_bucket queueName
    else do
      return overflowBucket

  logt $ "enqueue_one_message 2"

  -- HM.dirty_write_q enqueueWorkBucket msgRecord
  HM.dirty_write_q_sid (qsMnesiaSid s)  enqueueWorkBucket msgRecord
  let newTotalQueuedMsg = (qsTotalQueueSize s) + 1
      newEnqueueCount   = (qsEnqueueCount s)   + 1
  -- logm $ "enqueue_one_message:write done"
{-
    ok = eroq_log_dumper:dirty(EnqueueWorkBucket),
-}

  s' <- if enqueueWorkBucket == procBucket
          then do
             -- Inserted into processing bucket (head of queue) add key to index list....
               -- logm $ "enqueue_one_message:enqueueWorkBucket==procBucket"
               return $ s { qsTotalQueueSize = newTotalQueuedMsg
                          , qsEnqueueCount = newEnqueueCount
                          , qsIndexList = indexList ++ [key]
                          }
          else
             if enqueueWorkBucket == overflowBucket
               then do
                 -- Inserted into overflow bucket (tail of the queue)...
                 -- logm $ "enqueue_one_message:enqueueWorkBucket==overflowBucket"
                 return $ s { qsTotalQueueSize = newTotalQueuedMsg
                            , qsEnqueueCount   = newEnqueueCount
                            }
               else do
                 -- A new overflow bucket (new tail of queue) was created ....
                 logm $ "enqueue_one_message:new overflow bucket"
                 case procBucket == overflowBucket of
                   True -> do
                        -- logm $ "enqueue_one_message:not really??"
                        return ()
                   False -> do
                        -- logm $ "enqueue_one_message:yes"

                        -- But only swap the previous overflow bucket
                        -- (previous tail) out of ram if it is not the
                        -- head of the queue ....
                        HM.change_table_copy_type overflowBucket HM.DiscOnlyCopies


                 return $ s { qsCurrOverflowBucket = enqueueWorkBucket
                            , qsTotalQueueSize     = newTotalQueuedMsg
                            , qsEnqueueCount       = newEnqueueCount
                            }

  publish_queue_stats queueName (QStats appInfo newTotalQueuedMsg newEnqueueCount dequeueCount)

  return s'


-- ---------------------------------------------------------------------
{-
build_next_bucket_name(QueueName) ->
    Key = eroq_util:generate_key(),
    KeyStr = lists:flatten(io_lib:format("~18.18.0w", [Key])),
    list_to_atom(atom_to_list(QueueName)++"_"++KeyStr).
-}
build_next_bucket_name :: QName -> Process TableName
build_next_bucket_name (QN queueName) = do
    (QK key) <- generate_key
    let queueName' = (queueName ++ "_" ++ key)
    return $ TN queueName'

-- ---------------------------------------------------------------------

{-
make_next_bucket(QueueName) ->
    NewBucket = build_next_bucket_name(QueueName),
    {ok, _} = eroq_queue_meta:add_bucket(QueueName, NewBucket),
    ok = create_table(disc_copies, NewBucket),
    NewBucket.
-}

make_next_bucket :: QName -> Process TableName
make_next_bucket queueName = do
  newBucket <- build_next_bucket_name queueName
  _ <- meta_add_bucket queueName newBucket
  create_table HM.DiscCopies newBucket
  return newBucket


-- ---------------------------------------------------------------------

do_read_op :: ReadOp -> State -> Process (ReadOpReply,State)
do_read_op op s = do
    logm $ "do_read_op:" ++ (show op)
{-
handle_call({read, Action}, _From,  #eroq_queue_state{total_queue_size = Qs} = ServerState) ->
-}
    let qs = qsTotalQueueSize s
{-
    if (Qs == 0) ->
-}

    (res,s') <- if qs == 0
      then
        case op of
          ReadOpDequeue _q _worker mpid -> do
            case mpid of
              Just pid -> do
                let subsPidDict = qsSubscriberPidDict s
                case (Map.member pid subsPidDict) of
                  True -> do
                    -- Already in the list of subs ...
                    return (ReadOpReplyEmpty,s)
                  False -> do
                    monRef <- monitor pid
                    let newSubsPidDics = Map.insert pid monRef subsPidDict
                    logm $ "HroqQueue.do_read_op:qsSubscriberPidDict=" ++ (show newSubsPidDics)
                    return (ReadOpReplyEmpty,s { qsSubscriberPidDict = newSubsPidDics })
              Nothing ->  do return (ReadOpReplyEmpty,s)
          _ -> do return (ReadOpReplyEmpty,s)
{-
        case Action of
        {dequeue, _, _, _, SubscribeIfEmptyPid} ->
            if is_pid(SubscribeIfEmptyPid) ->
                MonRef = monitor(process, SubscribeIfEmptyPid),
                SubsPidDict = ServerState#eroq_queue_state.subscriber_pid_dict,
                case dict:find(SubscribeIfEmptyPid, SubsPidDict) of
                {ok, _} ->
                    %Already in the list of subs ...
                    {reply, {error, empty}, ServerState};
                _ ->
                    NewSubsPidDict = dict:store(SubscribeIfEmptyPid, MonRef, SubsPidDict),
                    {reply, {error, empty}, ServerState#eroq_queue_state{subscriber_pid_dict = NewSubsPidDict}}
                end;
            true ->
                {reply, {error, empty}, ServerState}
            end;
        _ ->
            {reply, {error, empty}, ServerState}
        end;
-}
      else do
{-
    true ->

        QueueName      = ServerState#eroq_queue_state.queue_name,
        AppInfo        = ServerState#eroq_queue_state.app_info,
        ProcBucket     = ServerState#eroq_queue_state.curr_proc_bucket,
        OverflowBucket = ServerState#eroq_queue_state.curr_overflow_bucket,
        EnqueueCount   = ServerState#eroq_queue_state.enqueue_count,

        [Key | T]      = ServerState#eroq_queue_state.index_list,
-}
        let
          queueName      = qsQueueName          s
          appInfo        = qsAppInfo            s
          procBucket     = qsCurrProcBucket     s
          overflowBucket = qsCurrOverflowBucket s
          enqueueCount   = qsEnqueueCount       s

          (key:t)        = qsIndexList          s

        case op of
          ReadOpDequeue q worker _mpid -> do
            r <- process_the_message key procBucket worker
            case r of
              Right () -> do
                let
                  newTotalQueuedMsg = (qsTotalQueueSize s) - 1
                  newDequeueCount   = (qsDequeueCount   s) + 1

                (newProcBucket,newIndexList) <-
                  case t of
                    [] -> do
                      if procBucket == overflowBucket
                        then return (procBucket,[])
                        else do
                          HM.delete_table procBucket
                          (nextBucket:_) <- meta_del_bucket queueName procBucket
                          HM.change_table_copy_type nextBucket HM.DiscCopies
                          indexList <- HM.dirty_all_keys nextBucket
                          return (nextBucket, sort indexList)
                    _ -> do return (procBucket,t)

                publish_queue_stats queueName (QStats appInfo newTotalQueuedMsg enqueueCount newDequeueCount)

                let s' = s { qsCurrProcBucket = newProcBucket
                           , qsIndexList      = newIndexList
                           , qsTotalQueueSize = newTotalQueuedMsg
                           , qsDequeueCount   = newDequeueCount }
                return (ReadOpReplyOk, s')
              Left err -> do
                return (ReadOpReplyError err, s)

          ReadOpPeek q -> do
            res <- do_peek key procBucket
            return (res,s)
{-
        case Action of
        {dequeue, WorkerModule, WorkerFunc, WorkerParams, _} ->

            case process_the_message(Key, ProcBucket, WorkerModule, WorkerFunc, WorkerParams) of
            ok ->

                NewTotalQueuedMsg = ServerState#eroq_queue_state.total_queue_size-1,
                NewDequeueCount   = ServerState#eroq_queue_state.dequeue_count+1,

                {NewProcBucket, NewIndexList} =
                case T of
                [] ->
                    if (ProcBucket == OverflowBucket) ->
                        {ProcBucket, []};
                    true ->
                        ok = eroq_util:retry_delete_table(10, ProcBucket),
                        {ok, [NextBucket | _]} = eroq_queue_meta:del_bucket(QueueName, ProcBucket),
                        ok = change_table_copy_type(NextBucket, disc_copies),
                        {NextBucket, lists:sort(mnesia:dirty_all_keys(NextBucket))}
                    end;
                _ ->
                    {ProcBucket, T}
                end,

                catch(eroq_stats_gatherer:publish_queue_stats(QueueName, {AppInfo, NewTotalQueuedMsg, EnqueueCount, NewDequeueCount})),

                {reply, ok, ServerState#eroq_queue_state{curr_proc_bucket = NewProcBucket, index_list = NewIndexList, total_queue_size = NewTotalQueuedMsg, dequeue_count = NewDequeueCount}};

            {error, Reason} ->
                {reply, {error, Reason}, ServerState}
            end;

        peek ->
            {reply, do_peek(Key, ProcBucket), ServerState}
        end

    end;

-}
    return (res,s')

-- ---------------------------------------------------------------------
{-
do_peek(Key, SourceBucket) ->
    case catch(eroq_util:retry_dirty_read(10, SourceBucket, Key)) of
    {ok, [#eroq_message{data = Message}]} ->
        {ok, Message};
    Reason ->
        {error, Reason}
    end.
-}

do_peek :: QKey -> TableName -> Process ReadOpReply
do_peek key sourceBucket = do
  res <- HM.dirty_read_q sourceBucket key
  logm $ "HroqQueue.do_peek:res=" ++ (show res)
  case res of
    Just msg -> return (ReadOpReplyMsg msg)
    Nothing  -> return (ReadOpReplyError (error $ "do_peek: read (sourceBucket,key) failed for " ++ (show (sourceBucket,key)) ))

-- ---------------------------------------------------------------------

{-
process_the_message(Key, SourceBucket, WorkerModule, WorkerFunc, WorkerParams) ->
    case catch(eroq_util:retry_dirty_read(10, SourceBucket, Key)) of
    {ok, [#eroq_message{id = Key, data = Message}]} ->
        case catch(WorkerModule:WorkerFunc(Key, Message, WorkerParams)) of
        ok ->
            ok = eroq_util:retry_dirty_delete(10, SourceBucket, Key);
        Reason ->
            {error, Reason}
        end;
    {ok, []} ->
        ?warn({process_the_message, "message not found", Key, SourceBucket}),
        ok;
    {error, Reason} ->
        {error, Reason}
    end.

-}


process_the_message :: QKey -> TableName -> Closure WorkerFunc -> Process (Either String ())
process_the_message key sourceBucket worker = do
  logm $ "process_the_message:(key,sourceBucket)=" ++ (show (key,sourceBucket))

  res <- HM.dirty_read_q sourceBucket key
  logm $ "process_the_message:res=" ++ (show res)
  case res of
    Just msg -> do
      logm $ "process_the_message: about to process.."
      f <- unClosure worker
      r <- f msg
      logm $ "process_the_message:r=" ++ (show r)
      case r of
        Right () -> do
          HM.dirty_delete_q sourceBucket key
          return $ Right ()
        Left err -> return (Left err)
    Nothing  -> do
      return (Left "process_the_message:res==Nothing")


-- ---------------------------------------------------------------------
{-
%worker piggies
create_table(Type, TableName) ->
    case mnesia:create_table(TableName, [{attributes, record_info(fields, eroq_message)}, {type, set}, {Type, [node()]}, {record_name, eroq_message}]) of
    {atomic, ok} ->
        ok;
    {aborted,{already_exists,TableName}} ->
        case mnesia:table_info(TableName, storage_type) of
        Type ->
            ok;
        _ ->
            retry_change_table_copy_type(TableName, Type, 10)
        end;
    Error ->
        %io:format("DEBUG create_table error ~p~n", [Error]),
        Error
    end.


-}

create_table :: HM.TableStorage -> TableName -> Process ()
create_table storage tableName = do
  HM.create_table storage tableName HM.RecordTypeQueueEntry
  logm "HroqQueue.create_table:not checking storage type"

-- ---------------------------------------------------------------------
