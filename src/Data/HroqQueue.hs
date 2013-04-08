{-# LANGUAGE DeriveDataTypeable   #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances    #-}
module Data.HroqQueue
  (
    enqueue
  , dequeue

  , startQueue

  -- * debug
  , enqueue_one_message
  )
  where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Data.Binary
import Data.DeriveTH
import Data.List
import Data.Typeable (Typeable)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
-- import qualified Data.ByteString.Lazy as B
import qualified Data.Map as Map
import qualified Data.Set as Set
import Data.Hroq
import Data.HroqLogger
import Data.HroqGroups
import qualified Data.HroqMnesia as HM
import Data.HroqQueueMeta
import Data.HroqStatsGatherer
import Data.HroqUtil


--------------------------------------------------------------------------------
-- Types                                                                      --
--------------------------------------------------------------------------------

-- Call and Cast request types. Response types are unnecessary as the GenProcess
-- API uses the Async API, which in turn guarantees that an async handle can
-- /only/ give back a reply for that *specific* request through the use of an
-- anonymous middle-man (as the sender and reciever in our case).


-- type QWorker = (QValue -> QValue)
type QWorker = (Int) -- Keep it simple while sorting the plumbing
-- instance Show QWorker where
--   show _ = "QWorker"

-- operations to be exposed

data Enqueue = Enqueue !QName !QValue
               deriving (Typeable, Show)
data Dequeue = Dequeue !QName !QWorker (Maybe ProcessId)
               deriving (Typeable, Show)-- Remove one entry

{-
$(derive makeBinary ''QName)
-- $(derive makeBinary ''QWorker) -- Need a closure, actually
$(derive makeBinary ''Enqueue)
$(derive makeBinary ''Dequeue)
-}

instance Binary Enqueue where
  put (Enqueue n v) = put n >> put v
  get = do
    n <- get
    v <- get
    return (Enqueue n v)

instance Binary Dequeue where
  put (Dequeue n w mpid) = put n >> put w >> put mpid
  get = do
    n <- get
    w <- get
    mpid <- get
    return (Dequeue n w mpid)



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
   , qsSubscriberPidDict  :: !(Set.Set ProcessId)
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
-- API                                                                        --
--------------------------------------------------------------------------------

-- |Add an item to a queue
enqueue :: ProcessId -> QName -> QValue -> Process ()
enqueue sid q v = call sid (Enqueue q v)

-- dequeue(QueueName, WorkerModule, WorkerFunc, WorkerParams)  when is_atom(QueueName)->
--     dequeue(QueueName, WorkerModule, WorkerFunc, WorkerParams, undefined).

-- dequeue(QueueName, WorkerModule, WorkerFunc, WorkerParams, SubscribeIfEmptyPid)  when is_atom(QueueName) and (is_pid(SubscribeIfEmptyPid) or (SubscribeIfEmptyPid == undefined)) ->
--     gen_server:call(?NAME(QueueName), {read, {dequeue, WorkerModule, WorkerFunc, WorkerParams, SubscribeIfEmptyPid}}, infinity).

dequeue :: ProcessId -> QName -> QWorker -> Maybe ProcessId -> Process ()
dequeue sid q w mp = call sid (Dequeue q w mp)


-- TODO: erlang version starts a Q gen server per Q, uses the QName to
-- lookup in the global process dict where it is.
-- | Start a Queue server
startQueue :: (QName,String,CleanupFunc) -> Process ProcessId
startQueue initParams =
  let server = serverDefinition
  in spawnLocal $ start initParams initFunc server >> return ()

-- -----------------------------------------------------------------------------

-- |Init callback
initFunc :: InitHandler (QName,String,CleanupFunc) State
initFunc (queueName,appInfo,doCleanup) = do

    logm $ "HroqQueue:initFunc started"

    -- process_flag(trap_exit, true),

    -- ok = mnesia:wait_for_tables([eroq_queue_meta_table], infinity),
    HM.wait_for_tables [hroq_queue_meta_table] Infinity
    logm $ "HroqQueue:initFunc 1"

    -- Run through the control table & delete all empty buckets ...
    -- {QueueSize, Buckets} = check_buckets(QueueName),
    (queueSize,buckets) <- check_buckets queueName
    logm $ "HroqQueue:initFunc 2:(queueSize,buckets)=" ++ (show (queueSize,buckets))

    -- ok = mnesia:wait_for_tables(Buckets, infinity),
    HM.wait_for_tables buckets Infinity
    logm $ "HroqQueue:initFunc 3"

    -- [CurrProcBucket | _] = Buckets,
    -- CurrOverflowBucket   = lists:last(Buckets),
    let currProcBucket     = head buckets
        currOverflowBucket = last buckets
        -- NOTE: these two buckets may be the same

{-
    case CurrProcBucket of
    CurrOverflowBucket ->
        ok = change_table_copy_type(CurrProcBucket, disc_copies);
    _ ->
        ok = change_table_copy_type(CurrProcBucket,     disc_copies),
        ok = change_table_copy_type(CurrOverflowBucket, disc_copies)
    end,
-}
    if currProcBucket == currOverflowBucket 
      then do
        HM.change_table_copy_type currProcBucket HM.DiscCopies
      else do
        HM.change_table_copy_type currProcBucket     HM.DiscCopies
        HM.change_table_copy_type currOverflowBucket HM.DiscCopies
    logm $ "HroqQueue:initFunc 4"


    allKeys <- HM.dirty_all_keys currProcBucket
    logm $ "HroqQueue:initFunc 5"

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
             , qsSubscriberPidDict  = Set.empty
             }


    -- eroq_groups:join(QueueName, ?MODULE),                
    join queueName "HroqQueue"
    logm $ "HroqQueue:initFunc 6"

    -- catch(eroq_stats_gatherer:publish_queue_stats(QueueName, {AppInfo, QueueSize, 0, 0})),
    publish_queue_stats queueName (appInfo, queueSize, 0, 0)

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
        , handleCall handleDequeue
        ]
    , infoHandlers =
        [
        -- handleInfo_ (\(ProcessMonitorNotification _ _ r) -> logm $ show r >> continue_)
         handleInfo (\dict (ProcessMonitorNotification _ _ r) -> do {logm $ show r; continue dict })
        ]
     , timeoutHandler = \_ _ -> stop $ TerminateOther "timeout az"
     , terminateHandler = \_ reason -> do { logm $ "HroqQueue terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State

-- Note: the handlers match on type signature
handleEnqueue :: State -> Enqueue -> Process (ProcessReply State ())
handleEnqueue s (Enqueue q v) = do
    -- logm $ "enqueue called with:" ++ (show (q,v))
    s' <- enqueue_one_message q v s
    reply () s'

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


handleDequeue :: State -> Dequeue -> Process (ProcessReply State ())
handleDequeue s (Dequeue q w mp) = do {(logm "dequeuing") ; reply () (s) }


-- ---------------------------------------------------------------------
-- Actual workers, not just handlers

enqueue_one_message :: QName -> QValue -> State -> Process State
enqueue_one_message queueName v s = do
  key <- generate_key
  -- logm $ "enqueue_one_message:key=" ++ (show key)
  let msgRecord = QE key v

{-
    AppInfo        = ServerState#eroq_queue_state.app_info,
    ProcBucket     = ServerState#eroq_queue_state.curr_proc_bucket,
    OverflowBucket = ServerState#eroq_queue_state.curr_overflow_bucket,
    MaxBucketSize  = ServerState#eroq_queue_state.max_bucket_size,
    IndexList      = ServerState#eroq_queue_state.index_list,
-}
  let appInfo        = qsAppInfo            s
      procBucket     = qsCurrProcBucket     s
      overflowBucket = qsCurrOverflowBucket s
      maxBucketSize  = qsMaxBucketSize      s
      indexList      = qsIndexList          s


{-
    DequeueCount   = ServerState#eroq_queue_state.dequeue_count,

    BucketSize = mnesia:table_info(OverflowBucket, size),
-}
      dequeueCount = qsDequeueCount s

  -- logm $ "enqueue_one_message:(procBucket,overflowBucket)=" ++ (show (procBucket,overflowBucket))

  HM.TISize bucketSize <- HM.table_info overflowBucket HM.TableInfoSize
  logm $ "enqueue_one_message:bucketSize=" ++ (show bucketSize)

{-
    EnqueueWorkBucket =
    if BucketSize >= MaxBucketSize ->
        %io:format("DEBUG: enq - create new bucket size(~p) = ~w ~n", [OverflowBucket, BucketSize]),
        make_next_bucket(QueueName);
    true ->
        OverflowBucket
    end,
-}
  enqueueWorkBucket <- if (bucketSize >= maxBucketSize)
    then do
      logm $ "DEBUG: enq - create new bucket size(" ++ (show overflowBucket) ++ ")=" ++ (show bucketSize)
      make_next_bucket queueName
    else do
      -- logm $ "enqueue_one_message:using existing bucket:" ++ (show (bucketSize,maxBucketSize))
      return overflowBucket
  -- logm $ "enqueue_one_message:enqueueWorkBucket=" ++ (show enqueueWorkBucket)

{-
    ok = eroq_util:retry_dirty_write(10, EnqueueWorkBucket, MsgRecord),

    NewTotalQueuedMsg = ServerState#eroq_queue_state.total_queue_size+1,
    NewEnqueueCount   = ServerState#eroq_queue_state.enqueue_count+1,

    ok = eroq_log_dumper:dirty(EnqueueWorkBucket),
-}
  -- retry_dirty_write (10::Integer) enqueueWorkBucket msgRecord
  HM.dirty_write_q enqueueWorkBucket msgRecord
  let newTotalQueuedMsg = (qsTotalQueueSize s) + 1
      newEnqueueCount   = (qsEnqueueCount s)   + 1
  -- logm $ "enqueue_one_message:write done"

{-
    NewServerState =
    if (EnqueueWorkBucket == ProcBucket) ->

        %Inserted into processing bucket (head of queue) add key to index list....
        ServerState#eroq_queue_state   {
                                       total_queue_size     = NewTotalQueuedMsg,
                                       enqueue_count        = NewEnqueueCount,
                                       index_list           = lists:append(IndexList, [MsgRecord#eroq_message.id])
                                       };

    (EnqueueWorkBucket == OverflowBucket) ->

        %Inserted into overflow bucket (tail of the queue)...

        ServerState#eroq_queue_state    {
                                        total_queue_size     = NewTotalQueuedMsg,
                                        enqueue_count        = NewEnqueueCount
                                        };


    true ->

        %A new overflow bucket (new tail of queue) was created ....

        if (ProcBucket =/= OverflowBucket) ->
            %But only swap the previous overflow bucket (previous tail) out of ram if it is not the head of the queue ....
            ok = change_table_copy_type(OverflowBucket, disc_only_copies);

        true ->
            ok

        end,

        ServerState#eroq_queue_state    {
                                        curr_overflow_bucket = EnqueueWorkBucket,
                                        total_queue_size     = NewTotalQueuedMsg,
                                        enqueue_count        = NewEnqueueCount
                                        }

    end,
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

{-
    catch(eroq_stats_gatherer:publish_queue_stats(QueueName, {AppInfo, NewTotalQueuedMsg, NewEnqueueCount, DequeueCount})),

    {ok, NewServerState}.
-}
  publish_queue_stats queueName (appInfo,newTotalQueuedMsg,newEnqueueCount,dequeueCount)

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
  meta_add_bucket queueName newBucket
  create_table HM.DiscCopies newBucket
  return newBucket


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
