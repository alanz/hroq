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
import Data.HroqGroups
import Data.HroqMnesia
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

data Enqueue = Enqueue QName QValue
               deriving (Typeable, Show)
data Dequeue = Dequeue QName QWorker (Maybe ProcessId)
               deriving (Typeable, Show)-- Remove one entry

$(derive makeBinary ''QName)
-- $(derive makeBinary ''QWorker) -- Need a closure, actually
$(derive makeBinary ''Enqueue)
$(derive makeBinary ''Dequeue)

type CleanupFunc = String

data State = QueueState
   { qsAppInfo            :: String
   , qsCurrProcBucket     :: TableName
   , qsCurrOverflowBucket :: TableName
   , qsTotalQueueSize     :: Integer
   , qsEnqueueCount       :: Integer
   , qsDequeueCount       :: Integer
   , qsMaxBucketSize      :: Integer
   , qsQueueName          :: QName
   , qsDoCleanup          :: CleanupFunc -- Should be a function eventually?
   , qsIndexList          :: [QKey]
   , qsSubscriberPidDict  :: Set.Set ProcessId
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

-- -define(MAX_BUCKET_SIZE,  eroq_util:app_param(max_bucket_size, 5000)).
-- maxBucketSize = 5000
maxBucketSizeConst = 5

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

    say $ "HroqQueue:initFunc started"

    -- process_flag(trap_exit, true),

    -- ok = mnesia:wait_for_tables([eroq_queue_meta_table], infinity),
    wait_for_tables [eroq_queue_meta_table] Infinity
    say $ "HroqQueue:initFunc 1"

    -- Run through the control table & delete all empty buckets ...
    -- {QueueSize, Buckets} = check_buckets(QueueName),
    (queueSize,buckets) <- check_buckets queueName
    say $ "HroqQueue:initFunc 2:(queueSize,buckets)=" ++ (show (queueSize,buckets))

    -- ok = mnesia:wait_for_tables(Buckets, infinity),
    wait_for_tables buckets Infinity
    say $ "HroqQueue:initFunc 3"

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
    case currProcBucket of
      currOverflowBucket -> do
        change_table_copy_type currProcBucket DiscCopies
      _ -> do
        change_table_copy_type currProcBucket     DiscCopies
        change_table_copy_type currOverflowBucket DiscCopies
    say $ "HroqQueue:initFunc 4"


    allKeys <- dirty_all_keys currProcBucket
    say $ "HroqQueue:initFunc 5"

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
    say $ "HroqQueue:initFunc 6"

    -- catch(eroq_stats_gatherer:publish_queue_stats(QueueName, {AppInfo, QueueSize, 0, 0})),
    publish_queue_stats queueName (appInfo, queueSize, 0, 0)

    say $ "HroqQueue:initFunc ending"

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
  say $ "check_buckets:" ++ (show queueName)
  mab <- meta_all_buckets queueName
  say $ " check_buckets:mab=" ++ (show mab)
  case mab of
    [b] -> do
      TIStorageType storage <- table_info b TableInfoStorageType
      case storage of
        DiscCopies -> do
            (TISize size) <- table_info b TableInfoSize
            return (size,[b])
        DiscOnlyCopies -> do
            (TISize size) <- table_info b TableInfoSize
            return (size,[b])
        StorageNone -> do
            nb <- make_next_bucket queueName
            return (0,[nb])
    bs -> do
      size <- traverse_check_buckets queueName bs 0
      mab' <- meta_all_buckets queueName
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
  TISize s <- table_info b TableInfoSize
  case s of
    0 -> do
          delete_table b
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
        -- handleInfo_ (\(ProcessMonitorNotification _ _ r) -> say $ show r >> continue_)
         handleInfo (\dict (ProcessMonitorNotification _ _ r) -> do {say $ show r; continue dict })
        ]
     , timeoutHandler = \_ _ -> stop $ TerminateOther "timeout az"
    } :: ProcessDefinition State

-- Note: the handlers match on type signature
handleEnqueue :: State -> Enqueue -> Process (ProcessReply State ())
handleEnqueue s (Enqueue q v) = do
    say $ "enqueue called with:" ++ (show (q,v))
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
handleDequeue s (Dequeue q w mp) = do {(say "dequeuing") ; reply () (s) }


-- ---------------------------------------------------------------------
-- Actual workers, not just handlers

enqueue_one_message :: QName -> QValue -> State -> Process State
enqueue_one_message queueName v s = do
  key <- generate_key
  say $ "enqueue_one_message:key=" ++ (show key)
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

  say $ "enqueue_one_message:(procBucket,overflowBucket)=" ++ (show (procBucket,overflowBucket))

  TISize bucketSize <- table_info overflowBucket TableInfoSize
  say $ "enqueue_one_message:bucketSize=" ++ (show bucketSize)

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
      say $ "DEBUG: enq - create new bucket size(" ++ (show overflowBucket) ++ ")=" ++ (show bucketSize)
      make_next_bucket queueName
    else do
      say $ "enqueue_one_message:using existing bucket:" ++ (show (bucketSize,maxBucketSize))
      return overflowBucket
  say $ "enqueue_one_message:enqueueWorkBucket=" ++ (show enqueueWorkBucket)

{-
    ok = eroq_util:retry_dirty_write(10, EnqueueWorkBucket, MsgRecord),

    NewTotalQueuedMsg = ServerState#eroq_queue_state.total_queue_size+1,
    NewEnqueueCount   = ServerState#eroq_queue_state.enqueue_count+1,

    ok = eroq_log_dumper:dirty(EnqueueWorkBucket),
-}
  -- retry_dirty_write (10::Integer) enqueueWorkBucket msgRecord
  dirty_write_q enqueueWorkBucket msgRecord
  let newTotalQueuedMsg = (qsTotalQueueSize s) + 1
      newEnqueueCount   = (qsEnqueueCount s)   + 1
  say $ "enqueue_one_message:write done"

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
               say $ "enqueue_one_message:enqueueWorkBucket==procBucket"
               return $ s { qsTotalQueueSize = newTotalQueuedMsg
                          , qsEnqueueCount = newEnqueueCount
                          , qsIndexList = indexList ++ [key]
                          }
          else
             if enqueueWorkBucket == overflowBucket
               then do
                 -- Inserted into overflow bucket (tail of the queue)...
                 say $ "enqueue_one_message:enqueueWorkBucket==overflowBucket"
                 return $ s { qsTotalQueueSize = newTotalQueuedMsg
                            , qsEnqueueCount   = newEnqueueCount
                            }
               else do
                 -- A new overflow bucket (new tail of queue) was created ....
                 say $ "enqueue_one_message:new overflow bucket"
                 case procBucket == overflowBucket of
                   True -> do
                        say $ "enqueue_one_message:not really??"
                        return ()
                   False -> do
                        say $ "enqueue_one_message:yes"

                        -- But only swap the previous overflow bucket
                        -- (previous tail) out of ram if it is not the
                        -- head of the queue ....
                        change_table_copy_type overflowBucket DiscOnlyCopies


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
  create_table DiscCopies newBucket
  return newBucket


-- ---------------------------------------------------------------------
