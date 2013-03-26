{-# LANGUAGE DeriveDataTypeable   #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances    #-}
module Data.HroqQueue
  (
    enqueue
  , dequeue

  , startQueues
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
import Data.Typeable (Typeable)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
-- import qualified Data.ByteString.Lazy as B
import qualified Data.Map as Map
import Data.Hroq

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

data ProcBucket = PB Int
data OverflowBucket = OB Int

data State = QueueState
   { qsAppInfo :: String
   , qsCurrProcBucket :: ProcBucket
   , qsCurrOverflowBucket :: OverflowBucket
   , qsTotalQueueSize :: Integer
   , qsEnqueueCount :: Integer
   , qsDequeueCount :: Integer
   , qsMaxBucketSize :: Integer
   , qsQueueName :: QName
   , qsDoCleanup :: String -- Should be a function eventually?
   , qsIndexList :: [ProcBucket]
   , qsSubscriberPidDict :: String -- Must be something else in time
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
startQueues :: State -> Process ProcessId
startQueues startState =
  let server = serverDefinition
  in spawnLocal $ start startState init' server >> return ()
  where init' :: InitHandler State State
        init' s = return $ InitOk s Infinity

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
enqueue_one_message q v s = do
  return s

{-
    AppInfo        = ServerState#eroq_queue_state.app_info,
    ProcBucket     = ServerState#eroq_queue_state.curr_proc_bucket,
    OverflowBucket = ServerState#eroq_queue_state.curr_overflow_bucket,
    MaxBucketSize  = ServerState#eroq_queue_state.max_bucket_size,
    IndexList      = ServerState#eroq_queue_state.index_list,

    DequeueCount   = ServerState#eroq_queue_state.dequeue_count,

    BucketSize = mnesia:table_info(OverflowBucket, size),

    EnqueueWorkBucket = 
    if BucketSize >= MaxBucketSize ->
        %io:format("DEBUG: enq - create new bucket size(~p) = ~w ~n", [OverflowBucket, BucketSize]),
        make_next_bucket(QueueName);
    true ->
        OverflowBucket
    end,
    
    ok = eroq_util:retry_dirty_write(10, EnqueueWorkBucket, MsgRecord),
    
    NewTotalQueuedMsg = ServerState#eroq_queue_state.total_queue_size+1,
    NewEnqueueCount   = ServerState#eroq_queue_state.enqueue_count+1,

    ok = eroq_log_dumper:dirty(EnqueueWorkBucket),

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

    catch(eroq_stats_gatherer:publish_queue_stats(QueueName, {AppInfo, NewTotalQueuedMsg, NewEnqueueCount, DequeueCount})),

    {ok, NewServerState}.

-}

