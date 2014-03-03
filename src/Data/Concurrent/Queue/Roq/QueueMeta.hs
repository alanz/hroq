{-# LANGUAGE DeriveDataTypeable #-}

module Data.Concurrent.Queue.Roq.QueueMeta
  (
    meta_add_bucket
  , meta_all_buckets
  , meta_del_bucket

  , hroq_queue_meta_table
  )
  where

import Control.Distributed.Process hiding (call)
import Data.List
import Data.Concurrent.Queue.Roq.Hroq
import Data.Concurrent.Queue.Roq.Logger
import Data.Concurrent.Queue.Roq.Mnesia
import Data.Concurrent.Queue.Roq.Util

-- ---------------------------------------------------------------------

hroq_queue_meta_table :: TableName
hroq_queue_meta_table = TN "hroq_queue_meta_table"

-- ---------------------------------------------------------------------
{-
-spec add_bucket(atom(), atom()) -> ok | {error, any()}.
add_bucket(QName, BucketId) ->
    case eroq_util:retry_dirty_read(10, eroq_queue_meta_table, QName) of
    {ok, []} ->
        case eroq_util:retry_dirty_write(10, eroq_queue_meta_table, #eroq_queue_meta{qid=QName, buckets = [BucketId], timestamp = now()}) of
        ok ->
            {ok, [BucketId]};
        Error ->
            Error
        end;
    {ok, [#eroq_queue_meta{buckets = B} = Meta] } ->
        NewBuckets = B ++ [BucketId],
        case eroq_util:retry_dirty_write(10, eroq_queue_meta_table, Meta#eroq_queue_meta{buckets = NewBuckets}) of
        ok ->
            {ok, NewBuckets};
        Error ->
            Error
        end;
    Error ->
        Error
    end.

-}
meta_add_bucket :: QName -> TableName -> Process [TableName]
meta_add_bucket queueName bucket = do
  -- logm $ "HroqQueueMeta:meta_add_bucket:" ++ (show (queueName,bucket))
  rv <- retry_dirty_read retryCnt hroq_queue_meta_table queueName
  case rv of
    Nothing -> do
       timestamp <- getTimeStamp
       retry_dirty_write retryCnt hroq_queue_meta_table  (MAllBuckets queueName [bucket] timestamp)
       return [bucket]
    Just (MAllBuckets _ b _) -> do
       let newBuckets = b ++ [bucket]
       timestamp <- getTimeStamp
       retry_dirty_write retryCnt hroq_queue_meta_table  (MAllBuckets queueName newBuckets timestamp)
       return newBuckets

retryCnt :: Integer
retryCnt = 10

-- ---------------------------------------------------------------------
{-
-spec all_buckets(atom()) -> {ok, list(atom())} | {error, any()}.
all_buckets(QName) ->
    case eroq_util:retry_dirty_read(10, eroq_queue_meta_table, QName) of
    {ok, []} ->
        {ok, []};
    {ok, [#eroq_queue_meta{buckets = B}] } ->
        {ok, B};
    Error ->
        Error
    end.
-}
meta_all_buckets :: QName -> Process [TableName]
meta_all_buckets queueName = do
  -- logm $ "HroqQueueMeta:meta_all_buckets :" ++ (show queueName)
  v <- dirty_read hroq_queue_meta_table queueName
  -- logm $ "HroqQueueMeta:meta_all_buckets v:" ++ (show v)
  case v of
    Nothing -> return []
    Just (MAllBuckets _ b _) -> return b

-- ---------------------------------------------------------------------

meta_del_bucket :: MetaKey -> TableName -> Process [TableName]
meta_del_bucket queueName bucket = do
  logm "HroqQueueMeta:meta_del_bucket undefined"
  r <- dirty_read hroq_queue_meta_table queueName
  case r of
    Just (MAllBuckets _ b _) -> do
      let newBuckets = b \\ [bucket]
      timestamp <- getTimeStamp
      retry_dirty_write retryCnt hroq_queue_meta_table (MAllBuckets queueName newBuckets timestamp)
      return newBuckets
    Nothing -> do

      logm $ "HroqQueueMeta:meta_del_bucket (queueName,bucket) failed for " ++ (show (queueName,bucket))
      return []

{-
-spec del_bucket(atom(), atom()) -> ok | {error, any()}.
del_bucket(QName, BucketId) ->
    case eroq_util:retry_dirty_read(10, eroq_queue_meta_table, QName) of
    {ok, []} ->
        {ok, []};
    {ok, [#eroq_queue_meta{buckets = B} = Meta] } ->
        NewBuckets = lists:delete(BucketId, B),
        case eroq_util:retry_dirty_write(10, eroq_queue_meta_table, Meta#eroq_queue_meta{buckets = NewBuckets}) of
        ok ->
            {ok, NewBuckets};
        Error ->
            Error
        end;
    Error ->
        Error
    end.
-}


