{-# LANGUAGE DeriveDataTypeable #-}

module Data.HroqQueueMeta
  (
    meta_add_bucket
  , meta_all_buckets
  , meta_del_bucket

  , eroq_queue_meta_table
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
import Data.HroqStatsGatherer
import Data.HroqUtil
import Data.TCache.Defs
import qualified Data.ByteString.Lazy.Char8 as C8

-- ---------------------------------------------------------------------

data Meta = MAllBuckets QName [TableName] TimeStamp
            deriving (Show,Read,Typeable)
 
instance Indexable Meta where
  key (MAllBuckets qn _ _) = show qn

instance Serializable Meta where
   serialize s  = C8.pack $ show s
   deserialize = read. C8.unpack

-- ---------------------------------------------------------------------

eroq_queue_meta_table :: TableName
eroq_queue_meta_table = TN "eroq_queue_meta_table"

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
  say $ "meta_add_bucket:" ++ (show (queueName,bucket))
  rv <- retry_dirty_read 10 eroq_queue_meta_table (MAllBuckets queueName [] nullTimeStamp)
  case rv of
    Nothing -> do 
       timestamp <- getTimeStamp
       retry_dirty_write 10  eroq_queue_meta_table  (MAllBuckets queueName [bucket] timestamp)
       return [bucket]
    Just (MAllBuckets _ b _) -> do 
       let newBuckets = b ++ [bucket]
       timestamp <- getTimeStamp
       retry_dirty_write 10  eroq_queue_meta_table  (MAllBuckets queueName newBuckets timestamp)
       return newBuckets

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
  say $ "meta_all_buckets :" ++ (show queueName)
  v <- dirty_read eroq_queue_meta_table (MAllBuckets queueName [] nullTimeStamp)
  say $ "meta_all_buckets v:" ++ (show v)
  case v of
    Nothing -> return []
    Just (MAllBuckets _ b _) -> return b

-- ---------------------------------------------------------------------

meta_del_bucket queueName bucket = do
  say "meta_del_bucket undefined"

