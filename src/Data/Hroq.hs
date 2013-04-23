{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Data.Hroq
  (
    ConsumerName(..)
  , ConsumerMessage(..)
  , DlqMessage(..)
  , Payload(..)
  , QKey(..)
  , consumerNameKey
  , QValue(..)
  , QEntry(..)
  , QName(..)
  , MetaKey(..)
  , Meta(..)
  , TableName(..)
  , TimeStamp
  , ioGetTimeStamp
  , getTimeStamp
  , nullTimeStamp
  ,  maxBucketSizeConst

  -- *bucket types
  , ProcBucket(..)
  , OverflowBucket(..)
  )
  where

import Control.Concurrent
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Monad(when,replicateM,foldM,liftM5,liftM4,liftM3,liftM2,liftM)
import Data.Binary
import Data.HroqLogger
import Data.Maybe
import Data.RefSerialize
import Data.Time.Clock
import Data.Typeable
import qualified Data.ByteString.Lazy.Char8 as C8
import qualified Data.Map as Map

-- ---------------------------------------------------------------------

-- -define(MAX_BUCKET_SIZE,  eroq_util:app_param(max_bucket_size, 5000)).
-- maxBucketSize = 5000
-- maxBucketSizeConst = 5
maxBucketSizeConst = 50
-- maxBucketSizeConst = 500

-- ---------------------------------------------------------------------

data ConsumerName = CN !String
                    deriving (Eq,Show,Read,Typeable)
instance Binary ConsumerName where
  put (CN n) = put n
  get = liftM CN get


data QName = QN !String
             deriving (Typeable,Show,Read,Eq)

instance Binary QName where
  put (QN s) = put s
  get = do
    s <- get
    return (QN s)

-- ---------------------------------------------------------------------

data QKey = QK !String
            deriving (Typeable,Show,Read,Eq,Ord)

instance Binary QKey where
  put (QK i) = put i
  get = do
    i <- get
    return $ QK i

consumerNameKey :: ConsumerName -> QKey
consumerNameKey (CN s) = QK s

{-

-record(eroq_message,           {id, data}).
maps on to QEntry

These three map on to QValue
-record(eroq_consumer_message,  {cid, key, msg, src_queue, timestamp = now()}).
-record(eroq_dlq_message,       {reason, data}).
-record(eroq_queue_meta,        {qid, buckets = [], timestamp = now()}).

-}


-- ---------------------------------------------------------------------


data QValue = QVP Payload
            | QVC ConsumerMessage
            | QVD DlqMessage
            | QVM Meta
            deriving (Typeable,Read,Show,Eq)

data QEntry = QE !QKey    -- ^Id
                 !QValue  -- ^payload
              deriving (Typeable,Read,Show,Eq)

instance Binary QValue where
  put (QVP m) = put 'P' >> put m
  put (QVC m) = put 'C' >> put m
  put (QVD m) = put 'D' >> put m
  put (QVM m) = put 'M' >> put m

  get = do 
    sel <- get
    case sel of
      'P' -> liftM QVP get
      'C' -> liftM QVC get
      'D' -> liftM QVD get
      'M' -> liftM QVM get
{-
instance Binary QValue where
  put (QV v) = put v
  get = liftM QV get
-}

instance Binary QEntry where
  put (QE k v) = put k >> put v
  get = do
    k <- get
    v <- get
    return $ QE k v 

instance Serialize QEntry where
  showp = showpBinary
  readp = readpBinary

-- ---------------------------------------------------------------------

type MetaKey = QName

data Meta = MAllBuckets !MetaKey ![TableName] !TimeStamp
            deriving (Show,Read,Typeable,Eq)

instance Binary Meta where
  put (MAllBuckets q tns ts) = put q >> put tns >> put ts
  get = do
    q <- get
    tns <- get
    ts <- get
    return $ MAllBuckets q tns ts


-- ---------------------------------------------------------------------

data TableName = TN !String
                 deriving (Show,Read,Typeable,Eq,Ord)

instance Binary TableName where
  put (TN s) = put s
  get = do
    s <- get
    return (TN s)

-- ---------------------------------------------------------------------

data ProcBucket = PB !QName
     deriving (Show)

data OverflowBucket = OB !QName
     deriving (Show)


data TimeStamp = TS !String
                 deriving (Show,Read,Eq)

instance Binary TimeStamp where
  put (TS s) = put s
  get = do
    s <- get
    return (TS s)

-- ---------------------------------------------------------------------

-- data Payload = Payload !(Map.Map String String)
data Payload = Payload !String
              deriving (Typeable,Read,Show,Eq)
instance Binary Payload where
  put (Payload p) = put p
  get = liftM Payload get

-- -record(eroq_consumer_message,  {cid, key, msg, src_queue, timestamp = now()}).

data ConsumerMessage = CM ConsumerName QKey QEntry QName TimeStamp
                       deriving (Typeable,Read,Show,Eq)
instance Binary ConsumerMessage where
  put (CM cn key val srcQueue ts) = put cn >> put key >> put val >> put srcQueue >> put ts
  get = liftM5 CM get get get get get

-- ---------------------------------------------------------------------

data DlqMessage = DM !String !QValue -- ^Reason, original message
                  deriving (Typeable,Read,Show,Eq)

instance Binary DlqMessage where
  put (DM reason msg) = put reason >> put msg
  get = liftM2 DM get get

-- ---------------------------------------------------------------------

ioGetTimeStamp :: IO TimeStamp
ioGetTimeStamp = do
  t <- getCurrentTime
  return $ TS (show t)

getTimeStamp :: Process TimeStamp
getTimeStamp = do
  ts <- liftIO $ ioGetTimeStamp
  return ts

nullTimeStamp :: TimeStamp
nullTimeStamp = TS "*notset*"

-- ---------------------------------------------------------------------
