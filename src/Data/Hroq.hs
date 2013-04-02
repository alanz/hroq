{-# LANGUAGE DeriveDataTypeable  #-}

module Data.Hroq
  (
    QKey(..)
  , QValue(..)
  , QEntry(..)
  , QName(..)
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
-- import Control.Workflow
import Data.Binary
import Data.Maybe
import Data.Persistent.Collection
import Data.RefSerialize
import Data.TCache
import Data.TCache.Defs
import Data.HroqLogger
import Data.Time.Clock
import Data.Typeable
import qualified Data.ByteString.Lazy.Char8 as C8
import qualified Data.Map as Map

-- ---------------------------------------------------------------------

-- -define(MAX_BUCKET_SIZE,  eroq_util:app_param(max_bucket_size, 5000)).
-- maxBucketSize = 5000
-- maxBucketSizeConst = 5
maxBucketSizeConst = 50

-- ---------------------------------------------------------------------

data QName = QN !String
             deriving (Typeable,Show,Read)

instance Indexable QName where
  key = show 

instance Serializable QName where
  serialize s  = C8.pack $ show s
  deserialize = read. C8.unpack
  -- setPersist =

instance Binary QName where
  put (QN s) = put s
  get = do
    s <- get
    return (QN s)

data QKey = QK !String
            deriving (Typeable,Show,Read,Eq,Ord)

instance Binary QKey where
  put (QK i) = put i
  get = do
    i <- get
    return $ QK i


type QValue = Map.Map String String
data QEntry = QE !QKey    -- ^Id
                 !QValue  -- ^payload
              deriving (Typeable,Read,Show)

instance Binary QEntry where
  put (QE k v) = put k >> put v
  get = do
    k <- get
    v <- get
    return $ QE k v 

instance Serialize QEntry where
  showp = showpBinary
  readp = readpBinary

instance Indexable QEntry where
  key (QE qk _) = show qk

instance Serializable QEntry where
   serialize s  = C8.pack $ show s
   deserialize = read. C8.unpack


data ProcBucket = PB !QName
     deriving (Show)

data OverflowBucket = OB !QName
     deriving (Show)


data TimeStamp = TS !String
                 deriving (Show,Read)

instance Binary TimeStamp where
  put (TS s) = put s
  get = do
    s <- get
    return (TS s)

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

