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
import Control.Workflow
import Data.Binary
import Data.Maybe
import Data.Persistent.Collection
import Data.RefSerialize
import Data.TCache
import Data.TCache.Defs
import Data.Time.Clock
import Data.Typeable
import qualified Data.ByteString.Lazy.Char8 as C8
import qualified Data.Map as Map

-- ---------------------------------------------------------------------

data QName = QN String
             deriving (Typeable,Show,Read)

instance Indexable QName where
  key = show 

instance Serializable QName where
  serialize s  = C8.pack $ show s
  deserialize = read. C8.unpack
  -- setPersist =

data QKey = QK String
            deriving (Typeable,Show,Read,Eq,Ord)

instance Binary QKey where
  put (QK i) = put i
  get = do
    i <- get
    return $ QK i


type QValue = Map.Map String String
data QEntry = QE QKey    -- ^Id
                 QValue  -- ^payload
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


data ProcBucket = PB QName
     deriving (Show)

data OverflowBucket = OB QName
     deriving (Show)


data TimeStamp = TS String
                 deriving (Show,Read)

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

