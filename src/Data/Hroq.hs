{-# LANGUAGE DeriveDataTypeable   #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE StandaloneDeriving   #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE ScopedTypeVariables  #-}

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
  , QueueMessage(..)
  , TimeStamp
  , ioGetTimeStamp
  , getTimeStamp
  , nullTimeStamp
  ,  maxBucketSizeConst

  -- *bucket types
  , ProcBucket(..)
  , OverflowBucket(..)

  -- * Thyme related
  , timeIntervalToDiffTime
  , diffTimeToTimeInterval
  , diffTimeToMicroSeconds
  , diffTimeToDelay
  , delayToDiffTime
  , microsecondsToNominalDiffTime
  , microSecondsPerSecond
  )
  where

import Control.Distributed.Process.Platform.Time hiding (delayToDiffTime,timeIntervalToDiffTime,diffTimeToDelay,microsecondsToNominalDiffTime,diffTimeToTimeInterval,NominalDiffTime)
import Control.Distributed.Process hiding (call)
import Control.Lens
import Data.AffineSpace
import Data.Binary
import Data.HroqLogger
import Data.Ratio ((%))
import Data.RefSerialize
import Data.Thyme.Calendar
import Data.Thyme.Clock
import Data.Thyme.Format
import Data.Thyme.Time.Core
import Data.Typeable
import GHC.Generics

-- ---------------------------------------------------------------------

-- -define(MAX_BUCKET_SIZE,  eroq_util:app_param(max_bucket_size, 5000)).
maxBucketSizeConst = 5000
-- maxBucketSizeConst = 5
-- maxBucketSizeConst = 50
-- maxBucketSizeConst = 500

-- ---------------------------------------------------------------------

data ConsumerName = CN !String
                    deriving (Eq,Show,Read,Typeable,Ord,Generic)
instance Binary ConsumerName where

data QName = QN !String
             deriving (Typeable,Show,Read,Eq,Ord,Generic)
instance Binary QName where

-- ---------------------------------------------------------------------

data QKey = QK !String
            deriving (Typeable,Show,Read,Eq,Ord,Generic)
instance Binary QKey where

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
            deriving (Typeable,Read,Show,Eq,Generic)
instance Binary QValue where

data QEntry = QE !QKey    -- ^Id
                 !QValue  -- ^payload
              deriving (Typeable,Read,Show,Eq,Generic)
instance Binary QEntry where

instance Serialize QEntry where
  showp = showpBinary
  readp = readpBinary

instance Binary NominalDiffTime where
  put ndt = put ((toSeconds ndt) :: Float)
  get = do
    (val :: Float) <- get
    return $ fromSeconds val



-- ---------------------------------------------------------------------

type MetaKey = QName

data Meta = MAllBuckets !MetaKey ![TableName] !TimeStamp
            deriving (Show,Read,Typeable,Eq,Generic)
instance Binary Meta where

-- ---------------------------------------------------------------------

data TableName = TN !String
                 deriving (Show,Read,Typeable,Eq,Ord,Generic)
instance Binary TableName where

-- ---------------------------------------------------------------------

data ProcBucket = PB !QName
     deriving (Show)

data OverflowBucket = OB !QName
     deriving (Show)


data TimeStamp = TS !String
                 deriving (Show,Read,Eq,Generic)
instance Binary TimeStamp where

-- ---------------------------------------------------------------------

-- data Payload = Payload !(Map.Map String String)
data Payload = Payload !String
              deriving (Typeable,Read,Show,Eq,Generic)
instance Binary Payload where

-- -record(eroq_consumer_message,  {cid, key, msg, src_queue, timestamp = now()}).

data ConsumerMessage = CM ConsumerName QKey QEntry QName TimeStamp
                       deriving (Typeable,Read,Show,Eq,Generic)
instance Binary ConsumerMessage where

-- ---------------------------------------------------------------------

data DlqMessage = DM !String !QValue -- ^Reason, original message
                  deriving (Typeable,Read,Show,Eq,Generic)
instance Binary DlqMessage where

-- ---------------------------------------------------------------------

-- |Signal sent from enqueue to consumer to notify of freshly enqueued
-- message
data QueueMessage = QueueMessage
                     deriving (Typeable,Show,Generic)
instance Binary QueueMessage where



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

-- TODO: give these back into distributed-process-platform

-- | given a @TimeInterval@, provide an equivalent @NominalDiffTim@
timeIntervalToDiffTime :: TimeInterval -> NominalDiffTime
timeIntervalToDiffTime ti = microsecondsToNominalDiffTime (fromIntegral $ asTimeout ti)

-- | given a @NominalDiffTim@@, provide an equivalent @TimeInterval@
diffTimeToTimeInterval :: NominalDiffTime -> TimeInterval
diffTimeToTimeInterval dt = microSeconds $ round $ diffTimeToMicroSeconds dt

{-# INLINE diffTimeToMicroSeconds #-}
diffTimeToMicroSeconds :: (TimeDiff t, Fractional n) => t -> n
diffTimeToMicroSeconds = (* recip 1000) . fromIntegral . view microseconds



-- | given a @NominalDiffTim@@, provide an equivalent @Delay@
diffTimeToDelay :: NominalDiffTime -> Delay
diffTimeToDelay dt = Delay $ diffTimeToTimeInterval dt

-- | given a @Delay@, provide an equivalent @NominalDiffTim@
delayToDiffTime :: Delay -> NominalDiffTime
delayToDiffTime (Delay ti) = timeIntervalToDiffTime ti
delayToDiffTime Infinity   = error "trying to convert Delay.Infinity to a NominalDiffTime"
delayToDiffTime (NoDelay)  = microsecondsToNominalDiffTime 0

-- | Create a 'NominalDiffTime' from a number of microseconds.
microsecondsToNominalDiffTime :: Integer -> NominalDiffTime
-- microsecondsToNominalDiffTime x = fromRational (x % (fromIntegral microSecondsPerSecond))
microsecondsToNominalDiffTime x = fromSeconds (x % microSecondsPerSecond)

{-# INLINE microSecondsPerSecond #-}
microSecondsPerSecond :: Integer
microSecondsPerSecond = 1000000




-- ---------------------------------------------------------------------
