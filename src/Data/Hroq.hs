{-# LANGUAGE DeriveDataTypeable  #-}

module Data.Hroq
  (
    QValue(..)
  , QEntry(..)
  , QName(..)
  )
  where

import Control.Concurrent
import Control.Workflow
import Data.Binary
import Data.Maybe
import Data.Persistent.Collection
import Data.Persistent.Collection
import Data.RefSerialize
import Data.TCache
import Data.Typeable
import qualified Data.Map as Map

-- ---------------------------------------------------------------------

data QName = QN String
             deriving (Typeable,Show)

type QValue = Map.Map String String
-- type QValue = String
data QEntry = QE Integer -- ^Id, not sure if we need this
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
