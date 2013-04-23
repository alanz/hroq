{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Data.HroqConsumerTH
  (
    acquire_and_store_msg
  , __remoteTable
  )
  where

import Control.Concurrent
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Internal.Types (NodeId(nodeAddress))
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform hiding (__remoteTable)
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Static (staticLabel, staticClosure)
import Control.Monad(when,replicateM,foldM,liftM3,liftM2,liftM)
import Data.Binary
import Data.Hroq
import Data.HroqApp
import Data.HroqLogger
import Data.HroqQueue
import Data.Maybe
import Data.RefSerialize
import Data.Time.Clock
import Data.Typeable hiding (cast)
import System.Environment (getArgs)
import qualified Data.ByteString.Lazy.Char8 as C8
import qualified Data.HroqMnesia as HM
import qualified Data.Map as Map

-- ---------------------------------------------------------------------
-- acquire_and_store_msg(Key, Msg, {CName, SrcQ}) -> ok = eroq_util:retry_dirty_write(10, eroq_consumer_local_storage_table, #eroq_consumer_message{cid=CName, key = Key, msg = Msg, src_queue = SrcQ, timestamp = now()}).

acquire_and_store_msger :: (ConsumerName, QName) -> QEntry -> Process (Either String ())
acquire_and_store_msger (cName,srcQ) entry = do
  logm $ "acquire_and_store_msger:undefined" ++ (show (cName,srcQ,entry))
  ts <- getTimeStamp
  let msg = CM cName entry srcQ ts
  HM.dirty_write_ls hroq_consumer_local_storage_table msg
  return (Right ())

remotable [ 'acquire_and_store_msger
          -- , 'purger
          ]

acquire_and_store_msg :: (ConsumerName, QName) -> Closure (QEntry -> Process (Either String ()))
acquire_and_store_msg params = ( $(mkClosure 'acquire_and_store_msger) params)
-- acquire_and_store_msg = undefined

