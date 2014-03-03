{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Data.Concurrent.Queue.Roq.ConsumerTH
  (
    acquire_and_store_msg
  , __remoteTable
  )
  where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Data.Concurrent.Queue.Roq.Hroq
import Data.Concurrent.Queue.Roq.App
import Data.Concurrent.Queue.Roq.Logger
import qualified Data.Concurrent.Queue.Roq.Mnesia as HM

-- ---------------------------------------------------------------------
-- acquire_and_store_msg(Key, Msg, {CName, SrcQ}) -> ok = eroq_util:retry_dirty_write(10, eroq_consumer_local_storage_table, #eroq_consumer_message{cid=CName, key = Key, msg = Msg, src_queue = SrcQ, timestamp = now()}).

acquire_and_store_msger :: (ConsumerName,QName) -> QEntry -> Process (Either String ())
acquire_and_store_msger (cName,srcQ) entry = do
  logm $ "acquire_and_store_msger:undefined" ++ (show (cName,srcQ,entry))
  ts <- getTimeStamp
  let (QE key _msg) = entry
  let msg = QE key (QVC (CM cName key entry srcQ ts))
  HM.dirty_write_q hroq_consumer_local_storage_table msg
  return (Right ())

remotable [ 'acquire_and_store_msger
          -- , 'purger
          ]

acquire_and_store_msg :: (ConsumerName, QName) -> Closure (QEntry -> Process (Either String ()))
acquire_and_store_msg params = ( $(mkClosure 'acquire_and_store_msger) params)
-- acquire_and_store_msg = undefined

