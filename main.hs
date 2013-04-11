{- # LANGUAGE DeriveDataTypeable  # -}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Concurrent
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node 
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
-- import Control.Workflow
import Data.Binary
import Data.DeriveTH
import Data.Hroq
import Data.HroqLogger
import Data.HroqMnesia
import Data.HroqQueueMeta
import Data.HroqQueue
import Data.Maybe
-- import Data.Persistent.Collection
import Data.RefSerialize
-- import Data.TCache
import Data.Typeable (Typeable)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import qualified Data.Map as Map
import qualified Data.HroqApp as App

-- https://github.com/tibbe/ekg
import qualified System.Remote.Monitoring as EKG

-- ---------------------------------------------------------------------

main = do
  -- EKG.forkServer "localhost" 8000

  node <- startLocalNode

  -- runProcess node worker
  runProcess node worker_mnesia

  closeLocalNode node
  
  return ()

-- ---------------------------------------------------------------------

worker :: Process ()
worker = do
  mnesiaSid <- startHroqMnesia ()
  logm "mnesia started"

  ms1 <- get_state
  logm $ "mnesia state ms1:" ++ (show ms1)

  App.start_app
  logm "app started"

  ms2 <- get_state
  logm $ "mnesia state ms2:" ++ (show ms2)

  let qNameA = QN "queue_a"
  let qNameB = QN "queue_b"

  -- qSida <- startQueue (qNameA,"appinfo","blah")
  -- logm $ "queue started:" ++ (show qSida)


  qSidb <- startQueue (qNameB,"appinfo","blah")
  logm $ "queue started:" ++ (show qSidb)

  ms3 <- get_state
  logm $ "mnesia state ms3:" ++ (show ms3)

  -- liftIO $ threadDelay (10*1000000) -- 10 seconds

  logm "worker started all"

  -- enqueue qSida qNameA (qval "foo1")
  -- enqueue qSida qNameA (qval "foo2")
  -- logm "enqueue done a"

  -- mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..8000]
  -- mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..2000]
  mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..800]

  -- mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..51]
  -- mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..11]
  logm "enqueue done b"

  -- mapM_ (\n -> enqueue qSida qNameA (qval $ "aaa" ++ (show n))) [1..8]
  -- logm "enqueue done a"

  -- r <- enqueue_one_message (QN "tablea" ) (qval "bar") s

  -- liftIO $ threadDelay (3*1000000) -- 3 seconds

  ms4 <- get_state
  logm $ "mnesia state ms4:" ++ (show ms4)

  liftIO $ threadDelay (1*1000000) -- 1 seconds

  logm $ "blurble"

  -- liftIO $ threadDelay (10*60*1000000) -- Ten minutes
  return ()

-- ---------------------------------------------------------------------

worker_mnesia :: Process ()
worker_mnesia = do
  mnesiaSid <- startHroqMnesia ()
  logm "mnesia started"

  ms1 <- get_state
  logm $ "mnesia state ms1:" ++ (show ms1)

  let table = TN "mnesiattest"

  create_table DiscCopies table RecordTypeQueueEntry

  wait_for_tables [table] Infinity

  ms2 <- get_state
  logm $ "mnesia state ms2:" ++ (show ms2)

  mapM_ (\n -> dirty_write_q table (QE (QK "a") (qval $ "bar" ++ (show n)))) [1..800]

  ms4 <- get_state
  logm $ "mnesia state ms4:" ++ (show ms4)

  liftIO $ threadDelay (1*1000000) -- 1 seconds

  logm $ "mnesia blurble"

  -- liftIO $ threadDelay (10*60*1000000) -- Ten minutes
  return ()

-- ---------------------------------------------------------------------  

startLocalNode :: IO LocalNode
startLocalNode = do
    -- [role, host, port] <- getArgs
  let [role, host, port] = ["foo","127.0.0.1", "10519"]
  -- Right transport <- createTransport host port defaultTCPParameters
  Right (transport,_internals) <- createTransportExposeInternals host port defaultTCPParameters
  node <- newLocalNode transport initRemoteTable
  startLoggerProcess node
  return node
  

-- ---------------------------------------------------------------------

qval str = QV $ Map.fromList [(str,str)]

-- ---------------------------------------------------------------------

