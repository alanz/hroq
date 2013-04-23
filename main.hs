{-# LANGUAGE TemplateHaskell #-}
{- # LANGUAGE DeriveDataTypeable  # -}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}


-- import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Concurrent
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Static (staticLabel, staticClosure)
import Data.Binary
import Data.DeriveTH
import Data.Hroq
import Data.HroqConsumer
import Data.HroqConsumerTH
import Data.HroqDlqWorkers
import Data.HroqLogger
import Data.HroqMnesia
import Data.HroqQueue
import Data.HroqQueueMeta
import Data.Maybe
import Data.RefSerialize
import Data.Typeable (Typeable)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import qualified Data.HroqApp as App
import qualified Data.Map as Map

-- https://github.com/tibbe/ekg
import qualified System.Remote.Monitoring as EKG

-- ---------------------------------------------------------------------

main = do
  ekg <- EKG.forkServer "localhost" 8000

  node <- startLocalNode

  runProcess node (worker ekg)
  -- runProcess node worker_mnesia

  closeLocalNode node
  
  return ()

-- ---------------------------------------------------------------------

worker :: EKG.Server -> Process ()
worker ekg = do
  mnesiaSid <- startHroqMnesia ekg
  logm "mnesia started"

  logm $ "mnesia state ms1:"
  log_state

  App.start_app
  logm "app started"

  logm $ "mnesia state ms2:"
  log_state

  let qNameA = QN "queue_a"
  let qNameB = QN "queue_b"

  qSida <- startQueue (qNameA,"appinfo","blah",ekg)
  logm $ "queue started:" ++ (show qSida)


  qSidb <- startQueue (qNameB,"appinfo","blah",ekg)
  logm $ "queue started:" ++ (show qSidb)

  logm $ "mnesia state ms3:" 
  log_state

  -- liftIO $ threadDelay (10*1000000) -- 10 seconds

  logm "worker started all"

  -- enqueue qSida qNameA (qval "foo1")
  -- enqueue qSida qNameA (qval "foo2")
  -- logm "enqueue done a"

  -- mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..80000]
  -- mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..8000]
  -- mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..2000]
  -- mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..800]
  -- mapM_ (\n -> enqueueCast qSidb qNameB (qval $ "bar" ++ (show n))) [1..800]

  -- mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..51]
  mapM_ (\n -> enqueue qNameB (qval $ "bar" ++ (show n))) [1..11]

  logm "enqueue done b"

  -- mapM_ (\n -> enqueue qSida qNameA (qval $ "aaa" ++ (show n))) [1..8]
  -- logm "enqueue done a"

  -- r <- enqueue_one_message (QN "tablea" ) (qval "bar") s

  -- liftIO $ threadDelay (3*1000000) -- 3 seconds

  logm $ "mnesia state ms4:"
  log_state

  liftIO $ threadDelay (1*1000000) -- 1 seconds

  pr <- peek qNameB
  logm $ "peek:pr=" ++ (show pr)

  liftIO $ threadDelay (1*1000000) -- 1 seconds

  pd <- dequeue qNameB (purge 0) Nothing
  logm $ "dequeue:pd=" ++ (show pd)

  liftIO $ threadDelay (1*1000000) -- 1 seconds

  pd <- dequeue qNameB (requeue qNameA) Nothing
  logm $ "dequeue:pd=" ++ (show pd)

  logm $ "blurble"

  -- liftIO $ threadDelay (10*60*1000000) -- Ten minutes
  return ()

-- ---------------------------------------------------------------------

worker_mnesia :: EKG.Server -> Process ()
worker_mnesia ekg = do
  mnesiaSid <- startHroqMnesia ekg
  logm "mnesia started"

  -- ms1 <- log_state
  -- logm $ "mnesia state ms1:" ++ (show ms1)

  let table = TN "mnesiattest"

  create_table DiscCopies table RecordTypeQueueEntry

  wait_for_tables [table] Infinity

  -- ms2 <- log_state
  -- logm $ "mnesia state ms2:" ++ (show ms2)

  let qe = QE (QK "a") (qval $ "bar2")
  let s =   (MnesiaState Map.empty Map.empty Map.empty)
  mapM_ (\n -> dirty_write_q table (QE (QK "a") (qval $ "bar" ++ (show n)))) [1..800]
  -- mapM_ (\n -> dirty_write_q table qe) [1..800]
  -- mapM_ (\n -> do_dirty_write_q s table qe) [1..800]


  -- ms4 <- log_state
  -- logm $ "mnesia state ms4:" ++ (show ms4)

  liftIO $ threadDelay (1*1000000) -- 1 seconds

  logm $ "mnesia blurble"

  -- liftIO $ threadDelay (10*60*1000000) -- Ten minutes
  return ()

-- ---------------------------------------------------------------------  

{-
startLocalNode :: IO LocalNode
startLocalNode = do
    -- [role, host, port] <- getArgs
  let [role, host, port] = ["foo","127.0.0.1", "10520"]
  -- Right transport <- createTransport host port defaultTCPParameters
  -- Right (transport,_internals) <- createTransportExposeInternals host port defaultTCPParameters
  backend <- initializeBackend role host rtable
  -- node <- newLocalNode transport rtable
  node <- newLocalNode backend
  startLoggerProcess node
  return node
  where
    rtable :: RemoteTable
    rtable = Data.HroqDlqWorkers.__remoteTable 
           $ Control.Distributed.Process.Platform.__remoteTable
           $ initRemoteTable
-}


startLocalNode :: IO LocalNode
startLocalNode = do
    -- [role, host, port] <- getArgs
  let [role, host, port] = ["foo","127.0.0.1", "10520"]
  -- Right transport <- createTransport host port defaultTCPParameters
  Right (transport,_internals) <- createTransportExposeInternals host port defaultTCPParameters
  node <- newLocalNode transport rtable
  startLoggerProcess node
  return node
  where
    rtable :: RemoteTable
    rtable = Data.HroqDlqWorkers.__remoteTable 
           $ Data.HroqConsumerTH.__remoteTable
           $ Control.Distributed.Process.Platform.__remoteTable
           $ initRemoteTable
  


-- ---------------------------------------------------------------------

-- qval str = QV $ Map.fromList [(str,str)]
qval str = QV str

-- ---------------------------------------------------------------------
