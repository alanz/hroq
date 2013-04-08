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
  -- forkIO $ do {_<- EKG.forkServer "localhost" 8000; return ()}
  EKG.forkServer "localhost" 8000

  node <- startLocalNode

  runProcess node worker

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

  logm "worker started all"

  -- enqueue qSida qNameA (qval "foo1")
  -- enqueue qSida qNameA (qval "foo2")
  -- logm "enqueue done a"

  -- mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..8000]
  -- mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..2000]
  -- mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..800]

  mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..8]
  logm "enqueue done b"

  -- r <- enqueue_one_message (QN "tablea" ) (qval "bar") s

  liftIO $ threadDelay (3*1000000) -- 3 seconds

  ms4 <- get_state
  logm $ "mnesia state ms4:" ++ (show ms4)

  liftIO $ threadDelay (1*1000000) -- 1 seconds

  logm $ "blurble"

  -- liftIO $ threadDelay (10*60*1000000) -- Ten minutes
  return ()

-- ---------------------------------------------------------------------  

startLocalNode :: IO LocalNode
startLocalNode = do
    -- [role, host, port] <- getArgs
  let [role, host, port] = ["foo","127.0.0.1", "10513"]
  -- Right transport <- createTransport host port defaultTCPParameters
  Right (transport,_internals) <- createTransportExposeInternals host port defaultTCPParameters
  node <- newLocalNode transport initRemoteTable
  startLoggerProcess node
  return node
  

-- ---------------------------------------------------------------------

qval str = Map.fromList [(str,str)]

-- ---------------------------------------------------------------------

