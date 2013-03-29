{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Concurrent
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node 
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Workflow
import Data.Binary
import Data.DeriveTH
import Data.Hroq
import Data.HroqQueue
import Data.Maybe
import Data.Persistent.Collection
import Data.RefSerialize
import Data.TCache
import Data.Typeable (Typeable)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import qualified Data.Map as Map
import qualified Data.HroqApp as App

-- https://github.com/tibbe/ekg
import qualified System.Remote.Monitoring as EKG

-- ---------------------------------------------------------------------

main = do
  forkIO $ do {_<- EKG.forkServer "localhost" 8000; return ()}
  node <- startLocalNode

  runProcess node worker

  closeLocalNode node
  
  return ()

-- ---------------------------------------------------------------------

worker :: Process ()
worker = do
  App.start_app

  let qName = QN "queue a"

  qSid <- startQueue (qName,"appinfo","blah")
  say $ "queue started:" ++ (show qSid)

  say "worker started all"

  enqueue qSid qName (qval "foo")
  say "enqueue done"


  liftIO $ threadDelay (10*60*1000000) -- Ten minutes
  return ()


-- ---------------------------------------------------------------------  

startLocalNode :: IO LocalNode
startLocalNode = do
    -- [role, host, port] <- getArgs
  let [role, host, port] = ["foo","127.0.0.1", "10503"]
  -- Right transport <- createTransport host port defaultTCPParameters
  Right (transport,_internals) <- createTransportExposeInternals host port defaultTCPParameters
  node <- newLocalNode transport initRemoteTable
  return node
  

-- ---------------------------------------------------------------------

-- | Provide an identification of a specific Q
qtest ::  RefQueue QEntry
qtest = getQRef "binaryq"

qval str = Map.fromList [(str,str)]

qmain = do
  syncWrite Synchronous
  push qtest (QE (QK "1") (qval "foo"))
  push qtest (QE (QK "2") (qval "bar"))
  syncCache

mm = do
  syncWrite Synchronous
  syncCache
  pickAll qtest


