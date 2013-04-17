{-# LANGUAGE TemplateHaskell #-}
module Data.HroqDlqWorkers
  (
    requeue
  , purge

  , __remoteTable
  )
  where

import Control.Concurrent
import Control.Distributed.Process hiding (call)
-- import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Internal.Types (NodeId(nodeAddress))
import Control.Distributed.Process.Node
-- import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Platform hiding (__remoteTable)
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Static (staticLabel, staticClosure)
import Data.Binary
import Data.Hroq
import Data.HroqLogger
import Data.HroqQueue
import Data.Maybe
import Data.RefSerialize
import Data.Typeable
import System.Environment (getArgs)
import qualified Data.ByteString.Lazy.Char8 as C8
import qualified Data.Map as Map

-- ---------------------------------------------------------------------


requeuer :: QName -> WorkerFunc
requeuer destQ entry@(QE _ qv) = do
  logm $ "purger:" ++ (show (destQ,entry))
  enqueue destQ qv
  return (Right ())

{-
requeue(Key, #eroq_dlq_message {data = OrigMsg}, DestQueue)->

    RoqMsg = #eroq_message{id = Key, data = OrigMsg},

    ok = eroq_queue:enqueue(DestQueue, RoqMsg).
-}
-- ---------------------------------------------------------------------

-- Note: the first parameter represents the environement, which is
-- serialised and remoted together with the function when is is turned
-- into a Closure
purger :: Int -> QEntry -> Process (Either String ())
purger _ entry = do
  logm $ "purger:" ++ (show entry)
  return (Right ())


-- ---------------------------------------------------------------------

remotable [ 'requeuer
          , 'purger
          ]



requeue :: QName -> Closure (QEntry -> Process (Either String ()))
requeue qe = ( $(mkClosure 'requeuer) qe)

purge :: Int -> Closure (QEntry -> Process (Either String ()))
purge qe = ( $(mkClosure 'purger) qe)



{-
main :: IO ()
main = do
   args <- getArgs
   case args of
     ["master", host, port] -> do
       backend <- initializeBackend host port rtable
       startMaster backend master
     ["slave", host, port] -> do
       backend <- initializeBackend host port rtable
       startSlave backend
   where
     rtable :: RemoteTable
     rtable = __remoteTable initRemoteTable

-}

