{-# LANGUAGE TemplateHaskell #-}
module Data.HroqDlqWorkers
  (
    requeue
  , purge

  , __remoteTable
  )
  where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Data.Hroq
import Data.HroqLogger
import Data.HroqQueue

-- ---------------------------------------------------------------------

requeuer :: QName -> WorkerFunc
requeuer destQ entry@(QE _ qv) = do
  logm $ "purger:" ++ (show (destQ,entry))
  qpid <- Data.HroqQueue.getSid destQ
  enqueue qpid destQ qv
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

