{-# LANGUAGE TemplateHaskell #-}
module Data.HroqDlqWorkers
  (
    requeue
  , purge
  )
  where

import Control.Concurrent
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Internal.Types (NodeId(nodeAddress))
import Control.Distributed.Process.Node
-- import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Platform
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

-- QKey -> QEntry -> Process (Either String ())
requeue :: WorkerFunc
requeue entry = do
  return (Right ())

{-
requeue(Key, #eroq_dlq_message {data = OrigMsg}, DestQueue)->

    RoqMsg = #eroq_message{id = Key, data = OrigMsg},

    ok = eroq_queue:enqueue(DestQueue, RoqMsg).
-}
-- ---------------------------------------------------------------------

-- purge :: WorkerFunc
purge :: QEntry -> Process (Either String ())
purge _entry = do
  return (Right ())

{-
purge(_, _, _)->
    ok.
-}

pp :: QKey -> Process Bool
pp k = do return False


-- ---------------------------------------------------------------------

isPrime :: Integer -> Process Bool
isPrime n = return . (n `elem`) . takeWhile (<= n) . sieve $ [2..]
   where
     sieve :: [Integer] -> [Integer]
     sieve (p : xs) = p : sieve [x | x <- xs, x `mod` p > 0]

remotable ['isPrime
          , 'purge
          , 'pp
          ]



ap :: QEntry -> Closure (Process (Either String ()))
ap qe = ( $(mkClosure 'purge) qe)

dp :: Closure (Process (Either String ()))
dp = ($(mkClosure 'purge) (QE (QK "a") (QV "b") ) )


ff = ($(mkClosure 'isPrime) (79 :: Integer))
-- gg = ($(mkClosure 'purge))

hh :: Closure (Process Bool)
hh = ($(mkClosure 'pp) (QK "cc"))

ii :: QKey -> Closure (Process Bool)
ii = ($(mkClosure 'pp))

master :: [NodeId] -> Process ()
master [] = liftIO $ putStrLn "no slaves"
master (slave:_) = do
   -- isPrime79 <- call $(functionTDict 'isPrime) slave ($(mkClosure 'isPrime) (79 :: Integer))
   -- liftIO $ print isPrime79
   liftIO $ print "foo"

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

