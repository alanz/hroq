{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE CPP #-}

module Main where

import Control.Concurrent.MVar
import Control.Exception (SomeException)
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform hiding (__remoteTable, monitor, send, nsend)
import Control.Distributed.Process.Platform.ManagedProcess
import Control.Distributed.Process.Platform.Test
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Serializable()

-- import MathsDemo
-- import Counter
-- import qualified SafeCounter as SafeCounter

import Data.Hroq
import Data.HroqStatsGatherer

#if ! MIN_VERSION_base(4,6,0)
import Prelude hiding (catch)
#endif

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import TestUtils
-- import ManagedProcessCommon

import qualified Network.Transport as NT
import Control.Monad (void)

-- utilities

-- ---------------------------------------------------------------------

testQueueStats :: ProcessId -> TestResult Int -> Process ()
testQueueStats _pid result = do
  ReplyQStatsNotFound <- get_queue_stats (QN "queue1")
  publish_queue_stats (QN "queue1") (QStats "info" 1 2 3)
  publish_queue_stats (QN "queue2") (QStats "info" 2 3 4)
  (ReplyQStats (QStats "info" 1 2 3)) <- get_queue_stats (QN "queue1")
  stash result 5

-- ---------------------------------------------------------------------

testConsumerStats :: ProcessId -> TestResult Int -> Process ()
testConsumerStats _pid result = do
  ReplyCStatsNotFound <- get_consumer_stats (CN "consumer1")
  publish_consumer_stats (CN "consumer1") (QStats "info" 1 2 3)
  publish_consumer_stats (CN "consumer2") (QStats "info" 2 3 4)
  (ReplyCStats (QStats "info" 1 2 3)) <- get_consumer_stats (CN "consumer1")
  stash result 5

-- ---------------------------------------------------------------------

tests :: NT.Transport  -> IO [Test]
tests transport = do
  localNode <- newLocalNode transport initRemoteTable
  pid <- forkProcess localNode $ hroq_stats_gatherer
  return [
          testGroup "stats gatherer" [
            testCase "queues"
              (delayedAssertion
               "stats for queues"
               localNode 5 (testQueueStats pid))
          , testCase "consumers"
              (delayedAssertion
               "stats for queues"
               localNode 5 (testQueueStats pid))
          ]
      ]

main :: IO ()
main = testMain $ tests

