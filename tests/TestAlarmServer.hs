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
import Control.Distributed.Process.Platform hiding (monitor, send, nsend)
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Test
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Serializable()
import Control.Distributed.Process.Platform.Timer


import Data.Concurrent.Queue.Roq.Hroq
import qualified Data.Concurrent.Queue.Roq.StatsGatherer as SG
import qualified Data.Concurrent.Queue.Roq.Groups as G
import qualified Data.Concurrent.Queue.Roq.AlarmServer as A
import Data.Concurrent.Queue.Roq.AlarmServer (__remoteTable)

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

logm = say

-- --------------------------------------------------------------------
-- Testing HroqAlarmServer
-- ---------------------------------------------------------------------

testQueueStats :: ProcessId -> TestResult Int -> Process ()
testQueueStats pid result = do
  SG.ReplyQStatsNotFound <- SG.get_queue_stats pid (QN "queue1")
  SG.publish_queue_stats pid (QN "queue1") (SG.QStats "info" 1 2 3)
  SG.publish_queue_stats pid (QN "queue2") (SG.QStats "info" 2 3 4)
  (SG.ReplyQStats (SG.QStats "info" 1 2 3)) <- SG.get_queue_stats pid (QN "queue1")
  stash result 5

-- ---------------------------------------------------------------------

testConsumerStats :: ProcessId -> TestResult Int -> Process ()
testConsumerStats pid result = do
  SG.ReplyCStatsNotFound <- SG.get_consumer_stats pid (CN "consumer1")
  SG.publish_consumer_stats pid (CN "consumer1") (SG.QStats "info" 1 2 3)
  SG.publish_consumer_stats pid (CN "consumer2") (SG.QStats "info" 2 3 4)
  (SG.ReplyCStats (SG.QStats "info" 1 2 3)) <- SG.get_consumer_stats pid (CN "consumer1")
  stash result 5

-- --------------------------------------------------------------------
-- Testing HroqGroups
-- ---------------------------------------------------------------------

testGroupsQueues :: ProcessId -> TestResult Int -> Process ()
testGroupsQueues _pid result = do
  -- logm $ "testGroupsQueues starting"
  [] <- G.queues
  [] <- G.consumers
  [] <- G.dlq_consumers
  -- logm $ "testGroupsQueues 1"
  G.join (G.NGQueue (QN "queue1"))
  -- logm $ "testGroupsQueues 2"
  [QN "queue1"] <- G.queues
  [] <- G.consumers
  [] <- G.dlq_consumers
  -- logm $ "testGroupsQueues 3"
  -- sleepFor 3 Seconds
  stash result 5

testGroupsConsumers :: ProcessId -> TestResult Int -> Process ()
testGroupsConsumers _pid result = do
  -- logm $ "testGroupsQueues starting"
  [] <- G.queues
  [] <- G.consumers
  [] <- G.dlq_consumers
  -- logm $ "testGroupsQueues 1"

  G.join (G.NGConsumer (CN "cons1"))
  -- logm $ "testGroupsQueues 2"

  [] <- G.queues
  [CN "cons1"] <- G.consumers
  [] <- G.dlq_consumers

  -- logm $ "testGroupsQueues 3"
  -- sleepFor 3 Seconds
  stash result 5

testGroupsDlqConsumers :: ProcessId -> TestResult Int -> Process ()
testGroupsDlqConsumers _pid result = do
  -- logm $ "testGroupsDlqConsumers starting"
  [] <- G.queues
  [] <- G.consumers
  [] <- G.dlq_consumers
  -- logm $ "testGroupsDlqConsumers 1"

  G.join (G.NGDlqConsumer (CN "cons1"))
  -- logm $ "testGroupsDlqConsumers 2"

  [] <- G.queues
  [] <- G.consumers
  [CN "cons1"] <- G.dlq_consumers

  -- logm $ "testGroupsDlqConsumers 3"
  -- sleepFor 3 Seconds
  stash result 5

terminatePid :: LocalNode -> ProcessId -> IO ()
terminatePid localNode pid = do
  runProcess localNode $ exit pid "done"

withGroups :: LocalNode -> IO a -> IO a
withGroups localNode test = do
  pid <- forkProcess localNode $ G.hroq_groups
  r <- test
  terminatePid localNode pid
  return r

-- ---------------------------------------------------------------------

tests :: NT.Transport  -> IO [Test]
tests transport = do
  localNode <- newLocalNode transport initRemoteTable
  sg_pid <- forkProcess localNode $ SG.hroq_stats_gatherer
  -- g_pid <- forkProcess localNode $ G.hroq_groups
  return [ testGroup "stats gatherer" [
            testCase "queues"
              (delayedAssertion
               "stats for queues"
               localNode 5 (testQueueStats sg_pid))
          , testCase "consumers"
              (delayedAssertion
               "stats for queues"
               localNode 5 (testQueueStats sg_pid))
          ]

        -- -----------------------------

        , testGroup "groups" [
            testCase "queues"
              (withGroups localNode
              (delayedAssertion
               "stats for queues"
               localNode 5 (testGroupsQueues sg_pid)))
          , testCase "consumers"
              (withGroups localNode
              (delayedAssertion
               "stats for consumers"
               localNode 5 (testGroupsConsumers sg_pid)))
          , testCase "dlq consumers"
              (withGroups localNode
              (delayedAssertion
               "stats for consumers"
               localNode 5 (testGroupsDlqConsumers sg_pid)))
          ]
      ]

rtable :: RemoteTable
rtable = Control.Distributed.Process.Platform.__remoteTable
       $ Data.Concurrent.Queue.Roq.AlarmServer.__remoteTable
       -- $ Data.Concurrent.Queue.Roq.ConsumerTH.__remoteTable
       -- $ Data.Concurrent.Queue.Roq.DlqWorkers.__remoteTable
       -- $ Data.Concurrent.Queue.Roq.Groups.__remoteTable
       -- $ Data.Concurrent.Queue.Roq.SampleWorker.__remoteTable
       -- $ Data.Concurrent.Queue.Roq.StatsGatherer.__remoteTable
       $ initRemoteTable


main :: IO ()
main = testMain $ tests

