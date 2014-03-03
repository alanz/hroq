{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}


import Control.Concurrent
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform hiding (send)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Platform.Timer
import Data.AffineSpace
import Data.Concurrent.Queue.Roq.Hroq
import Data.Concurrent.Queue.Roq.AlarmServer
import Data.Concurrent.Queue.Roq.Consumer
import Data.Concurrent.Queue.Roq.ConsumerTH
import Data.Concurrent.Queue.Roq.DlqWorkers
import Data.Concurrent.Queue.Roq.Groups
import Data.Concurrent.Queue.Roq.HandlePool
import Data.Concurrent.Queue.Roq.Logger
import Data.Concurrent.Queue.Roq.Mnesia
import Data.Concurrent.Queue.Roq.Queue
import Data.Concurrent.Queue.Roq.QueueWatchServer
import Data.Concurrent.Queue.Roq.SampleWorker
import Data.Concurrent.Queue.Roq.StatsGatherer
import Data.Thyme.Clock
import Data.Thyme.Format
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import qualified Data.Concurrent.Queue.Roq.App as App
import qualified Data.Concurrent.Queue.Roq.Groups as G
import qualified Data.Map as Map

-- https://github.com/tibbe/ekg
import qualified System.Remote.Monitoring as EKG

-- ---------------------------------------------------------------------

main = do
  ekg <- EKG.forkServer "localhost" 8000

  node <- startLocalNode

  -- runProcess node (worker ekg)
  -- runProcess node (worker_consumer ekg)
  -- runProcess node (worker_mnesia ekg)

  runProcess node (worker_supervised ekg)

  closeLocalNode node

  return ()

-- ---------------------------------------------------------------------

worker_supervised :: EKG.Server ->  Process ()
worker_supervised ekg = do
  mnesiaSid <- startHroqMnesia ekg
  logm "mnesia started"

  logm "worker_supervised starting"
  pid <- App.start_app
  logm $ "worker_supervised started:pid=" ++ show pid
  sleepFor 2 Seconds

  alarmPid <- hroq_alarm_server_pid

  -- check the alarm server is alive
  logm $ "worker_supervised alarm server check starting"
  rc <- check alarmPid
  logm $ "worker_supervised alarm server check done:got" ++ show rc

  rt <- triggers alarmPid
  logm $ "worker_supervised alarm server triggers done:got" ++ show rt


  sleepFor 1 Seconds

  Data.Concurrent.Queue.Roq.QueueWatchServer.ping

  logm $ "starting queue group stuff"
  q1 <- queues
  logm $ "queues:q1=" ++ show q1

  G.join (G.NGQueue (QN "queue1"))

  q2 <- queues
  logm $ "queues:q1=" ++ show q2


  let qNameA = QN "queue_a"
  let qNameB = QN "queue_b"

  qSida <- startQueue (qNameA,"MAIN1","blah",ekg)
  logm $ "queue started:" ++ (show qSida)


  qSidb <- startQueue (qNameB,"MAIN","blah",ekg)
  logm $ "queue started:" ++ (show qSidb)


  logm $ "starting queue group stuff"
  q2a <- queues
  logm $ "queues:q2=" ++ show q2a


  sid <- getSelfPid

  let numberToEnqueue = 10000
  startTime <- liftIO getCurrentTime
  logm "enqueue tasks starting"
  -- mapM_ (\n -> enqueue qNameB (qval $ "bar" ++ (show n))) [1..10]
  -- spawnLocal $ (mapM_ (\n -> sleepFor 15 Millis >> enqueue qNameB (qval $ "bar" ++ (show n))) [1..numberToEnqueue])
  -- spawnLocal $ (mapM_ (\n -> sleepFor 20 Millis >> enqueue qNameA (qval $ "bar" ++ (show n))) [1..numberToEnqueue])
  spawnLocal $ (mapM_ (\n -> {- sleepFor 10 Micros >> -} enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..numberToEnqueue] >> send sid 'b')
  spawnLocal $ (mapM_ (\n -> {- sleepFor 10 Micros >> -} enqueue qSida qNameA (qval $ "bar" ++ (show n))) [1..numberToEnqueue] >> send sid 'a')
  logm "enqueue done b 1"

  done1 <- expect :: Process Char
  logm $ "got done1:" ++ show done1
  done2 <- expect :: Process Char
  logm $ "got done2:" ++ show done2

  endTime <- liftIO getCurrentTime
  logm $ "(time,numberToEnqueue=)" ++ show (endTime .-. startTime,numberToEnqueue)

  sleepFor 10 Seconds
  logm "worker_supervised done"

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
  -- mapM_ (\n -> enqueue qNameB (qval $ "bar" ++ (show n))) [1..11]

  liftIO $ threadDelay (5*1000000) -- 1 seconds
  logm "enqueue done b starting"
  mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [1..1]
  logm "enqueue done b 1"

  liftIO $ threadDelay (1*1000000) -- 1 seconds
  mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [2..2]
  logm "enqueue done b 2"

  liftIO $ threadDelay (1*1000000) -- 1 seconds
  mapM_ (\n -> enqueue qSidb qNameB (qval $ "bar" ++ (show n))) [3..3]
  logm "enqueue done b 3"

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
  liftIO $ threadDelay (1*1000000) -- 1 seconds

  -- liftIO $ threadDelay (10*60*1000000) -- Ten minutes
  return ()

-- ---------------------------------------------------------------------

worker_consumer :: EKG.Server -> Process ()
worker_consumer ekg = do
  mnesiaSid <- startHroqMnesia ekg
  logm "mnesia started"

  App.start_app
  logm "app started"

  {-
  {ok, QPid} = eroq_queue:start_link(my_queue, "SLAP", true).
  {ok, DlqPid} = eroq_queue:start_link(my_dlq, "SLAP-DLQ", true).
  -}
  qPid   <- startQueue (QN "SLAP",    "my_queue","SLAP",ekg)
  dlqPid <- startQueue (QN "SLAP-DLQ","my_dlq",  "SLAP",ekg)

{-
%The consumer will call CMod:CFun(Key, Message, CArgs) to process a message on the queue
CMod = my_worker_mod.
CFun = process_message.
CArgs = [my_args_any].
CInitialState = active. %active or paused - the entry state of the consumer
CDoCleanupAtShutdown = true.
AppConsumerTypeInfo = "SLAP-DLQ".
SrcQueue = my_queue.    %I.e the consumer will process messages on this queue
Dlq      = my_dlq.      %I.e. if your CMod:CFun returns an unexpected value or cause an exception, the message will be placed on this queue

{ok, CPid} = eroq_consumer:start_link(my_cons, AppConsumerTypeInfo, SrcQueue, Dlq, CMod, CFun, CArgs, CInitialState, CDoCleanupAtShutdown).

startConsumer :: (ConsumerName,String,QName,QName,ConsumerFuncClosure,AppParams,String,String,ConsumerState,Bool,EKG.Server) -> Process ProcessId

-}
  cpid <- startConsumer (CN "my_cons","SLAP-DLQ",QN "SLAP",QN "SLAP-DLQ",sampleWorker,AP [],"foo","bar",ConsumerActive,False,ekg)

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

  liftIO $ threadDelay (5*1000000)
  logm "enqueue starting SLAP"
  mapM_ (\n -> enqueue qPid (QN "SLAP") (qval $ "bar" ++ (show n))) [1..8000]
  logm "enqueue done SLAP 1"

  liftIO $ threadDelay (3*1000000)
  mapM_ (\n -> enqueue qPid (QN "SLAP") (qval $ "baz" ++ (show n))) [1..8000]
  logm "enqueue done SLAP 2"

  liftIO $ threadDelay (3*1000000)
  mapM_ (\n -> enqueue qPid (QN "SLAP") (qval $ "bat" ++ (show n))) [1..8000]
  logm "enqueue done SLAP 3"

  liftIO $ threadDelay (1*1000000) -- 1 seconds

  logm $ "blurble"
  liftIO $ threadDelay (1*1000000) -- 1 seconds

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
  mapM_ (\n -> dirty_write_q table (QE (QK "a") (qval $ "bar" ++ (show n)))) [1..80000]
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
    rtable = Data.Concurrent.Queue.Roq.DlqWorkers.__remoteTable
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
    rtable = Control.Distributed.Process.Platform.__remoteTable
           $ Data.Concurrent.Queue.Roq.AlarmServer.__remoteTable
           $ Data.Concurrent.Queue.Roq.ConsumerTH.__remoteTable
           $ Data.Concurrent.Queue.Roq.DlqWorkers.__remoteTable
           $ Data.Concurrent.Queue.Roq.Groups.__remoteTable
           $ Data.Concurrent.Queue.Roq.HandlePool.__remoteTable
           $ Data.Concurrent.Queue.Roq.QueueWatchServer.__remoteTable
           $ Data.Concurrent.Queue.Roq.SampleWorker.__remoteTable
           $ Data.Concurrent.Queue.Roq.StatsGatherer.__remoteTable
           $ initRemoteTable



-- ---------------------------------------------------------------------

-- qval str = QV $ Map.fromList [(str,str)]
qval str = (QVP $ Payload str)

-- ---------------------------------------------------------------------
{-

 (time,numberToEnqueue=)(22.725541s,10000)

-}
