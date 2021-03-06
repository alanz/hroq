module Data.Concurrent.Queue.Roq.Sup
  (
  hroq_start_link
  )
  where

import Control.Distributed.Process hiding (call)
-- import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Supervisor
import Control.Distributed.Process.Platform.Time
import Data.Concurrent.Queue.Roq.AlarmServer
import Data.Concurrent.Queue.Roq.Groups
import Data.Concurrent.Queue.Roq.HandlePool
import Data.Concurrent.Queue.Roq.Logger
import Data.Concurrent.Queue.Roq.StatsGatherer
import Data.Concurrent.Queue.Roq.QueueWatchServer

-- ---------------------------------------------------------------------

hroq_start_link :: a -> Closure (String -> Process ()) -> Process ProcessId
hroq_start_link alarmFun qwFun = do
  logm "hroq_start_link starting"
  pid <- start restartStrategy (childSpec alarmFun qwFun)
  logm "hroq_start_link started"
  return pid


  -- For now, start each designated process via code, move to
  -- supervisor later (when it exists)

  -- But we are actually not using any of these at the moment. hmm.

{-

start_link(AlarmFun, QueueWatchFun) when is_function(AlarmFun,1) and is_function(QueueWatchFun,1) ->
    supervisor:start_link({local,?MODULE}, ?MODULE, [AlarmFun, QueueWatchFun]).

init([AlarmFun, QueueWatchFun]) ->
    RestartSpec = {one_for_all, 1, 10},

    % one_for_all - if one child process terminates and should be
    % restarted, all other child processes are terminated and then all child
    % processes are restarted.

    % To prevent a supervisor from getting into an infinite loop of
    % child process terminations and restarts, a maximum restart
    % frequency is defined using two integer values MaxR and MaxT. If
    % more than MaxR restarts occur within MaxT seconds, the
    % supervisor terminates all child processes and then itself.


    ERoqStatsGather = {eroq_stats_gatherer,
                        {eroq_stats_gatherer, start_link, []},
                        permanent, 5000, worker, [eroq_stats_gatherer]},

    ERoqLogDumper = {eroq_log_dumper,
                       {eroq_log_dumper, start_link, []},
                       permanent, 5000, worker, [eroq_log_dumper]},

    ERoqGroups = {eroq_groups,
                       {eroq_groups, start_link, []},
                       permanent, 5000, worker, [eroq_groups]},

    ERoqAlarms     = {eroq_alarms,
                         {eroq_alarm_server, start_link, [AlarmFun]},
                         permanent, 5000, worker, [eroq_alarm_server]},

    ERoqQueueWatch = {eroq_queue_watch,
                         {eroq_queue_watch_server, start_link, [QueueWatchFun]},
                         permanent, 5000, worker, [eroq_queue_watch_server]},

    {ok, {RestartSpec, [ERoqStatsGather, ERoqLogDumper, ERoqGroups, ERoqAlarms, ERoqQueueWatch]}}.

-}


-- ----------------------------------------------------------------------

childSpec :: a -> Closure (String -> Process ()) -> [ChildSpec]
childSpec alarmFun queueWatchFun =
    [
      defaultWorker hroqStatsGatherer
    -- , defaultWorker hroq_log_dumper
    , defaultWorker hroqGroups
    , defaultWorker hroqAlarms
    , defaultWorker hroqQueueWatch
    , defaultWorker hroqHandlePool
    ]
  where
    hroqStatsGatherer = RunClosure hroq_stats_gatherer_closure
    hroqGroups        = RunClosure hroq_groups_closure
    hroqAlarms        = RunClosure hroq_alarm_server_closure
    hroqQueueWatch    = RunClosure (hroq_queue_watch_server_closure queueWatchNoOpCallbackClosure)
    hroqHandlePool    = RunClosure hroq_handle_pool_server_closure

defaultWorker :: ChildStart -> ChildSpec
defaultWorker clj =
  ChildSpec
  {
    childKey     = ""
  , childType    = Worker
  , childRestart = Permanent
  , childStop    = TerminateTimeout (Delay $ milliSeconds 5000)
  , childStart   = clj
  , childRegName = Nothing
  }

-- ---------------------------------------------------------------------

restartStrategy :: RestartStrategy
restartStrategy = -- restartAll
   RestartAll {intensity = RestartLimit {maxR = maxRestarts 5,
                                         maxT = seconds 60},
               mode = RestartInOrder {order = LeftToRight}}


