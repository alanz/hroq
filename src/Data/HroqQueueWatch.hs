{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE DeriveDataTypeable   #-}
{-# LANGUAGE DeriveGeneric        #-}
module Data.HroqQueueWatch
  (

  -- * API
  queue_watch

  -- * Types
  , Metric (..)

  -- * Debug
  -- , ping

  -- * Remote Table
  -- , Data.HroqQueueWatchServer.__remoteTable

  ) where


import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Platform hiding (__remoteTable,monitor)
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Serializable()
import Control.Exception hiding (try,catch)
import Data.Binary
import Data.Hroq
import Data.HroqGroups
import Data.HroqLogger
import Data.HroqStatsGatherer
import Data.Time.Calendar
import Data.Time.Clock
import Data.Typeable (Typeable)
import GHC.Generics
import System.Environment
import Text.Regex.Base
import Text.Regex.TDFA
import qualified Data.Map as Map


-- ---------------------------------------------------------------------

data Metric = Size | Enq | Deq
            deriving Show

-- ---------------------------------------------------------------------
{-

-module(eroq_queue_watch).

-include("eroq.hrl").

-export([
        queue_watch/1
        ]).

-define(EROQ_QUEUE_WATCH_USE_SIZE, size).
-define(EROQ_QUEUE_WATCH_USE_ENQ,  enq).
-define(EROQ_QUEUE_WATCH_USE_DEQ,  deq).
-}

-- ---------------------------------------------------------------------
{-
queue_watch(QueueWatchConstructs)->

    case preprocess_constructs(QueueWatchConstructs, []) of
    {error, Reason} ->
        {error, Reason};

    {ok, PreprocessedCons} ->
        Queues      = eroq_groups:queues(),

        TS = make_timestamp(),

        FinalDict  = do_queue_watch_queues(Queues, dict:new(), PreprocessedCons),

        {ok, TS ++ contruct_queue_watch_string(FinalDict, PreprocessedCons)}

    end.
-}

queue_watch :: [(String,String,Metric)] -> Process String
queue_watch queueWatchConstructs = do
  logm $ "HroqQueueWatch.queue_watch entered"
  let preprocessedCons = preprocess_constructs queueWatchConstructs []
  qs <- queues
  ts <- make_timestamp
  finalDict <- do_queue_watch_queues qs Map.empty preprocessedCons
  return $ ts ++ (construct_queue_watch_string finalDict preprocessedCons)

{-
%Worker piggies
make_timestamp()->
    {Year, Month, Day}     = erlang:date(),
    {Hour, Minute, Second} = erlang:time(),
    lists:flatten(io_lib:format("[~4.4.0w-~2.2.0w-~2.2.0w ~2.2.0w:~2.2.0w:~2.2.0w]", [Year, Month, Day, Hour, Minute, Second])).
-}


make_timestamp :: Process String
make_timestamp = do
  now <- liftIO $ getCurrentTime
  return (show now)

-- ---------------------------------------------------------------------
{-
preprocess_constructs([], PreProcessedConstructs)->
    {ok, PreProcessedConstructs};
preprocess_constructs([{Label, AppInfoRegExp, Metric} | T], PreProcessedConstructs)->

    case re:compile(AppInfoRegExp) of
    {ok, CompiledRegexp} ->

        case Metric of
        "size" ->
            preprocess_constructs(T, PreProcessedConstructs ++ [{Label, CompiledRegexp, size}]);
        "enq" ->
            preprocess_constructs(T, PreProcessedConstructs ++ [{Label, CompiledRegexp, enq}]);
        "deq" ->
            preprocess_constructs(T, PreProcessedConstructs ++ [{Label, CompiledRegexp, deq}]);
        _ ->
            {error, {metric, Label, Metric}}
        end;

    _ ->
        {error, {regexp, Label, AppInfoRegExp}}
    end.
-}

preprocess_constructs ::
  [(String, String, Metric)] -> [(String, Regex, Metric)] -> [(String, Regex, Metric)]
preprocess_constructs [] preProcessedConstructs = preProcessedConstructs
preprocess_constructs ((label,appInfoRegExp,metric):t) preProcessedConstructs
  =  preprocess_constructs t preProcessedConstructs ++ [(label,compiledRegex,metric)]
  where
    mkr :: String -> Regex
    mkr reg = makeRegex reg

    compiledRegex = mkr appInfoRegExp


-- ---------------------------------------------------------------------
{-
contruct_queue_watch_string(_, [])->
    "";
contruct_queue_watch_string(Dict, [{Label, _, _} | T])->

    case dict:find(Label, Dict) of
    {ok, Value} ->
        Tag  = " " ++ Label ++ " " ++ integer_to_list(Value);
    _ ->
        Tag  = " " ++ Label ++ " 0"
    end,

    Tag ++ contruct_queue_watch_string(Dict, T).
-}
construct_queue_watch_string :: Map.Map String Integer -> [(String,Regex,Metric)] -> String
construct_queue_watch_string _ [] = ""
construct_queue_watch_string dict ((label,_,_):t)
  = tag ++ construct_queue_watch_string dict t
  where
    tag = case Map.lookup label dict of
     Just v  -> " " ++ label ++ " " ++ show v
     Nothing -> " " ++ label ++ " 0"

-- ---------------------------------------------------------------------
{-
increment_dict_value(Label, IncValue, Dict)->

    case dict:find(Label, Dict) of
    {ok, Value} ->
        NewValue = Value + IncValue;
    _ ->
        NewValue = IncValue
    end,

    dict:store(Label, NewValue, Dict).
-}

increment_dict_value ::
  (Num a, Ord k) => k -> a -> Map.Map k a -> Map.Map k a
increment_dict_value label incValue dict =
  case Map.lookup label dict of
    Just v  -> Map.insert label (v + incValue) dict
    Nothing -> Map.insert label      incValue  dict

-- ---------------------------------------------------------------------
{-
process_queue(_, Dict, [])->
    Dict;
process_queue({AppInfo, Size, Enq, Deq}, Dict, [{Label, Regexp, Metric} | T])->

    case re:run(AppInfo, Regexp, [{capture, first}]) of
    {match, _} ->

        case Metric of
        size ->
            NewDict = increment_dict_value(Label, Size, Dict);
        enq ->
            NewDict = increment_dict_value(Label, Enq, Dict);
        deq ->
            NewDict = increment_dict_value(Label, Deq, Dict)
        end;

    _ ->
        NewDict = Dict
    end,

    process_queue({AppInfo, Size, Enq, Deq}, NewDict, T).
-}

process_queue
  :: QStats
  -> Map.Map String Integer
  -> [(String,Regex,Metric)]
  -> Map.Map String Integer
process_queue _ dict [] = dict
process_queue qs@(QStats appInfo size enq deq) dict ((label,regexp,metric):t)
   = process_queue qs newDict t
  where
    newDict = if matchTest regexp appInfo
      then case metric of
        Size -> increment_dict_value label size dict
        Enq  -> increment_dict_value label enq  dict
        Deq  -> increment_dict_value label deq  dict
      else dict

-- ---------------------------------------------------------------------
{-
do_queue_watch_queues([], Dict, _)->
    Dict;
do_queue_watch_queues([QueueName | T], Dict, QueueWatchConstructs)->

    NewDict =
    case catch(eroq_stats_gatherer:get_queue_stats(QueueName)) of
    {ok, {AppInfo, Size, Enq, Deq}} ->
        process_queue({AppInfo, Size, Enq, Deq}, Dict, QueueWatchConstructs);

    _ ->
        Dict

    end,

    do_queue_watch_queues(T, NewDict, QueueWatchConstructs).
-}

do_queue_watch_queues ::
  [QName] -> Map.Map String Integer-> [(String, Regex, Metric)]
  -> Process (Map.Map String Integer)
do_queue_watch_queues [] dict _ = return dict
do_queue_watch_queues (queueName:t) dict queueWatchConstructs = do
  mQueueStats <- get_queue_stats queueName
  let newDict = case mQueueStats of
        ReplyQStats queueStats -> process_queue queueStats dict queueWatchConstructs
        ReplyQStatsNotFound -> dict
  do_queue_watch_queues t newDict queueWatchConstructs

-- ---------------------------------------------------------------------
{-
%-ifdef('TEST').
%-include_lib("eunit/include/eunit.hrl").
%
%
%eroq_queue_watch_all_test()->
%
%    mnesia:stop(),
%
%    mnesia:delete_schema([node()]),
%
%    mnesia:create_schema([node()]),
%
%    mnesia:start(),
%
%    eroq_groups:start_link(),
%
%    eroq_log_dumper:start_link(),
%
%    eroq_queue:start_link(feq1,      "feq_1",        true),
%    eroq_queue:start_link(feq1_dlq,  "feq_1_dlq",    true),
%
%    eroq_queue:start_link(feq2,      "feq_2",        true),
%    eroq_queue:start_link(feq2_dlq,  "feq_2_dlq",    true),
%
%    eroq_queue:start_link(beq1,      "beq_1",        true),
%    eroq_queue:start_link(beq1_dlq,  "beq_1_dlq",    true),
%
%    eroq_queue:start_link(beq2,      "beq_2",        true),
%    eroq_queue:start_link(beq2_dlq,  "beq_2_dlq",    true),
%
%    eroq_consumer:start_link(feq1_cons, "feq_1_cons", feq1, unset, ?MODULE, dequeue_fail, [], paused, true),
%    eroq_consumer:start_link(beq1_cons, "beq_1_cons", beq1, unset, ?MODULE, dequeue_fail, [], paused, true),
%
%    ok = eroq_queue:enqueue(feq1, "Test Message"),
%    ok = eroq_queue:enqueue(feq1, "Test Message"),
%    ok = eroq_queue:enqueue(feq2, "Test Message"),
%
%    ok = eroq_queue:enqueue(feq1_dlq, "Test Message"),
%    ok = eroq_queue:enqueue(feq1_dlq, "Test Message"),
%    ok = eroq_queue:enqueue(feq2_dlq, "Test Message"),
%    ok = eroq_queue:enqueue(feq2_dlq, "Test Message"),
%
%    ok = eroq_queue:enqueue(beq1, "Test Message"),
%    ok = eroq_queue:enqueue(beq1, "Test Message"),
%    ok = eroq_queue:enqueue(beq2, "Test Message"),
%    ok = eroq_queue:enqueue(beq2, "Test Message"),
%
%    ok = eroq_queue:enqueue(beq1_dlq, "Test Message"),
%    ok = eroq_queue:enqueue(beq1_dlq, "Test Message"),
%    ok = eroq_queue:enqueue(beq2_dlq, "Test Message"),
%    ok = eroq_queue:enqueue(beq2_dlq, "Test Message"),
%    ok = eroq_queue:enqueue(beq2_dlq, "Test Message"),
%
%    ok = eroq_queue:dequeue(feq1, ?MODULE, dequeue, []),
%    ok = eroq_queue:dequeue(feq2, ?MODULE, dequeue, []),
%
%
%    ok = eroq_queue:dequeue(feq1_dlq, ?MODULE, dequeue, []),
%    ok = eroq_queue:dequeue(feq2_dlq, ?MODULE, dequeue, []),
%
%    ok = eroq_queue:dequeue(beq1, ?MODULE, dequeue, []),
%    ok = eroq_queue:dequeue(beq2, ?MODULE, dequeue, []),
%
%    ok = eroq_queue:dequeue(beq1_dlq, ?MODULE, dequeue, []),
%    ok = eroq_queue:dequeue(beq2_dlq, ?MODULE, dequeue, []),
%
%    eroq_consumer:resume(feq1_cons),
%    eroq_consumer:resume(beq1_cons),
%
%    timer:sleep(2000),
%
%    {ok, Qw} = eroq_queue_watch:queue_watch(    [
%
%                                                {"FEQ",  "^feq_[0-9]+$",        "size"},
%                                                {"FENQ", "^feq_[0-9]+$",        "enq"},
%                                                {"FDEQ", "^feq_[0-9]+$",        "deq"},
%                                                {"FDLQ", "^feq_[0-9]+_dlq$",    "size"},
%
%                                                {"BEQ",  "^beq_[0-9]+$",        "size"},
%                                                {"BENQ", "^beq_[0-9]+$",        "enq"},
%                                                {"BDEQ", "^beq_[0-9]+$",        "deq"},
%                                                {"BDLQ", "^beq_[0-9]+_dlq$",    "size"}
%
%                                                ]),
%
%    {match, _} = re:run(Qw, "^.* FEQ 0 FENQ 3 FDEQ 3 FDLQ 2 BEQ 1 BENQ 4 BDEQ 3 BDLQ 3$", [{capture, first}]),
%
%    eroq_consumer:stop(beq1_cons),
%
%    eroq_consumer:stop(feq1_cons),
%
%    timer:sleep(2000),
%
%    eroq_queue:stop(beq2_dlq),
%    eroq_queue:stop(beq2),
%
%    eroq_queue:stop(beq1_dlq),
%    eroq_queue:stop(beq1),
%
%    eroq_queue:stop(feq2_dlq),
%    eroq_queue:stop(feq2),
%
%    eroq_queue:stop(feq1_dlq),
%    eroq_queue:stop(feq1),
%
%    timer:sleep(2000),
%
%    eroq_groups:stop(),
%
%    timer:sleep(2000),
%
%    mnesia:stop(),
%
%    mnesia:delete_schema([node()]),
%
%    ok.
%
%
%
%-endif.


%EOF
-}
