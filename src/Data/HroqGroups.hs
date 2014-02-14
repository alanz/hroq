{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE DeriveDataTypeable   #-}
{-# LANGUAGE DeriveGeneric        #-}

module Data.HroqGroups
  (
  -- * Starting the server
    hroq_groups
  , hroq_groups_closure


  -- * API
  -- ** Group membership
  , join
  , leave
  -- ** Queries
  , queues
  , consumers
  , dlq_consumers

  -- * Types
  , NameGroup (..)
  , GetMembersReply (..)

  -- * Debug

  -- * Remote Table
  , __remoteTable
  )
  where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Platform hiding (__remoteTable,monitor)
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Data.Binary
import qualified Data.Map as Map
import Data.Hroq
import Data.HroqLogger
import Data.Typeable
import GHC.Generics

--------------------------------------------------------------------------------
-- Types                                                                      --
--------------------------------------------------------------------------------

data NameGroup = NGQueue QName | NGConsumer ConsumerName | NGDlqConsumer ConsumerName
  deriving (Typeable, Generic, Eq, Show,Ord)
instance Binary NameGroup where

data Type = TQueue | TConsumer | TDlqConsumer
  deriving (Typeable, Generic, Eq, Show)
instance Binary Type where

nameGroupType :: NameGroup -> Type
nameGroupType (NGQueue _)       = TQueue
nameGroupType (NGConsumer _)    = TConsumer
nameGroupType (NGDlqConsumer _) = TDlqConsumer

-- Call and Cast request types.
data Join = Join NameGroup ProcessId
  deriving (Typeable, Generic, Eq, Show)
instance Binary Join where

-- --
data Leave = Leave NameGroup
  deriving (Typeable, Generic, Eq, Show)
instance Binary Leave where

-- --

data GetMembers = GetMembers Type
  deriving (Typeable, Generic, Eq, Show)
instance Binary GetMembers where

data GetMembersReply = QueueMembers [QName]
                     | ConsumerMembers [ConsumerName]
                     | DlqConsumerMembers [ConsumerName]
  deriving (Typeable, Generic, Eq, Show)
instance Binary GetMembersReply where

-- ---------------------------------------------------------------------

-- -record(state, {mdict = dict:new(), gdict = dict:new()}).
data State = ST { stMdict :: Map.Map MonitorRef NameGroup
                , stGdict :: Map.Map NameGroup (MonitorRef,ProcessId)
                }
emptyState :: State
emptyState = ST Map.empty Map.empty

--------------------------------------------------------------------------------
-- API                                                                        --
--------------------------------------------------------------------------------

-- -------------------------------------
{-
%%% Interface funcs
join(Name, Type) when ((Type == eroq_queue) or (Type == eroq_consumer) or (Type == eroq_dlq_consumer)) and (is_atom(Name))->
    gen_server:call(?MODULE, {join, Name, Type, self()}, ?GEN_SERVER_TIMEOUT_MS).
-}
join :: NameGroup -> Process ()
join nameGroup = do
  pid <- getServerPid
  sid <- getSelfPid
  call pid (Join nameGroup sid)


-- -------------------------------------
{-
leave(Name, Type) when ((Type == eroq_queue) or (Type == eroq_consumer) or (Type == eroq_dlq_consumer)) and (is_atom(Name))->
    gen_server:call(?MODULE, {leave, Name, Type}, ?GEN_SERVER_TIMEOUT_MS).
-}
leave :: NameGroup -> Process ()
leave nameGroup = do
  pid <- getServerPid
  sid <- getSelfPid
  call pid (Join nameGroup sid)


-- -------------------------------------
{-
queues() ->
    gen_server:call(?MODULE, {members, eroq_queue}, ?GEN_SERVER_TIMEOUT_MS).
-}
queues :: Process GetMembersReply
queues = do
  pid <- getServerPid
  call pid (GetMembers TQueue)

-- -------------------------------------

{-
consumers() ->
    gen_server:call(?MODULE, {members, eroq_consumer}, ?GEN_SERVER_TIMEOUT_MS).
-}
consumers :: Process GetMembersReply
consumers = do
  pid <- getServerPid
  call pid (GetMembers TConsumer)

-- -------------------------------------
{-
dlq_consumers() ->
    gen_server:call(?MODULE, {members, eroq_dlq_consumer}, ?GEN_SERVER_TIMEOUT_MS).

-}
dlq_consumers :: Process GetMembersReply
dlq_consumers = do
  pid <- getServerPid
  call pid (GetMembers TDlqConsumer)

-- ---------------------------------------------------------------------

getServerPid :: Process ProcessId
getServerPid = do
  mpid <- whereis hroqGroupsProcessName
  case mpid of
    Just pid -> return pid
    Nothing -> do
      logm "HroqGroups:getServerPid failed"
      error "blow up"

hroqGroupsProcessName :: String
hroqGroupsProcessName = "HroqGroups"

-- ---------------------------------------------------------------------
{-
%%% Behaviour funcs
start_link()->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) -> {ok, #state{}}.

-}

hroq_groups :: Process ()
hroq_groups = start_groups

start_groups :: Process ()
start_groups = do
  self <- getSelfPid
  register hroqGroupsProcessName self
  serve 0 initFunc serverDefinition
  where initFunc :: InitHandler Integer State
        initFunc i = do
          logm $ "HroqGroup:start.initFunc"
          return $ InitOk emptyState Infinity

serverDefinition :: ProcessDefinition State
serverDefinition = defaultProcess {
     apiHandlers = [
          handleCall handleJoinCall
        , handleCall handleLeaveCall
        , handleCall handleMembersCall

        -- , handleCast handlePublishQueueStatsCast
        -- , handleCast handlePublishConsumerStatsCast
        -- , handleCast (\s Ping -> do {logm $ "HroqGroups:ping"; continue s })

        ]
    , infoHandlers =
        [
         handleInfo handleInfoProcessMonitorNotification
        ]
     , timeoutHandler = \_ _ -> do
           {logm "HroqGroups:timout exit"; stop $ ExitOther "timeout az"}
     , shutdownHandler = \_ reason -> do
           { logm $ "HroqGroups terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State


-- ---------------------------------------------------------------------

{-

handle_call({join, Name, Type, Pid}, _From, #state{mdict = M, gdict = G} = State) ->
    case dict:find({Type, Name}, G) of
    {ok, _} ->
        %Already registered... ignore
        {reply, ok, State};
    error ->
        Ref = monitor(process, Pid),
        {reply, ok, State#state{mdict = dict:store(Ref, {Type, Name}, M), gdict = dict:store({Type, Name}, {Ref, Pid}, G)}}
    end;
-}
handleJoinCall :: State -> Join -> Process (ProcessReply () State)
handleJoinCall st@(ST {stMdict = m, stGdict = g}) (Join nameGroup pid) = do
    logm $ "handleJoinCall called with:" ++ (show (nameGroup))

    case Map.lookup nameGroup g of
      Just _ -> reply () st
      Nothing -> do
        ref <- monitor pid
        reply () st { stMdict = Map.insert ref nameGroup m
                    , stGdict = Map.insert nameGroup (ref,pid) g
                    }

-- ---------------------------------------------------------------------
{-
handle_call({leave, Name, Type}, _From, #state{mdict = M, gdict = G} = State) ->
    case dict:find({Type, Name}, G) of
    {ok, {Ref, _Pid}} ->
        true = demonitor(Ref, [flush]),
        {reply, ok, State#state{mdict = dict:erase(Ref, M), gdict = dict:erase({Type, Name}, G)}};
    error ->
        %Not found - ignore ....
        {reply, ok, State}
    end;
-}
handleLeaveCall :: State -> Leave -> Process (ProcessReply () State)
handleLeaveCall st@(ST {stMdict = m, stGdict = g}) (Leave nameGroup) = do
    logm $ "handleLeaveCall called with:" ++ (show (nameGroup))

    case Map.lookup nameGroup g of
      Just (ref,_pid) -> do
        unmonitor ref
        reply () st { stMdict = Map.delete ref m
                    , stGdict = Map.delete nameGroup g
                    }
      -- Not found - ignore ....
      Nothing -> reply () st

-- ---------------------------------------------------------------------
{-
handle_call({members, Type}, _From, #state{gdict = G} = State) ->
    Res = [N || {{T,N}, _} <- dict:to_list(G), T == Type],
    {reply, Res, State};
-}
handleMembersCall :: State -> GetMembers -> Process (ProcessReply GetMembersReply State)
handleMembersCall st@(ST {stGdict = g}) (GetMembers groupType) = do
    logm $ "handleMembersCall called with:" ++ (show (groupType))

    let members = [ ng | (ng,_) <- Map.toList g, nameGroupType ng == groupType]
        toq   xs = map (\(NGQueue       n) -> n) xs
        toc   xs = map (\(NGConsumer    n) -> n) xs
        todlq xs = map (\(NGDlqConsumer n) -> n) xs

    case groupType of
        TQueue       -> reply (QueueMembers       (toq members)) st
        TConsumer    -> reply (ConsumerMembers    (toc members)) st
        TDlqConsumer -> reply (DlqConsumerMembers (todlq members)) st


-- ---------------------------------------------------------------------
{-
handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.
-}

-- ---------------------------------------------------------------------
{-
handle_info({'DOWN', Ref, process, _Pid, _Reason}, #state{mdict = M, gdict = G} = State) ->
    case dict:find(Ref, M) of
    {ok, GKey} ->
        {noreply, State#state{mdict = dict:erase(Ref, M), gdict = dict:erase(GKey, G)}};
    error ->
        {noreply, State}
    end;
handle_info(_, State) ->
    {noreply, State}.
-}
handleInfoProcessMonitorNotification :: State -> ProcessMonitorNotification -> Process (ProcessAction State)
handleInfoProcessMonitorNotification st n@(ProcessMonitorNotification ref _pid _reason) = do
  logm $ "handleInfoProcessMonitorNotification called with: " ++ show n
  let m = stMdict st
      g = stGdict st

  case Map.lookup ref m of
        Just key -> continue st { stMdict = Map.delete ref m
                                , stGdict = Map.delete key g }
        Nothing                -> continue st

-- ---------------------------------------------------------------------
{-
terminate(_, _) -> ok.

code_change(_,State,_)-> {ok, State}.

%EOF
-}

-- ---------------------------------------------------------------------

-- NOTE: the TH crap has to be a the end, as it can only see the stuff lexically before it in the file

$(remotable [ 'start_groups
            ])

hroq_groups_closure :: Closure (Process ())
hroq_groups_closure = ($(mkStaticClosure 'start_groups))
