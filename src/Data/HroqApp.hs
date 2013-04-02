{-# LANGUAGE DeriveDataTypeable  #-}

module Data.HroqApp
  (
    start_app
  , create_queue_meta_table
  )
  where

import Control.Concurrent
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
-- import Control.Workflow
import Data.Binary
import Data.DeriveTH
import Data.Hroq
import Data.HroqLogger
import Data.HroqSup
import Data.HroqGroups
import Data.HroqMnesia
import Data.HroqQueueMeta
import Data.HroqStatsGatherer
import Data.HroqUtil
import Data.List
import Data.Maybe
import Data.Persistent.Collection
import Data.RefSerialize
import Data.TCache
import Data.Typeable (Typeable)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import qualified Data.Map as Map
import qualified Data.Set as Set

-- ---------------------------------------------------------------------
{-
start(_, _) ->

    case mnesia:table_info(schema, storage_type) of
    ram_copies ->
        ?info("Changing MNESIA schema from ram_copies to disc_copies..."),
        {atomic, ok} = mnesia:change_table_copy_type(schema, node(), disc_copies);
    disc_copies ->
        ok
    end,

    WaitForMnesia = get_wait_for_mnesia(),

    if (WaitForMnesia == true) ->
        AllTables = mnesia:system_info(tables),
        ?info("Wait tables ["++integer_to_list(length(AllTables))++"] mnesia..."),
        ok = mnesia:wait_for_tables(AllTables, infinity),
        ?info("Wait tables ["++integer_to_list(length(AllTables))++"] mnesia... Done.");
    true ->
        ?info("Not waiting for MNESIA to load all tables..."),
        ok
    end,

    ?info("Creating consumer local storage table..."),
    ok = create_consumer_local_storage_table(),

    ?info("Creating queue meta table..."),
    ok = create_queue_meta_table(),

    AlarmFun = get_callback(alarms_callback),
    QWFun    = get_callback(queue_watch_callback),

    eroq_sup:start_link(AlarmFun, QWFun).

stop(_) ->
    ok.
-}

start_app :: Process ()
start_app = do
    logm "HroqApp.start"

{-
    case mnesia:table_info(schema, storage_type) of
    ram_copies ->
        ?info("Changing MNESIA schema from ram_copies to disc_copies..."),
        {atomic, ok} = mnesia:change_table_copy_type(schema, node(), disc_copies);
    disc_copies ->
        ok
    end,

    WaitForMnesia = get_wait_for_mnesia(),

    if (WaitForMnesia == true) ->
        AllTables = mnesia:system_info(tables),
        ?info("Wait tables ["++integer_to_list(length(AllTables))++"] mnesia..."),
        ok = mnesia:wait_for_tables(AllTables, infinity),
        ?info("Wait tables ["++integer_to_list(length(AllTables))++"] mnesia... Done.");
    true ->
        ?info("Not waiting for MNESIA to load all tables..."),
        ok
    end,
-}

    logm ("Creating consumer local storage table...")
    create_consumer_local_storage_table

    logm ("Creating queue meta table...")
    create_queue_meta_table

    -- AlarmFun = get_callback(alarms_callback),
    let alarmFun = get_callback "alarms_callback"
    -- QWFun    = get_callback(queue_watch_callback),
    let qwFun = get_callback "queue_watch_callback"
    -- eroq_sup:start_link(AlarmFun, QWFun).
    hroq_start_link alarmFun qwFun


-- ---------------------------------------------------------------------

{-
create_queue_meta_table() ->
    TableName = eroq_queue_meta_table,
    case mnesia:create_table(TableName, [{attributes, record_info(fields, eroq_queue_meta)}, {type, set}, {disc_copies, [node()]}, {record_name, eroq_queue_meta}]) of
    {atomic, ok} ->
        ok;
    {aborted,{already_exists,TableName}} ->
        case mnesia:table_info(TableName, storage_type) of
        disc_copies ->
            ok;
        _ ->
            {atomic, ok} = mnesia:change_table_copy_type(TableName, node(), disc_copies),
            ok
        end
    end.
-}

hroq_queue_meta_table :: TableName
hroq_queue_meta_table = TN "hroq_queue_meta_table"

create_queue_meta_table :: Process ()
create_queue_meta_table = do
  logm "create_queue_meta_table undefined"
  let tableName = hroq_queue_meta_table
  res <- create_table DiscCopies tableName
  logm $ "create_queue_meta_table:create_table res=" ++ (show res)
  return ()

-- ---------------------------------------------------------------------

hroq_consumer_local_storage_table :: TableName
hroq_consumer_local_storage_table = TN "hroq_consumer_local_storage_table"

create_consumer_local_storage_table :: Process ()
create_consumer_local_storage_table = do
  logm "create_consumer_local_storage_table undefined"
  let tableName = hroq_consumer_local_storage_table
  res <- create_table DiscCopies tableName
  logm $ "create_consumer_local_storage_table:create_table res=" ++ (show res)
  return ()
{-
    case mnesia:create_table(TableName, [{attributes, record_info(fields, eroq_consumer_message)}, {type, set}, {disc_copies, [node()]}, {record_name, eroq_consumer_message}]) of
    {atomic, ok} ->
        ok;
    {aborted,{already_exists,TableName}} ->
        case mnesia:table_info(TableName, storage_type) of
        disc_copies ->
            ok;
        _ ->
            {atomic, ok} = mnesia:change_table_copy_type(TableName, node(), disc_copies),
            ok
        end

    end.
-}
-- ---------------------------------------------------------------------
{-
get_callback(CBName) ->
    case application:get_env(mira_eroq, CBName) of
    {ok, {Mod, Func}} ->
        fun(P) -> Mod:Func(P) end;
    D ->
        ?warn({"Invalid callback definition for " ++ atom_to_list(CBName) ++ ": ",  D}),
        fun (_P) -> ok end
    end.
-}
get_callback cbname = do
  logm $ "get_callback undefined:" ++ (show cbname)

