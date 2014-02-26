{-# LANGUAGE DeriveDataTypeable  #-}

module Data.HroqApp
  (
    start_app
  , create_queue_meta_table
  , hroq_consumer_local_storage_table

  )
  where

import Control.Distributed.Process hiding (call)
import Data.Hroq
import Data.HroqLogger
import Data.HroqSup
import Data.HroqMnesia
import Data.HroqQueueMeta
import Data.HroqQueueWatchServer

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
    let qwFun = queueWatchNoOpCallbackClosure
    -- let qwFun = get_callback "queue_watch_callback"

    -- eroq_sup:start_link(AlarmFun, QWFun).

    supPid <- hroq_start_link alarmFun qwFun
    -- supPid <- hroq_start_link undefined undefined

    return ()


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

create_queue_meta_table :: Process ()
create_queue_meta_table = do
  let tableName = hroq_queue_meta_table
  res <- create_table DiscCopies tableName RecordTypeMeta
  logm $ "create_queue_meta_table:create_table res=" ++ (show res)
  return ()

-- ---------------------------------------------------------------------

hroq_consumer_local_storage_table :: TableName
hroq_consumer_local_storage_table = TN "hroq_consumer_local_storage_table"

create_consumer_local_storage_table :: Process ()
create_consumer_local_storage_table = do
  logm "create_consumer_local_storage_table"
  let tableName = hroq_consumer_local_storage_table
  res <- create_table DiscCopies tableName RecordTypeConsumerLocal
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
get_callback "alarms_callback" = f
  where
    f = do
      logm $ "get_callback undefined:alarms_callback"
get_callback "queue_watch_callback" = f
  where
    f = do
      logm $ "get_callback undefined:queue_watch_callback"
get_callback unk = do
      logm $ "get_callback undefined:" ++ show unk
