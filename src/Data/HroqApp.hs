{-# LANGUAGE DeriveDataTypeable  #-}

module Data.HroqApp
  (
    create_queue_meta_table
  )
  where

import Control.Concurrent
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Workflow
import Data.Binary
import Data.DeriveTH
import Data.Hroq
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
  say "create_queue_meta_table undefined"
