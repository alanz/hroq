module Data.HroqMnesia
  (
    TableStorage(..)
  , TableName(..)
  , change_table_copy_type
  , create_table
  , delete_table
  , dirty_write
  , dirty_read
  , dirty_all_keys
  , wait_for_tables

  , TableInfoReq(..)
  , TableInfoRsp(..)
  , table_info
  )
  where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Data.Binary
import Data.DeriveTH
import Data.Typeable (Typeable)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import qualified Data.Map as Map
import Data.Hroq
import Data.Persistent.Collection
import Data.RefSerialize
import Data.TCache
import Data.Typeable


data TableStorage = DiscOnlyCopies
                  | DiscCopies
                  | StorageNone
                  deriving (Show)

data TableName = TN String
                 deriving (Show)

change_table_copy_type :: TableName -> TableStorage -> Process ()
change_table_copy_type bucket storageType = do
  say $ "change_table_copy_type undefined for:" ++ (show (bucket,storageType))

-- ---------------------------------------------------------------------

create_table :: TableStorage -> TableName -> Process ()
create_table storage name = do
  say "create_table undefined"

-- ---------------------------------------------------------------------

delete_table :: TableName -> Process ()
delete_table name = do
  say "create_table undefined"

-- ---------------------------------------------------------------------
-- |Write the value to a TCache Q
dirty_write :: TableName -> QEntry -> Process ()
dirty_write tableName record = do
  let qname = getQueue tableName
  liftIO $ push qname record
  liftIO $ syncCache
  return ()

-- ---------------------------------------------------------------------

dirty_read :: TableName -> QKey -> Process [QEntry]
dirty_read tableName key = do
  say $ "dirty_read:undefined:" ++ (show (tableName,key))
  return []

-- ---------------------------------------------------------------------

dirty_all_keys :: TableName -> Process [QKey]
dirty_all_keys tableName = do
  say "dirty_all_keys undefined"
  return []

-- ---------------------------------------------------------------------

wait_for_tables :: [TableName] -> Delay -> Process ()
wait_for_tables tables maxWait = do
  say $ "wait_for_tables undefined"


-- ---------------------------------------------------------------------

data TableInfoReq = TableInfoSize
                  | TableInfoStorageType
                    deriving (Show)

data TableInfoRsp = TISize Integer
                  | TIStorageType TableStorage
                    deriving (Show)

table_info :: TableName -> TableInfoReq -> Process TableInfoRsp
table_info tableName TableInfoSize        = getBucketSize  tableName
table_info tableName TableInfoStorageType = getStorageType tableName
table_info tableName infoReq = do
  say $ "table_info undefined:" ++ (show (tableName,infoReq))
  error "foo"

-- ---------------------------------------------------------------------

getBucketSize :: TableName -> Process TableInfoRsp
getBucketSize tableName = do
  say "getBucketSize undefined"
  error "foo"

getStorageType :: TableName -> Process TableInfoRsp
getStorageType tableName = do
  say "getStorageType undefined"
  error "foo"

-- ---------------------------------------------------------------------
-- TCache specific functions used here

-- | Provide an identification of a specific Q
getQueue :: TableName -> RefQueue QEntry
getQueue (TN name) = getQRef name


