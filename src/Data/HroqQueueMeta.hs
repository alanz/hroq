module Data.HroqQueueMeta
  (
    meta_add_bucket
  , meta_all_buckets
  , meta_del_bucket
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
import Data.List
import Data.Typeable (Typeable)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
-- import qualified Data.ByteString.Lazy as B
import qualified Data.Map as Map
import qualified Data.Set as Set
import Data.Hroq
import Data.HroqGroups
import Data.HroqMnesia
import Data.HroqStatsGatherer
import Data.HroqUtil

meta_add_bucket :: QName -> TableName -> Process ()
meta_add_bucket queueName bucket = do
  say "meta_add_bucket undefined"
  return ()

meta_all_buckets :: QName -> Process [TableName]
meta_all_buckets queueName = do
  say "meta_add_bucket undefined"
  return []

meta_del_bucket queueName bucket = do
  say "meta_del_bucket undefined"

