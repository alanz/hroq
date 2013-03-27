module Data.HroqUtil
  (
    TableStorage(..)
  , retry_dirty_write
  , generate_key
  , change_table_copy_type
  , getBucketSize
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


data TableStorage = DiscOnlyCopies

retry_dirty_write = undefined

generate_key :: Process QKey
generate_key = undefined

change_table_copy_type :: OverflowBucket -> TableStorage -> Process ()
change_table_copy_type bucket DiscOnlyCopies = undefined

getBucketSize :: OverflowBucket -> Process Integer
getBucketSize bucket = undefined

