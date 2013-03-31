module Data.HroqSup
  (
  hroq_start_link
  )
  where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Concurrent
import Control.Workflow
import Data.Binary
import Data.Maybe
import Data.HroqLogger
import Data.Persistent.Collection
import Data.RefSerialize
import Data.TCache
import Data.TCache.Defs
import Data.Typeable
import qualified Data.Map as Map
import qualified Data.ByteString.Lazy.Char8 as C8

-- ---------------------------------------------------------------------

hroq_start_link alarmFun qwFun = do
  logm "hroq_start_link undefined"

