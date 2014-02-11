module Data.HroqGroups
  (
    join
  , hroq_groups
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
-- import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import qualified Data.Map as Map
import Data.Hroq
import Data.HroqLogger
-- import Data.Persistent.Collection
import Data.RefSerialize
-- import Data.TCache
import Data.Typeable

-- ---------------------------------------------------------------------

hroq_groups = error $ "hroq_groups undefined"

-- ---------------------------------------------------------------------

join :: QName -> String -> Process ()
join qname moduleName = do
  logm "join undefined"



