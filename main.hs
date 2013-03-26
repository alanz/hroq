{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE FlexibleInstances #-}

import Control.Concurrent
import Control.Workflow
import Data.Binary
import Data.Maybe
import Data.Hroq
import Data.Persistent.Collection
import Data.Persistent.Collection
import Data.RefSerialize
import Data.TCache
import Data.Typeable
import qualified Data.Map as Map

-- ---------------------------------------------------------------------

-- | Provide an identification of a specific Q
qtest ::  RefQueue QEntry
qtest = getQRef "binaryq"

qval str = Map.fromList [(str,str)]

main = do
  syncWrite Synchronous
  push qtest (QE 1 (qval "foo"))
  push qtest (QE 2 (qval "bar"))
  syncCache

mm = do
  syncWrite Synchronous
  syncCache
  pickAll qtest

