{-# LANGUAGE TemplateHaskell #-}
module Data.HroqSampleWorker
  (
    sampleWorker

  , __remoteTable
  )
  where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Data.Hroq
import qualified Data.HroqConsumer as HC
import Data.HroqLogger

-- ---------------------------------------------------------------------

-- Note: the first parameter represents the environement, which is
-- serialised and remoted together with the function when is is turned
-- into a Closure
sampleWorkerBase :: Int -> QEntry -> Process (Either String HC.ConsumerReply)
sampleWorkerBase _ entry = do
  logm $ "sampleWorkerBase:" ++ (show entry)
  return (Right HC.ConsumerReplyOk)

-- ---------------------------------------------------------------------

remotable [ 'sampleWorkerBase
          -- , 'purger
          ]



-- sampleWorker :: Int -> Closure (QEntry -> Process (Either String ()))
-- sampleWorker qe = ( $(mkClosure 'sampleWorkerBase) qe)

sampleWorker :: Closure (QEntry -> Process (Either String HC.ConsumerReply))
sampleWorker = ( $(mkClosure 'sampleWorkerBase) (0::Int))



