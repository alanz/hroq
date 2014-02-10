hroq
====

Haskell Robust Overflow Queue


The dev environment is running Debian/testing, GHC 7.6.3, haskell
platform 2013.2.2.0

Build process
-------------

## Install haskell-distributed into a sandbox

    cd /some/directory/
    git clone https://github.com/alanz/distributed-process-platform.git
    cd distributed-process-platform
    make -f Makefile.dev

This should result in a full version of haskell-distributed installed
into a sandbox at
`/some/directory/distributed-process-platform/.cabal-sandbox`

## Initialise sandbox for this project to make use of the haskell-distributed one

    cabal sandbox init --sandbox=/some/directory/distributed-process-platform/.cabal-sandbox

NOTE: you must use the full path, a `~` does not get interpreted properly

On my box

    cabal sandbox init --sandbox=/home/alanz/mysrc/github/alanz/distributed-process-platform/.cabal-sandbox

## Normal dev cycle

    cabal clean && cabal configure && cabal build


Contributors:

  Part of the code based on https://github.com/agocorona/TCache


