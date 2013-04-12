#!/bin/sh

# cabal build --ghc-options="-O2"
# cabal build --ghc-options="-O2 -prof -auto-all -caf-all -fforce-recomp -osuf=.o_p"

#cabal build --ghc-options="-O2 -prof -auto-all -caf-all -fforce-recomp "

# TH build
#cabal build --ghc-options="-O2"
#cabal build --ghc-options="-O2 -prof -auto-all -caf-all -fforce-recomp -fprof-auto-top -fprof-auto-calls -osuf=.o_p"


cabal build --ghc-options="-O2 -prof -auto-all -caf-all -fforce-recomp -fprof-auto-top -fprof-auto-calls "


