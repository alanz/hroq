#!/bin/sh

rm -fr .hroqdata

# Get data by cost centre
#./dist/build/hroq/hroq +RTS -hc -RTS
#./dist/build/hroq/hroq +RTS -hc -xc -RTS
#./dist/build/hroq/hroq +RTS -hc -xt -RTS
#./dist/build/hroq/hroq +RTS -hc -p -RTS
#./dist/build/hroq/hroq +RTS -hc -p  -RTS

# Get data by originating module
#./dist/build/hroq/hroq +RTS -hm -RTS
#./dist/build/hroq/hroq +RTS -hmData.HroqMnesia -RTS
#./dist/build/hroq/hroq +RTS -hmData.HroqQueue -RTS

# Get data by closure description
./dist/build/hroq/hroq +RTS -hd -p -xc -RTS

# Get data by type
#./dist/build/hroq/hroq +RTS -hy -RTS

# Get data by retainer set
#./dist/build/hroq/hroq +RTS -hr -RTS

# Get data by biography
#./dist/build/hroq/hroq +RTS -hb -RTS
#./dist/build/hroq/hroq +RTS -hc -hbdrag,void -RTS



hp2ps -c hroq.hp

