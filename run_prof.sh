#!/bin/sh

# Get data by cost centre
#./dist/build/hroq/hroq +RTS -hc -RTS


# Get data by originating module
#./dist/build/hroq/hroq +RTS -hm -RTS

# Get data by type
#./dist/build/hroq/hroq +RTS -hy -RTS

# Get data by closure description
./dist/build/hroq/hroq +RTS -hd -RTS

