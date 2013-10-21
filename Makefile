CONF=./dist/setup-config
CABAL=hroq.cabal
BUILD_DEPENDS=$(CONF) $(CABAL)

BASE_GIT := git://github.com/haskell-distributed
REPOS=$(shell cat REPOS | sed '/^$$/d')

.PHONY: all
all: build

.PHONY: test
test: build
	cabal test --show-details=always

.PHONY: build
build: configure
	cabal build

.PHONY: configure
configure: $(BUILD_DEPENDS)

.PHONY: ci
ci: $(REPOS) test

$(BUILD_DEPENDS): sandbox
	cabal configure --enable-tests

.PHONY: deps
deps: $(REPOS) sandbox
	cabal install --dependencies-only
	cabal install --enable-tests

$(REPOS): sandbox
	- mkdir ./subrepos
	- (cd ./subrepos && git clone $(BASE_GIT)/$@.git)
	(cd ./subrepos/$@ && git fetch && git checkout development)
	cabal sandbox add-source ./subrepos/$@
	cabal install $@

.PHONY: sandbox
sandbox: cabal.sandbox.config

cabal.sandbox.config:
	cabal sandbox init

.PHONY: clean
clean:
	cabal clean

.PHONY: reallyclean
reallyclean:
	cabal clean
	cabal sandbox delete


