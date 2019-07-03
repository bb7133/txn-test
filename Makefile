TAGS =

default: build

build: export GO111MODULE=on
build:
ifeq ($(TAGS),)
	$(CGO_FLAGS) go build -o bin/txn-test test_pessismistic.go
else
	$(CGO_FLAGS) go build -tags "$(TAGS)" -o bin/txn-test test_pessismistic.go
endif

check:
	golint -set_exit_status ...

