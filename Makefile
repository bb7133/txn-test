TAGS =

default: build

build: export GO111MODULE=on
build:
ifeq ($(TAGS),)
	$(CGO_FLAGS) go build -o bin/txn-test test_pessismistic.go
	$(CGO_FLAGS) go build -o bin/txn-multi-test src/test_pessismistic_multi_tables.go
else
	$(CGO_FLAGS) go build -tags "$(TAGS)" -o bin/txn-test test_pessismistic.go
	$(CGO_FLAGS) go build -tags "$(TAGS)" -o bin/txn-multi-test test_pessismistic_multi_tables.go
endif

check:
	golint -set_exit_status ...

