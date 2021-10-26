CURDIR := $(shell pwd)
DIR:=TEST_SOURCE_PATH=$(CURDIR)

##
## List of commands:
##

## default:
all: test

test:
	 go test  -race ./...

mod:
	@echo "======================================================================"
	@echo "Run MOD"
	@go mod verify
	@go mod tidy
	@go mod vendor
	@go mod download
	@go mod verify
