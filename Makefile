APP_NAME=requester
GRPC_API_PROTO_PATH:=./api/grpc

GOLANGCILINT_VERSION	   := v1.58.2
PROTOLINT_VERSION		   := v0.42.2
MOCKGEN_VERSION            := v1.6.0
PROTOC_GEN_GO_VERSION      := v1.33.0
PROTOC_GEN_GO_GRPC_VERSION := v1.3.0
PROTOC_VERSION             := 3.20.3

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)

## Generic
.PHONY: help
help: # show list of all commands
	@awk 'BEGIN {FS = ":.*?# "} { \
		if (/^[a-zA-Z_-]+:.*?#.*$$/) \
			{ printf "  ${YELLOW}%-20s${RESET}${WHTIE}%s${RESET}\n", $$1, $$2} \
		else if (/^## .*$$/) \
			{ printf "${CYAN}%s:${RESET}\n", substr($$1,4)} \
		}' $(MAKEFILE_LIST)

## Testing

.PHONY: test-coverprofile
test-coverprofile:
	@go test ./... -count=1 -cover -coverprofile=cover.out

.PHONY: test
test: test-coverprofile # run tests
	@go tool cover -func=cover.out

.PHONY: test-coverage
test-coverage: _test-coverprofile # run tests and show coverage
	@go tool cover -html cover.out

## Linting

.PHONY: lint
lint: lint-go lint-proto # run linters

.PHONY: lint-go
lint-go: # lint .go files
	@LOCAL_VERSION=`golangci-lint --version | cut -d ' ' -f 4`; \
	if [ "$$LOCAL_VERSION" != "$(GOLANGCILINT_VERSION)" ]; then \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCILINT_VERSION); \
		LOCAL_VERSION=`golangci-lint --version | cut -d ' ' -f 4`; \
		echo "$(CYAN)Installed golangci-lint version $$LOCAL_VERSION$(RESET)"; \
	fi; \
	echo "$(CYAN)Running golangci-lint on version $$LOCAL_VERSION$(RESET)"; \
	golangci-lint run -v --timeout 5m

.PHONY: lint-proto
lint-proto: # lint .proto files
	@LOCAL_VERSION=`protolint version | cut -d ' ' -f 3`; \
	if [ "$$LOCAL_VERSION" != "$(PROTOLINT_VERSION)" ]; then \
	 	go install github.com/yoheimuta/protolint/cmd/protolint@$(PROTOLINT_VERSION); \
	 	LOCAL_VERSION=`protolint version | cut -d ' ' -f 3`; \
	fi; \
	echo "$(CYAN)Running protolint on version $$LOCAL_VERSION$(RESET)"; \
	protolint lint -reporter unix $(GRPC_API_PROTO_PATH)

## Source generation

.PHONY: gen
gen: gen-mock gen-grpc # run all source generators

.PHONY: gen-mock
gen-mock: # generate mocks go sources
	@LOCAL_VERSION=`mockgen --version | cut -d ' ' -f 3`; \
	if [ "$$LOCAL_VERSION" != "$(MOCKGEN_VERSION)" ]; then \
		go install github.com/golang/mock/mockgen@$(MOCKGEN_VERSION); \
		LOCAL_VERSION=`mockgen --version | cut -d ' ' -f 3`; \
		echo "$(CYAN)Installed mockgen version $$LOCAL_VERSION$(RESET)"; \
	fi; \
	echo "$(CYAN)Running mockgen on version $$LOCAL_VERSION$(RESET)"; \
	mockgen \
		-source ./internal/$(APP_NAME)/logic/dataprovider.go \
		-destination ./internal/$(APP_NAME)/dataproviders/mock_dataproviders/dataprovider_mocks.go
	mockgen \
		-source ./internal/$(APP_NAME)/logic/logic.go \
		-destination ./internal/$(APP_NAME)/logic/mock_logic/logic_mocks.go

GRPC_INSTALL_SOURCE_WIN:=https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-win64.zip
GRPC_INSTALL_SOURCE_LIN:=https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-linux-x86_64.zip
GRPC_INSTALL_FILENAME:=third_party/protoc.zip

.PHONY: install-grpc
install-grpc:
ifeq ($(OS),Windows_NT) # Windows
	@powershell -Command "Invoke-WebRequest -OutFile ${GRPC_INSTALL_FILENAME} -Uri ${GRPC_INSTALL_SOURCE_WIN}"
	@echo "$(CYAN)Downloaded protoc to $(RESET)";
	@powershell -Command "Expand-Archive -Path ${GRPC_INSTALL_FILENAME} -DestinationPath third_party/protoc -Force"
	@echo "$(CYAN)Unzipped protoc to third_party/protoc$(RESET)";
	@rm ${GRPC_INSTALL_FILENAME}
	@LOCAL_VERSION=`third_party/protoc/bin/protoc.exe --version | cut -d ' ' -f 2`; \
	echo "$(CYAN)Installed protoc version $$LOCAL_VERSION$(RESET)";
else # Linux
	@wget -qO ${GRPC_INSTALL_FILENAME} ${GRPC_INSTALL_SOURCE_LIN}
	@echo "$(CYAN)Downloaded protoc$(RESET)";
	@unzip -qod third_party/protoc ${GRPC_INSTALL_FILENAME}
	@echo "$(CYAN)Unzipped protoc to third_party/protoc$(RESET)";
	@rm -f ${GRPC_INSTALL_FILENAME}
	@LOCAL_VERSION=`third_party/protoc/bin/protoc --version | cut -d ' ' -f 2`; \
	echo "$(CYAN)Installed protoc version $$LOCAL_VERSION$(RESET)";
endif
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	@LOCAL_VERSION=`protoc-gen-go --version | cut -d ' ' -f 2`; \
	echo "$(CYAN)Installed protoc-gen-go version $$LOCAL_VERSION$(RESET)";
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	@LOCAL_VERSION=`protoc-gen-go-grpc --version | cut -d ' ' -f 2`; \
	echo "$(CYAN)Installed protoc-gen-go-grpc version $$LOCAL_VERSION$(RESET)";

GRPC_PKG_DIR:=./pkg/api/grpc
GRPC_API_PROTO_PATH=./api/grpc
GOPATH_BIN := $(shell go env GOPATH)\bin
PROTOC_GEN_GO = $(GOPATH_BIN)\protoc-gen-go
PROTOC_GEN_GO_GRPC = $(GOPATH_BIN)\protoc-gen-go-grpc

.PHONY: grpc-gen
grpc-gen:
	@mkdir -p ${GRPC_PKG_DIR}
	./third_party/protoc/bin/protoc -I=${GRPC_API_PROTO_PATH} \
			--plugin=$(PROTOC_GEN_GO) \
			--plugin=$(PROTOC_GEN_GO_GRPC) \
			--go_out=${GRPC_PKG_DIR} \
			--go_opt=paths=source_relative \
			--go-grpc_out=${GRPC_PKG_DIR} \
			--go-grpc_opt=paths=source_relative \
	${GRPC_API_PROTO_PATH}/${APP_NAME}.proto