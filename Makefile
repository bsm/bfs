default: vet test

vet/%: %
	@cd $< && go vet ./...

test/%: %
	@cd $< && go test ./...

bench/%: %
	@cd $< && go test ./... -run=NONE -bench=. -benchmem

bump-deps/%: %
	@cd $< && go get -u ./... && go mod tidy

vet: vet/. $(patsubst %/go.mod,vet/%,$(wildcard */go.mod))
test: test/. $(patsubst %/go.mod,test/%,$(wildcard */go.mod))
bench: bench/. $(patsubst %/go.mod,bench/%,$(wildcard */go.mod))
bump-deps: bump-deps/. $(patsubst %/go.mod,bump-deps/%,$(wildcard */go.mod))

# go get -u github.com/davelondon/rebecca/cmd/becca
README.md: README.md.tpl
	becca -package github.com/bsm/bfs
