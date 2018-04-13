default: vet test

test:
	go test ./...

vet:
	go vet ./...

bench:
	go test ./... -run=NONE -bench=. -benchmem

# go get -u github.com/davelondon/rebecca/cmd/becca
README.md: README.md.tpl
	becca -package github.com/bsm/bfs
