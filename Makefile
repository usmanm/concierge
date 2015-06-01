.PHONY: build
build:
	go build
test:
	go test -check.v
installdeps:
	cat packages.txt | xargs go get
