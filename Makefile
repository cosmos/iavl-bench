install:
	cd bench && go install ./cmd/gen-changesets
	cd bench && go install ./cmd/iavl-bench-all
	cd iavl-v0 && go install .
	cd iavl-v1 && go install .
	cd iavl-v2/alpha5 && go install .
	cd iavl-v2/alpha6 && go install .
	cd memiavl && go install .
	cd store-v1/latest && go install .
	cd store-v1/iavl-v2 && go install .
	cd store-v1/memiavl && go install .

PHONY: install