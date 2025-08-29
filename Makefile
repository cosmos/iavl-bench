install:
	cd bench && go install ./cmd/iavl-bench-all
	cd iavl-v1 && go install .
	cd iavl-v2 && go install .
	cd memiavl && go install .
