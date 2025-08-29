install:
	cd bench && go install ./cmd/iavl-bench-all
	cd iavl-v1 && go install .
	cd iavl-v2/alpha5 && go install .
	cd iavl-v2/alpha6 && go install .
	cd memiavl && go install .
