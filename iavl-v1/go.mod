module github.com/cosmos/iavl-bench/iavl-v1

go 1.23.0

toolchain go1.24.2

require (
	github.com/cosmos/iavl v1.3.5
	github.com/cosmos/iavl-bench/bench v0.0.4
	github.com/syndtr/goleveldb v1.0.1-0.20220721030215-126854af5e6d
)

require (
	cosmossdk.io/api v0.9.2 // indirect
	cosmossdk.io/core v0.12.1-0.20240725072823-6a2d039e1212 // indirect
	github.com/cosmos/cosmos-proto v1.0.0-beta.5 // indirect
	github.com/cosmos/gogoproto v1.7.0 // indirect
	github.com/cosmos/ics23/go v0.10.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/ebitengine/purego v0.8.4 // indirect
	github.com/emicklei/dot v1.6.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/shirou/gopsutil/v4 v4.25.7 // indirect
	github.com/spf13/cobra v1.7.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tidwall/btree v1.8.1 // indirect
	github.com/tklauser/go-sysconf v0.3.15 // indirect
	github.com/tklauser/numcpus v0.10.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/net v0.39.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/text v0.24.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250422160041-2d3770c4ea7f // indirect
	google.golang.org/grpc v1.72.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)

replace github.com/cosmos/iavl-bench/bench => ../bench
