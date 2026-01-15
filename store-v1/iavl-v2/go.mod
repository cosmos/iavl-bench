module bench-sdk-store-iavl-v2

go 1.25.5

require (
	cosmossdk.io/log v1.6.1
	github.com/cosmos/cosmos-db v1.1.3
	github.com/cosmos/iavl-bench/bench v0.0.4
	github.com/cosmos/iavl-bench/store-v1 v0.0.0
)

// pinned store version with:
// go get cosmossdk.io/store@1a022151cbf285e04b341732213ad1facd46ceee
// from https://github.com/cosmos/cosmos-sdk/pull/24960
require cosmossdk.io/store v1.10.0-rc.2.0.20250829194042-1a022151cbf2

// this won't work with the latest iavl
replace github.com/cosmos/iavl v1.3.5 => github.com/cosmos/iavl v1.2.6

require (
	cosmossdk.io/api v0.9.2 // indirect
	cosmossdk.io/errors v1.0.2 // indirect
	cosmossdk.io/math v1.5.3 // indirect
	github.com/DataDog/zstd v1.5.7 // indirect
	github.com/aybabtme/uniplot v0.0.0-20151203143629-039c559e5e7e // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bvinc/go-sqlite-lite v0.6.1 // indirect
	github.com/bytedance/sonic v1.14.2 // indirect
	github.com/bytedance/sonic/loader v0.4.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/cockroachdb/errors v1.12.0 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240816210425-c5d0cb0b6fc0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20241215232642-bb51bb14a506 // indirect
	github.com/cockroachdb/pebble v1.1.5 // indirect
	github.com/cockroachdb/redact v1.1.6 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20250429170803-42689b6311bb // indirect
	github.com/cometbft/cometbft/api v1.1.0-rc1 // indirect
	github.com/cometbft/cometbft/v2 v2.0.0-rc1 // indirect
	github.com/cosmos/cosmos-proto v1.0.0-beta.5 // indirect
	github.com/cosmos/gogoproto v1.7.2 // indirect
	github.com/cosmos/iavl v1.3.5 // indirect
	github.com/cosmos/iavl/v2 v2.0.0-alpha.5.0.20250829183505-8e11ef38d394 // indirect
	github.com/cosmos/ics23/go v0.11.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/ebitengine/purego v0.9.1 // indirect
	github.com/emicklei/dot v1.9.0 // indirect
	github.com/getsentry/sentry-go v0.33.0 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-metrics v0.5.4 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/kocubinski/costor-api v1.1.2 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/linxGnu/grocksdb v1.10.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20251013123823-9fd1530e3ec3 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/onsi/gomega v1.34.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_golang v1.23.2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/procfs v0.19.2 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	github.com/shirou/gopsutil/v4 v4.25.11 // indirect
	github.com/spf13/cast v1.8.0 // indirect
	github.com/spf13/cobra v1.10.2 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20220721030215-126854af5e6d // indirect
	github.com/tidwall/btree v1.8.1 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	golang.org/x/arch v0.21.0 // indirect
	golang.org/x/crypto v0.46.0 // indirect
	golang.org/x/exp v0.0.0-20250819193227-8b4c13bb791b // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.78.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/cosmos/iavl-bench/store-v1 => ..

replace github.com/cosmos/iavl-bench/bench => ../../bench
