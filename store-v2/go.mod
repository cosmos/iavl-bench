module store-v2

go 1.20

replace (
	cosmossdk.io/store/v2 => ../../../cosmos-sdk/store-v2/store
	github.com/kocubinski/iavl-bench/bench => ../bench
)

require (
	cosmossdk.io/log v1.2.1
	cosmossdk.io/store/v2 v2.0.0-20220818150845-0e16987c2161
	github.com/cosmos/cosmos-db v1.0.0
	github.com/kocubinski/costor-api v0.0.9
	github.com/kocubinski/iavl-bench/bench v0.0.0-00010101000000-000000000000
	github.com/prometheus/client_golang v1.16.0
	github.com/spf13/cobra v1.7.0
	github.com/syndtr/goleveldb v1.0.1-0.20220721030215-126854af5e6d
)

require (
	cosmossdk.io/core v0.10.0 // indirect
	cosmossdk.io/errors v1.0.0 // indirect
	github.com/DataDog/zstd v1.5.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cockroachdb/errors v1.10.0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/pebble v0.0.0-20230819001538-1798fbf5956c // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/cosmos/gogoproto v1.4.11 // indirect
	github.com/cosmos/iavl v1.0.0-beta.2 // indirect
	github.com/cosmos/ics23/go v0.10.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/emicklei/dot v1.5.0 // indirect
	github.com/getsentry/sentry-go v0.23.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/compress v1.16.5 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/linxGnu/grocksdb v1.8.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_model v0.4.0 // indirect
	github.com/prometheus/common v0.44.0 // indirect
	github.com/prometheus/procfs v0.10.1 // indirect
	github.com/rogpeppe/go-internal v1.11.0 // indirect
	github.com/rs/zerolog v1.30.0 // indirect
	github.com/spf13/cast v1.5.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/crypto v0.12.0 // indirect
	golang.org/x/exp v0.0.0-20230811145659-89c5cff77bcb // indirect
	golang.org/x/sys v0.11.0 // indirect
	golang.org/x/text v0.12.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230815205213-6bfd019c3878 // indirect
	google.golang.org/grpc v1.57.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)
