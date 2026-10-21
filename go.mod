module github.com/bzed/lokihouse

go 1.27.1

toolchain go1.27.3

replace github.com/Azure/azure-sdk-for-go => github.com/Azure/azure-sdk-for-go v68.0.0+incompatible

replace github.com/Azure/azure-sdk-for-go/sdk/storage/azblob => github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.8.0

replace github.com/Azure/azure-storage-blob-go => github.com/MasslessParticle/azure-storage-blob-go v0.14.1-0.20240322194317-344980fda573

replace github.com/gocql/gocql => github.com/grafana/gocql v0.0.0-20200605141915-ba5dc39ece85

replace github.com/hashicorp/memberlist => github.com/grafana/memberlist v0.3.1-0.20260921102813-8c522db348af

replace github.com/grafana/regexp => github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853

replace github.com/influxdata/go-syslog/v3 => github.com/leodido/go-syslog/v4 v4.6.0

replace github.com/thanos-io/objstore => github.com/grafana/objstore v0.0.0-20260721130030-becd33cc63ff

replace github.com/twmb/franz-go => github.com/twmb/franz-go v1.21.7

replace github.com/twmb/franz-go/pkg/kadm => github.com/twmb/franz-go/pkg/kadm v1.18.0

replace github.com/twmb/franz-go/pkg/kmsg => github.com/twmb/franz-go/pkg/kmsg v1.13.1

replace github.com/grafana/loki/pkg/push => github.com/grafana/loki/pkg/push v0.0.0-20261012182635-bc1962860a16

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.49.0
	github.com/axiomhq/hyperloglog v0.3.0
	github.com/buger/jsonparser v1.6.1
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/coreos/go-systemd/v22 v22.7.0
	github.com/drone/envsubst v1.0.3
	github.com/dustin/go-humanize v1.1.0
	github.com/felixge/httpsnoop v1.1.0
	github.com/go-kit/log v0.2.1
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674
	github.com/grafana/dskit v0.0.0-20260928073014-b6a53f6bee29
	github.com/grafana/loki/v3 v3.0.0-20261012182635-bc1962860a16
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2
	github.com/prometheus/client_golang v1.25.0
	github.com/prometheus/common v0.72.0
	github.com/prometheus/otlptranslator v1.0.0
	github.com/prometheus/prometheus v0.314.0-rc.0.0.20260923171314-5f325dd18c88
	github.com/stretchr/testify v1.12.1
	go.opentelemetry.io/collector/pdata v1.68.0
	go.uber.org/goleak v1.3.0
	go.yaml.in/yaml/v4 v4.0.0-rc.6
	google.golang.org/grpc v1.84.0
)

require (
	cel.dev/expr v0.25.3 // indirect
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth v0.24.1-0.20261001053825-dbc26066f70a // indirect
	cloud.google.com/go/auth/oauth2adapt v0.3.0 // indirect
	cloud.google.com/go/compute/metadata v0.10.0 // indirect
	cloud.google.com/go/iam v1.14.0 // indirect
	cloud.google.com/go/monitoring v1.31.0 // indirect
	cloud.google.com/go/storage v1.69.0 // indirect
	dario.cat/mergo v1.0.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.23.3 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.14.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.13.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.8.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.10.1 // indirect
	github.com/ClickHouse/ch-go v0.74.0 // indirect
	github.com/DataDog/sketches-go v1.4.8 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.62.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.62.0 // indirect
	github.com/IBM/go-sdk-core/v5 v5.25.0 // indirect
	github.com/IBM/ibm-cos-sdk-go v1.15.1 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.5.0 // indirect
	github.com/Masterminds/sprig/v3 v3.3.0 // indirect
	github.com/Workiva/go-datastructures v1.1.8 // indirect
	github.com/alecthomas/units v0.0.0-20240927000941-0f3dac36c52b // indirect
	github.com/alibabacloud-go/debug v1.0.1 // indirect
	github.com/alibabacloud-go/tea v1.5.3 // indirect
	github.com/aliyun/aliyun-oss-go-sdk v3.0.2+incompatible // indirect
	github.com/aliyun/credentials-go v1.4.13 // indirect
	github.com/andybalholm/brotli v1.2.6 // indirect
	github.com/aws/aws-sdk-go-v2 v1.47.2 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.21 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.33.8 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.20.8 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.114.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.10.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.38.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.43.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.51.3 // indirect
	github.com/aws/smithy-go v1.28.4 // indirect
	github.com/baidubce/bce-sdk-go v0.9.276 // indirect
	github.com/bboreham/go-loser v0.0.0-20230920113527-fcc2c21820a3 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.26.0 // indirect
	github.com/bits-and-blooms/bloom/v3 v3.7.1 // indirect
	github.com/c2h5oh/datasize v0.0.0-20231215233829-aa82cc1e6500 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cncf/xds/go v0.0.0-20260202195803-dba9d589def2 // indirect
	github.com/coder/quartz v0.3.1 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/cristalhq/hedgedhttp v0.9.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/dgryski/go-metro v0.0.0-20261008173524-5059e0b1da03 // indirect
	github.com/dolthub/maphash v0.1.0 // indirect
	github.com/dolthub/swiss v0.2.1 // indirect
	github.com/ebitengine/purego v0.11.1 // indirect
	github.com/edsrzf/mmap-go v1.2.1-0.20241212181136-fad1cd13edbd // indirect
	github.com/efficientgo/core v1.0.0-rc.3 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.39.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.3 // indirect
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/gabriel-vasile/mimetype v1.4.15 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-jose/go-jose/v4 v4.1.5 // indirect
	github.com/go-logfmt/logfmt v0.6.1 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-openapi/analysis v1.0.1 // indirect
	github.com/go-openapi/errors v0.22.9 // indirect
	github.com/go-openapi/jsonpointer v1.0.2 // indirect
	github.com/go-openapi/jsonreference v1.0.3 // indirect
	github.com/go-openapi/loads v0.25.3 // indirect
	github.com/go-openapi/spec v1.0.1 // indirect
	github.com/go-openapi/strfmt v0.27.3 // indirect
	github.com/go-openapi/swag v0.29.2 // indirect
	github.com/go-openapi/swag/cmdutils v0.29.2 // indirect
	github.com/go-openapi/swag/conv v0.29.2 // indirect
	github.com/go-openapi/swag/fileutils v0.29.2 // indirect
	github.com/go-openapi/swag/jsonutils v0.29.2 // indirect
	github.com/go-openapi/swag/loading v0.29.2 // indirect
	github.com/go-openapi/swag/mangling v0.29.2 // indirect
	github.com/go-openapi/swag/netutils v0.29.2 // indirect
	github.com/go-openapi/swag/pools v0.29.2 // indirect
	github.com/go-openapi/swag/stringutils v0.29.2 // indirect
	github.com/go-openapi/swag/typeutils v0.29.2 // indirect
	github.com/go-openapi/swag/yamlutils v0.29.2 // indirect
	github.com/go-openapi/validate v1.0.0 // indirect
	github.com/go-playground/locales v0.14.2 // indirect
	github.com/go-playground/universal-translator v0.18.2 // indirect
	github.com/go-playground/validator/v10 v10.30.5 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/gofrs/flock v0.13.1 // indirect
	github.com/gogo/googleapis v1.4.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/gogo/status v1.1.1 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/s2a-go v0.1.11 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.23 // indirect
	github.com/googleapis/gax-go/v2 v2.26.2 // indirect
	github.com/grafana/gomemcache v0.0.0-20260728143316-9448343bd654 // indirect
	github.com/grafana/jsonparser v0.0.0-20250909130937-5f438463be34 // indirect
	github.com/grafana/loki/pkg/push v0.0.0-20250630054201-94c0ba7b0952 // indirect
	github.com/grafana/otel-profiling-go v0.7.0 // indirect
	github.com/grafana/pyroscope-go/godeltaprof v0.1.12 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.3.4 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.31.0 // indirect
	github.com/hashicorp/consul/api v1.34.5 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-metrics v0.7.0 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.8 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-sockaddr v1.0.7 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/hashicorp/memberlist v0.7.0 // indirect
	github.com/hashicorp/serf v0.11.0 // indirect
	github.com/huandu/xstrings v1.6.2 // indirect
	github.com/influxdata/tdigest v0.0.2-0.20210216194612-fc98d27c9e8b // indirect
	github.com/jaegertracing/jaeger-idl v0.14.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kamstrup/intmap v0.5.2 // indirect
	github.com/klauspost/compress v1.20.1 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/klauspost/pgzip v1.2.7 // indirect
	github.com/knadh/koanf/maps v0.1.3 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.1 // indirect
	github.com/knadh/koanf/v2 v2.3.8 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/leodido/go-urn v1.5.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20260802145828-341c2f0c90b5 // indirect
	github.com/mattn/go-colorable v0.1.16 // indirect
	github.com/mattn/go-ieproxy v0.0.12 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/mdlayher/socket v0.7.0 // indirect
	github.com/mdlayher/vsock v1.3.0 // indirect
	github.com/miekg/dns v1.1.73 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.3.0 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/ncruces/go-strftime v1.1.0 // indirect
	github.com/ncw/swift v1.0.53 // indirect
	github.com/ncw/swift/v2 v2.0.5 // indirect
	github.com/oklog/run v1.2.0 // indirect
	github.com/oklog/ulid/v2 v2.1.2 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics v0.162.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/deltatocumulativeprocessor v0.162.0 // indirect
	github.com/opentracing-contrib/go-grpc v0.1.4 // indirect
	github.com/opentracing-contrib/go-stdlib v1.1.1 // indirect
	github.com/opentracing/opentracing-go v1.2.1-0.20220228012449-10b1cf09e00b // indirect
	github.com/parquet-go/bitpack v1.1.0 // indirect
	github.com/parquet-go/jsonlite v1.5.5 // indirect
	github.com/parquet-go/parquet-go v0.32.0 // indirect
	github.com/paulmach/orb v0.13.0 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.32 // indirect
	github.com/pires/go-proxyproto v0.15.0 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20260702190614-8ae5a48058df // indirect
	github.com/power-devops/perfstat v0.0.0-20260916203055-22a1a467d9f0 // indirect
	github.com/prometheus/alertmanager v0.34.1 // indirect
	github.com/prometheus/client_golang/exp v0.0.0-20261008142031-a4329cfb379b // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/exporter-toolkit v0.20.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/prometheus/sigv4 v0.5.0 // indirect
	github.com/puzpuzpuz/xsync/v4 v4.5.0 // indirect
	github.com/redis/go-redis/v9 v9.23.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/richardartoul/molecule v1.0.0 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/sercand/kuberesolver/v6 v6.0.1 // indirect
	github.com/shirou/gopsutil/v4 v4.26.9 // indirect
	github.com/shopspring/decimal v1.5.0 // indirect
	github.com/sony/gobreaker/v2 v2.4.0 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/spiffe/go-spiffe/v2 v2.9.0 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/thanos-io/objstore v0.0.0-20261009124658-72d3b9ff63af // indirect
	github.com/tinylib/msgp v1.6.5 // indirect
	github.com/tjhop/slog-gokit v0.2.0 // indirect
	github.com/tklauser/go-sysconf v0.4.0 // indirect
	github.com/tklauser/numcpus v0.12.0 // indirect
	github.com/twmb/franz-go v1.21.7 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.18.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	github.com/twmb/franz-go/plugin/kotel v1.7.1 // indirect
	github.com/twmb/franz-go/plugin/kprom v1.2.1 // indirect
	github.com/twpayne/go-geom v1.7.0 // indirect
	github.com/uber/jaeger-client-go v2.30.0+incompatible // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.etcd.io/bbolt v1.5.0 // indirect
	go.etcd.io/etcd/api/v3 v3.7.2 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.7.2 // indirect
	go.etcd.io/etcd/client/v3 v3.7.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector/component v1.68.0 // indirect
	go.opentelemetry.io/collector/confmap v1.68.0 // indirect
	go.opentelemetry.io/collector/consumer v1.68.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.162.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.68.0 // indirect
	go.opentelemetry.io/collector/internal/componentalias v0.162.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.162.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.162.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.68.0 // indirect
	go.opentelemetry.io/collector/processor v1.68.0 // indirect
	go.opentelemetry.io/collector/processor/xprocessor v0.162.0 // indirect
	go.opentelemetry.io/contrib/bridges/prometheus v0.72.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.47.0 // indirect
	go.opentelemetry.io/contrib/exporters/autoexport v0.72.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.72.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace v0.72.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.72.0 // indirect
	go.opentelemetry.io/contrib/propagators/jaeger v1.47.0 // indirect
	go.opentelemetry.io/contrib/samplers/jaegerremote v0.38.0 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.23.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.23.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.69.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.23.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.47.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk/log v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.opentelemetry.io/proto/otlp v1.11.1 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	go4.org/netipx v0.0.0-20260823151212-3075585bcbeb // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/exp v0.0.0-20260908205506-85c1c2202aba // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/oauth2 v0.37.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/time v0.16.0 // indirect
	google.golang.org/api v0.301.0 // indirect
	google.golang.org/genproto v0.0.0-20261005182115-fad411399dd8 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20261005182115-fad411399dd8 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261005182115-fad411399dd8 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	gopkg.in/ini.v1 v1.67.3 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/apimachinery v0.37.1 // indirect
	k8s.io/client-go v0.37.1 // indirect
	k8s.io/klog/v2 v2.140.0 // indirect
	k8s.io/utils v0.0.0-20260707023825-cf1189d6abe3 // indirect
	modernc.org/libc v1.77.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
	modernc.org/sqlite v1.60.1 // indirect
	sigs.k8s.io/structured-merge-diff/v7 v7.0.0 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
	zombiezen.com/go/sqlite v1.4.2 // indirect
)
