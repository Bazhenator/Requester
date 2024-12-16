module github.com/Bazhenator/requester

go 1.23.4

require (
	github.com/Bazhenator/buffer v0.0.0-20241215220935-e8bd9c528ba1
	github.com/Bazhenator/cleaner v1.0.0
	github.com/Bazhenator/generator v0.0.0-20241215220840-88bf27bcf3d0
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.57.0
	go.uber.org/zap v1.18.1
	google.golang.org/grpc v1.69.0
)

require google.golang.org/genproto/googleapis/rpc v0.0.0-20241209162323-e6fa225c2576 // indirect

require (
	github.com/Bazhenator/tools v0.0.1
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	go.opentelemetry.io/otel v1.32.0
	go.opentelemetry.io/otel/metric v1.32.0 // indirect
	go.opentelemetry.io/otel/trace v1.32.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0
	golang.org/x/net v0.32.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/protobuf v1.35.2
)
