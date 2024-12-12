package connections

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	commonMsgSize = 1024 * 1024 * 1024 * 1
)

var (
	CommonCallOptions = []grpc.CallOption{
		grpc.MaxCallRecvMsgSize(commonMsgSize),
		grpc.MaxCallSendMsgSize(commonMsgSize),
	}
)

func GetCommonDialOptions() []grpc.DialOption {
	tr := otel.GetTracerProvider()
	tp := otel.GetTextMapPropagator()

	res := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(commonMsgSize),
		),
		grpc.WithStatsHandler(
			otelgrpc.NewClientHandler(
				otelgrpc.WithTracerProvider(tr),
				otelgrpc.WithPropagators(tp),
			),
		),
	}

	return res
}
