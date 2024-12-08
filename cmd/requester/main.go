package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata"

	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcZap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpcRecovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpcCtxTags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/Bazhenator/requester/configs"
	"github.com/Bazhenator/tools/src/logger"
	middlewareLogging "github.com/Bazhenator/tools/src/middleware/log"
	grpcListener "github.com/Bazhenator/tools/src/server/grpc/listener"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("service stopped with error: %v", err)
	}
}

func run() error {
	// Initializing requester's config
	config, err := configs.NewConfig()
	if err != nil {
		return err
	}

	// Initializing requester's logger
	l, err := logger.NewLogger(config.LoggerConfig)
	if err != nil {
		return err
	}
	defer func() {
		if err := l.Sync(); err != nil {
			l.Error(err.Error())
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initializing requester's grpc server 
	grpcServer := newGrpcServer(config, l.Logger)
	defer grpcServer.GracefulStop()

	var c = make(chan os.Signal, 1)
	defer signal.Stop(c)

	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-c
		l.InfoCtx(ctx, "Got signal", logger.NewField("signal", s))
		switch s {
		case syscall.SIGTERM, syscall.SIGINT:
			l.InfoCtx(ctx, "graceful stop grpc server")
			grpcServer.GracefulStop()
		}
	}()

	reflection.Register(grpcServer)

	// TODO: add repos/logic/delivery configs

	lis, deferGrpc, err := grpcListener.NewGrpcListener(config.Grpc)
	if err != nil {
		return err
	}
	defer deferGrpc(lis)

	if err = grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	return nil
}

func newGrpcServer(c *configs.Config, l *zap.Logger) *grpc.Server {
	s := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{Timeout: time.Second * time.Duration(c.Grpc.Timeout)}),
		grpc.MaxRecvMsgSize(c.Grpc.MaxReceiveSize),
		grpc.MaxSendMsgSize(c.Grpc.MaxSendSize),
		grpcMiddleware.WithUnaryServerChain(
			grpcRecovery.UnaryServerInterceptor(),
			grpcCtxTags.UnaryServerInterceptor(),
			otelgrpc.UnaryServerInterceptor(),
			grpcZap.UnaryServerInterceptor(l, grpcZap.WithMessageProducer(middlewareLogging.LogsProducer)),
		),
		grpcMiddleware.WithStreamServerChain(
			grpcRecovery.StreamServerInterceptor(),
			grpcCtxTags.StreamServerInterceptor(),
			otelgrpc.StreamServerInterceptor(), 		
			grpcZap.StreamServerInterceptor(l, grpcZap.WithMessageProducer(middlewareLogging.LogsProducer)),
		),
	)
	return s
}
