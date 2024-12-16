package delivery

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/Bazhenator/requester/configs"
	requester "github.com/Bazhenator/requester/pkg/api/grpc"
	"github.com/Bazhenator/tools/src/logger"
)

type DispatcherServer struct {
	requester.UnimplementedRequesterServiceServer

	c *configs.Config
	l *logger.Logger

	logic DispatcherService
}

func NewDispatcherServer(c *configs.Config, l *logger.Logger, logic DispatcherService) *DispatcherServer {
	return &DispatcherServer{
		c: c,
		l: l,

		logic: logic,
	}
}

func (s *DispatcherServer) LaunchService(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.l.DebugCtx(ctx, "Cleaning service started...")

	err := s.logic.LaunchDispatcher(ctx)
	if err != nil {
		s.l.ErrorCtx(ctx, "Error while launching dispatcher:", logger.NewErrorField(err))
		return &emptypb.Empty{}, err
	}

	return &emptypb.Empty{}, nil
}

func (s *DispatcherServer) CreateStatisticsReport(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.l.DebugCtx(ctx, "Generating statistics report...")

	err := s.logic.CreateStatisticsReport(ctx)
	if err != nil {
		s.l.ErrorCtx(ctx, "Error while creating statistics report:", logger.NewErrorField(err))
		return &emptypb.Empty{}, err
	}

	return &emptypb.Empty{}, nil
}
