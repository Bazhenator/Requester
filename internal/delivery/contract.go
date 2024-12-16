package delivery

import "context"

type DispatcherService interface {
	LaunchDispatcher(context.Context) error
	CreateStatisticsReport(context.Context) error
}