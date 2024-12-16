package logic

import (
	"context"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	cGrpc "github.com/Bazhenator/cleaner/pkg/api/grpc"
	"github.com/Bazhenator/requester/configs"
	"github.com/Bazhenator/requester/internal/logic/dto"
	"github.com/Bazhenator/requester/pkg/connections/buffer"
	"github.com/Bazhenator/requester/pkg/connections/cleaner"
	"github.com/Bazhenator/requester/pkg/connections/generator"
	"github.com/Bazhenator/tools/src/logger"
)

type Dispatcher struct {
	c *configs.Config
	l *logger.Logger

	TotalTime  time.Duration
	buffer     buffer.Connection
	generator  generator.Connection
	cleaner    cleaner.Connection
	TeamsStats []*dto.TeamStat
	ReqsStats  []*dto.RequestStat
}

func NewDispatcher(c *configs.Config, l *logger.Logger,
	bc buffer.Connection, gc generator.Connection, cc cleaner.Connection) *Dispatcher {
	reqsStats := make([]*dto.RequestStat, 0)

	return &Dispatcher{
		c: c,
		l: l,

		buffer:     bc,
		generator:  gc,
		cleaner:    cc,
		TeamsStats: nil,
		ReqsStats:  reqsStats,
	}
}

func (d *Dispatcher) LaunchDispatcher(ctx context.Context) error {
	d.l.Debug("LaunchDispatcher starting...")
	timer := time.Now()

	resp, _ := d.cleaner.Client.GetAvailableTeams(ctx, &emptypb.Empty{})
	teamsStats := make([]*dto.TeamStat, 0, len(resp.TeamsIds))
	d.TeamsStats = teamsStats

	go func() {
		d.l.Debug("Starting request generator...")

		_, err := d.generator.Client.StartGenerator(ctx, &emptypb.Empty{})
		if err != nil {
			d.l.ErrorCtx(ctx, "Failed to start generator", logger.NewErrorField(err))
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		d.l.Debug("Starting dispatcher loop...")

		for {
			select {
			case <-ctx.Done():
				d.l.InfoCtx(ctx, "Dispatcher loop stopped due to context cancellation")
				d.TotalTime = time.Since(timer)

				return

			default:
				resp, err := d.cleaner.Client.GetAvailableTeams(ctx, &emptypb.Empty{})
				if err != nil {
					d.l.ErrorCtx(ctx, "Failed to get available teams", logger.NewErrorField(err))

					time.Sleep(time.Second)
					continue
				}

				if len(resp.TeamsIds) == 0 {
					d.l.Debug("No available teams, waiting...")

					time.Sleep(time.Second)
					continue
				}

				reqResp, err := d.buffer.Client.PopTop(ctx, &emptypb.Empty{})
				if err != nil {
					d.l.ErrorCtx(ctx, "Failed to pop request from buffer", logger.NewErrorField(err))

					time.Sleep(time.Second)
					continue
				}

				if reqResp == nil || reqResp.GetReq() == nil {
					d.l.InfoCtx(ctx, "Buffer is empty, stopping dispatcher loop")

					return
				}

				for _, teamID := range resp.TeamsIds {
					req, err := d.cleaner.Client.ProceedCleaning(ctx, &cGrpc.ProceedCleaningIn{
						Req: &cGrpc.Request{
							Id:           reqResp.GetReq().GetId(),
							ClientId:     reqResp.GetReq().GetClientId(),
							Priority:     reqResp.GetReq().GetPriority(),
							CleaningType: reqResp.GetReq().GetCleaningType(),
						},
						TeamId: teamID,
					})

					if err != nil {
						d.l.ErrorCtx(ctx, "Failed to proceed cleaning", logger.NewErrorField(err))
					} else {
						d.l.InfoCtx(ctx, "Request successfully assigned to team", logger.NewField("team_id", teamID))

						stat := req.GetReq()
						d.ReqsStats = append(d.ReqsStats, &dto.RequestStat{
							Id:            stat.GetId(),
							GeneratorId:   reqResp.GetReq().GetGeneratorId(),
							TeamId:        stat.GetTeamId(),
							Priority:      stat.GetPriority(),
							TimeInCleaner: stat.GetTimeInCleaner(),
							TimeInBuffer:  reqResp.GetReq().GetTimeInBuffer(),
						})

						break
					}
				}
			}
		}
	}()

	wg.Wait()
	d.l.Info("LaunchDispatcher finished")
	return nil
}

func (d *Dispatcher) CreateStatisticsReport(ctx context.Context) error {
	reportFile, err := os.Create("statistics.txt")
	if err != nil {
		d.l.Error("failed to create report file", logger.NewErrorField(err))
		return err
	}
	defer reportFile.Close()

	resp, err := d.cleaner.Client.GetTeamsStats(ctx, &emptypb.Empty{})
	for _, i := range resp.Teams {
		d.TeamsStats = append(d.TeamsStats, &dto.TeamStat{
			Id:                 i.GetId(),
			Speed:              i.GetSpeed(),
			ProccessedRequests: i.GetProcessedRequests(),
			TotalBusyTime:      i.GetTotalBusyTime(),
		})
	}

	//TODO: add creating report logic

	return nil
}
