package logic

import (
	"context"
	"fmt"
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
	"github.com/jung-kurt/gofpdf"
)

const (
	serviceSize = 10
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

	time.Sleep(time.Second * 3)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		d.l.Debug("Starting dispatcher loop...")

		for {
			select {
			case <-ctx.Done():
				d.l.InfoCtx(ctx, "Dispatcher loop stopped due to context cancellation")
				
				return
			default:
				resp, err := d.cleaner.Client.GetAvailableTeams(ctx, &emptypb.Empty{})
				if err != nil {
					d.l.ErrorCtx(ctx, "Failed to get available teams", logger.NewErrorField(err))

					time.Sleep(time.Second * 3)
					continue
				}

				time.Sleep(time.Second)

				if len(resp.TeamsIds) == 0 {
					d.l.Debug("No available teams, waiting...")

					time.Sleep(time.Second)
					continue
				}

				reqResp, err := d.buffer.Client.PopTop(ctx, &emptypb.Empty{})
				if err != nil {
					teamsAmount := 0

					for teamsAmount != serviceSize {
						time.Sleep(time.Second * 5)
						resp, _ := d.cleaner.Client.GetAvailableTeams(ctx, &emptypb.Empty{})
						teamsAmount = len(resp.GetTeamsIds())
					}

					d.TotalTime = time.Since(timer)
					d.l.ErrorCtx(ctx, "Buffer is empty, stopping dispatcher loop", logger.NewErrorField(err))

					return
				}

				time.Sleep(time.Second)

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

				time.Sleep(time.Second)
			}
		}
	}()

	wg.Wait()
	d.l.Info("LaunchDispatcher finished")
	return nil
}

func (d *Dispatcher) CreateStatisticsReport(ctx context.Context) error {
	reportFileName := "statistics.pdf"

	pdf := gofpdf.New("P", "mm", "A4", "")
	pdf.SetFont("Arial", "", 12)
	pdf.AddPage()

	pdf.SetFont("Arial", "B", 16)
	pdf.Cell(40, 10, "Statistics Report")
	pdf.Ln(20)

	resp, err := d.cleaner.Client.GetTeamsStats(ctx, &emptypb.Empty{})
	if err != nil {
		d.l.Error("failed to fetch team statistics", logger.NewErrorField(err))
		return err
	}

	for _, team := range resp.Teams {
		d.TeamsStats = append(d.TeamsStats, &dto.TeamStat{
			Id:                 team.GetId(),
			Speed:              team.GetSpeed(),
			ProccessedRequests: team.GetProcessedRequests(),
			TotalBusyTime:      team.GetTotalBusyTime(),
		})
	}

	totalTime := d.TotalTime.Seconds()

	pdf.SetFont("Arial", "B", 12)
	pdf.Cell(40, 10, "Team Statistics")
	pdf.Ln(10)

	pdf.SetFont("Arial", "", 10)
	pdf.SetFillColor(200, 200, 200)
	pdf.CellFormat(30, 10, "ID", "1", 0, "C", true, 0, "")
	pdf.CellFormat(30, 10, "Speed", "1", 0, "C", true, 0, "")
	pdf.CellFormat(50, 10, "Processed Requests", "1", 0, "C", true, 0, "")
	pdf.CellFormat(50, 10, "Total Busy Time", "1", 0, "C", true, 0, "")
	pdf.CellFormat(30, 10, "Load (%)", "1", 1, "C", true, 0, "")

	for _, teamStat := range d.TeamsStats {
		load := (teamStat.TotalBusyTime / totalTime) * 100
		pdf.CellFormat(30, 10, fmt.Sprintf("%d", teamStat.Id), "1", 0, "C", false, 0, "")
		pdf.CellFormat(30, 10, fmt.Sprintf("%d", teamStat.Speed), "1", 0, "C", false, 0, "")
		pdf.CellFormat(50, 10, fmt.Sprintf("%d", teamStat.ProccessedRequests), "1", 0, "C", false, 0, "")
		pdf.CellFormat(50, 10, fmt.Sprintf("%.2f", teamStat.TotalBusyTime) + "sec", "1", 0, "C", false, 0, "")
		pdf.CellFormat(30, 10, fmt.Sprintf("%.2f", load), "1", 1, "C", false, 0, "")
	}

	pdf.Ln(10)
	pdf.SetFont("Arial", "B", 12)
	pdf.Cell(40, 10, "Request Statistics")
	pdf.Ln(10)

	pdf.SetFont("Arial", "", 10)
	pdf.SetFillColor(200, 200, 200)
	pdf.CellFormat(20, 10, "ID", "1", 0, "C", true, 0, "")
	pdf.CellFormat(30, 10, "Generator ID", "1", 0, "C", true, 0, "")
	pdf.CellFormat(30, 10, "Team ID", "1", 0, "C", true, 0, "")
	pdf.CellFormat(20, 10, "Priority", "1", 0, "C", true, 0, "")
	pdf.CellFormat(40, 10, "Time in Cleaner", "1", 0, "C", true, 0, "")
	pdf.CellFormat(40, 10, "Time in Buffer", "1", 1, "C", true, 0, "")

	for _, reqStat := range d.ReqsStats {
		pdf.CellFormat(20, 10, fmt.Sprintf("%d", reqStat.Id), "1", 0, "C", false, 0, "")
		pdf.CellFormat(30, 10, fmt.Sprintf("%d", reqStat.GeneratorId), "1", 0, "C", false, 0, "")
		pdf.CellFormat(30, 10, fmt.Sprintf("%d", reqStat.TeamId), "1", 0, "C", false, 0, "")
		pdf.CellFormat(20, 10, fmt.Sprintf("%d", reqStat.Priority), "1", 0, "C", false, 0, "")
		pdf.CellFormat(40, 10, fmt.Sprintf("%.2f", reqStat.TimeInCleaner) + "sec", "1", 0, "C", false, 0, "")
		pdf.CellFormat(40, 10, fmt.Sprintf("%.2f", reqStat.TimeInBuffer) + "sec", "1", 1, "C", false, 0, "")
	}

	if err := pdf.OutputFileAndClose(reportFileName); err != nil {
		d.l.Error("failed to save PDF report", logger.NewErrorField(err))
		return err
	}

	d.l.Info("PDF report generated successfully", logger.NewField("file", reportFileName))
	return nil
}
