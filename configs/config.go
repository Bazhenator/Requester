package configs

import (
	"go.uber.org/multierr"

	"github.com/Bazhenator/tools/src/logger"
	grpcListener "github.com/Bazhenator/tools/src/server/grpc/listener"
)

// Config is a main configuration struct for application
type Config struct {
	Environment  string
	Grpc         *grpcListener.GrpcConfig
	LoggerConfig *logger.LoggerConfig
}

type AppVersionCheckConfig struct {
	Host string
}

// NewConfig returns application config instance
func NewConfig() (*Config, error) {
	var errorBuilder error

	grpcConfig, err := grpcListener.NewStandardGrpcConfig()
	multierr.AppendInto(&errorBuilder, err)

	loggerConfig, err := logger.NewLoggerConfig()
	multierr.AppendInto(&errorBuilder, err)

	if errorBuilder != nil {
		return nil, errorBuilder
	}

	glCfg := &Config{
		Grpc:         grpcConfig,
		LoggerConfig: loggerConfig,
	}

	return glCfg, nil
}
