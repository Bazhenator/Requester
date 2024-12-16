package configs

import (
	"errors"
	"os"

	"go.uber.org/multierr"

	"github.com/Bazhenator/tools/src/logger"
	grpcListener "github.com/Bazhenator/tools/src/server/grpc/listener"
)

const (
	envBufferService    = "BUFFER_SERVICE"
	envGeneratorService = "GENERATOR_SERVICE"
	envCleanerService   = "CLEANER_SERVICE"
)

// Config is a main configuration struct for application
type Config struct {
	Environment   string
	Grpc          *grpcListener.GrpcConfig
	BufferHost    string
	GeneratorHost string
	CleanerHost   string
	LoggerConfig  *logger.LoggerConfig
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

	bufferStr, ok := os.LookupEnv(envBufferService)
	if !ok {
		multierr.AppendInto(&errorBuilder, errors.New("BUFFER_SERVICE is not defined"))
	}

	generatorStr, ok := os.LookupEnv(envGeneratorService)
	if !ok {
		multierr.AppendInto(&errorBuilder, errors.New("GENERATOR_SERVICE is not defined"))
	}

	cleanerStr, ok := os.LookupEnv(envCleanerService)
	if !ok {
		multierr.AppendInto(&errorBuilder, errors.New("CLEANER_SERVICE is not defined"))
	}

	if errorBuilder != nil {
		return nil, errorBuilder
	}

	glCfg := &Config{
		Grpc:          grpcConfig,
		BufferHost:    bufferStr,
		GeneratorHost: generatorStr,
		CleanerHost:   cleanerStr,
		LoggerConfig:  loggerConfig,
	}

	return glCfg, nil
}
