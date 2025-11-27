package main

import (
	"time"
	slog "log"

	"github.com/go-kit/kit/log"
	"context"
)

func loggingMiddleware(logger log.Logger) ServiceMiddleware {
	return func(next BaseService) BaseService {
		return logmw{logger, next}
	}
}

type logmw struct {
	logger log.Logger
	BaseService
}

func (mw logmw) Execute(ctx context.Context) (result string, err error) {
	defer func(begin time.Time) {
		slog.Println("Info err=", err, "took=", time.Since(begin))
	}(time.Now())

	result, err = mw.BaseService.Execute(ctx)
	return
}