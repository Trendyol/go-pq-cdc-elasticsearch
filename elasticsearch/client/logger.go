package client

import (
	"net/http"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
)

type LoggerAdapter struct{}

func (l *LoggerAdapter) LogRoundTrip(_ *http.Request, _ *http.Response, err error, _ time.Time, _ time.Duration) error {
	if err == nil {
		return nil
	}

	logger.Error("elasticsearch error", "error", err)
	return nil
}

func (l *LoggerAdapter) RequestBodyEnabled() bool {
	return true
}

func (l *LoggerAdapter) ResponseBodyEnabled() bool {
	return true
}
