package writer

import (
	"context"
	"fmt"

	"github.com/amir20/drain/internal"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"go.uber.org/zap"
)

type InfluxWriter struct {
	channel chan internal.Event
	logger  *zap.SugaredLogger
	client  influxdb2.Client
	writer  api.WriteAPIBlocking
}

func NewInfluxWriter(logger *zap.SugaredLogger) *InfluxWriter {
	client := influxdb2.NewClient("http://localhost:8086", "secret")
	writeAPI := client.WriteAPIBlocking("dozzle", "drain")
	return &InfluxWriter{
		channel: make(chan internal.Event),
		logger:  logger,
		client:  client,
		writer:  writeAPI,
	}
}

func (w *InfluxWriter) Start() chan<- internal.Event {
	go func() {
		for row := range w.channel {
			if err := w.WriteEvent(row, w.logger); err != nil {
				w.logger.Errorf("failed to write event: %v", err)
			}
		}
	}()
	return w.channel
}

func (w *InfluxWriter) WriteEvent(row internal.Event, logger *zap.SugaredLogger) error {
	if row.Name == "" {
		return fmt.Errorf("name is required")
	}
	p := influxdb2.NewPointWithMeasurement(row.Name).
		AddTag("version", row.Version).
		AddField("browser", row.Browser).
		AddField("auth_provider", row.AuthProvider).
		AddField("has_documentation", row.HasDocumentation).
		AddField("filter_length", row.FilterLength).
		AddField("clients", row.Clients).
		AddField("has_custom_address", row.HasCustomAddress).
		AddField("has_custom_base", row.HasCustomBase).
		AddField("has_hostname", row.HasHostname).
		AddField("running_containers", row.RunningContainers).
		AddField("has_actions", row.HasActions).
		SetTime(row.CreatedAt)
	err := w.writer.WritePoint(context.Background(), p)
	if err != nil {
		return err
	}
	return nil
}
