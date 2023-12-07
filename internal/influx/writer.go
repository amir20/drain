package ga

import (
	"context"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/amir20/drain/internal"
	"go.uber.org/zap"
)

func WriteEvent(row internal.Event, logger *zap.SugaredLogger) error {
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host: "http://localhost:8086",
	})

	if err != nil {
		panic(err)
	}
	// Close client at the end and escalate error if present
	defer func(client *influxdb3.Client) {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}(client)

	// Create point using full params constructor
	p := influxdb3.NewPoint("stat",
		map[string]string{"unit": "temperature"},
		map[string]interface{}{"avg": 24.5, "max": 45.0},
		time.Now())
	// write point synchronously
	err = client.WritePoints(context.Background(), p)
	if err != nil {
		panic(err)
	}
	// Create point using fluent style
	p = influxdb3.NewPointWithMeasurement("stat").
		SetTag("unit", "temperature").
		SetField("avg", 23.2).
		SetField("max", 45.0).
		SetTimestamp(time.Now())
	// write point synchronously
	err = client.WritePoints(context.Background(), p)
	if err != nil {
		panic(err)
	}
}
