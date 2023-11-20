package writer

import (
	"context"
	"fmt"

	"os"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"
)

type WriterRow struct {
	CreatedAt         time.Time
	AuthProvider      string
	RemoteIP          string
	Version           string
	RemoteHostLength  int
	HasDocumentation  bool
	FilterLength      int
	HasCustomAddress  bool
	HasCustomBase     bool
	HasHostname       bool
	RunningContainers int
	Browser           string
}

type ParquetWriter struct {
	channel chan WriterRow
	wg      *sync.WaitGroup
	logger  *zap.SugaredLogger
	maxRows int
	maxIdle time.Duration
	maxWait time.Duration
}

func NewParquetWriter(logger *zap.SugaredLogger) *ParquetWriter {
	return &ParquetWriter{
		channel: make(chan WriterRow),
		wg:      &sync.WaitGroup{},
		logger:  logger,
		maxRows: 50000,
		maxIdle: 60 * time.Second,
		maxWait: 1 * time.Hour,
	}
}

func (p *ParquetWriter) Start() chan WriterRow {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			file, err := os.Create(fmt.Sprintf("data/data-%d.temp", time.Now().Unix()))
			if err != nil {
				p.logger.Fatalf("failed to create file: %w", err)
			}
			writer := parquet.NewGenericWriter[WriterRow](file, parquet.Compression(&parquet.Zstd))
			i := 0
			closed := false

			cxt, cancel := context.WithDeadline(context.Background(), time.Now().Add(p.maxIdle))
			defer cancel()
		loop:
			for {
				idleContext, idleCancel := context.WithDeadline(cxt, time.Now().Add(p.maxIdle))
				defer idleCancel()
				select {
				case <-idleContext.Done():
					if i > 0 {
						break loop
					}
					continue
				case row, ok := <-p.channel:
					if ok {
						i++
						writer.Write([]WriterRow{row})
					} else {
						closed = true
						break loop
					}
				}

				if i > p.maxRows {
					break
				}
			}

			if i > 0 {
				p.logger.Infof("writing %d rows", i)
				writer.Close()
				file.Close()
				os.Rename(file.Name(), fmt.Sprintf("data/data-%s.parquet", time.Now().Format(time.RFC3339)))
			} else {
				p.logger.Info("removing empty file")
				file.Close()
				os.Remove(file.Name())
			}

			if closed {
				break
			}
		}
	}()

	return p.channel
}

func (p *ParquetWriter) Stop() {
	close(p.channel)
	p.wg.Wait()
}
