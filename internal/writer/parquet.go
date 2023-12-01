package writer

import (
	"context"
	"fmt"

	"os"
	"sync"
	"time"

	"github.com/amir20/drain/internal"
	"github.com/amir20/drain/internal/ga"
	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"
)

type ParquetWriter struct {
	channel chan internal.Event
	wg      *sync.WaitGroup
	logger  *zap.SugaredLogger
	maxRows int
	maxIdle time.Duration
	maxWait time.Duration
}

func NewParquetWriter(logger *zap.SugaredLogger) *ParquetWriter {
	return &ParquetWriter{
		channel: make(chan internal.Event),
		wg:      &sync.WaitGroup{},
		logger:  logger,
		maxRows: 50000,
		maxIdle: 5 * time.Minute,
		maxWait: 1 * time.Hour,
	}
}

func (p *ParquetWriter) Start() chan internal.Event {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			file, err := os.Create(fmt.Sprintf("data/data-%d.temp", time.Now().Unix()))
			if err != nil {
				p.logger.Fatalf("failed to create file: %w", err)
			}
			writer := parquet.NewGenericWriter[internal.Event](file, parquet.Compression(&parquet.Zstd))
			i := 0
			closed := false

			cxt, cancel := context.WithDeadline(context.Background(), time.Now().Add(p.maxWait))
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
						writer.Write([]internal.Event{row})
						if row.Name != "" {
							go ga.SendEvent(row, row.Name, p.logger)
						}
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
