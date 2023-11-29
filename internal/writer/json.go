package writer

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"

	"os"
	"sync"
	"time"

	"github.com/amir20/drain/internal"
	"github.com/amir20/drain/internal/ga"

	"go.uber.org/zap"
)

type JsonWriter struct {
	channel chan internal.Event
	wg      *sync.WaitGroup
	logger  *zap.SugaredLogger
	maxRows int
	maxIdle time.Duration
	maxWait time.Duration
}

func NewJsonWriter(logger *zap.SugaredLogger) *JsonWriter {
	return &JsonWriter{
		channel: make(chan internal.Event),
		wg:      &sync.WaitGroup{},
		logger:  logger,
		maxRows: 50000,
		maxIdle: 1 * time.Minute,
		maxWait: 1 * time.Hour,
	}
}

func (p *JsonWriter) Start() chan internal.Event {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			file, err := os.Create(fmt.Sprintf("data/data-%d.json.gz.temp", time.Now().Unix()))
			if err != nil {
				p.logger.Fatalf("failed to create file: %w", err)
			}

			bw := bufio.NewWriter(file)
			gzipWriter := gzip.NewWriter(bw)
			writer := json.NewEncoder(gzipWriter)
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
						writer.Encode(row)
						go ga.SendEvent(row, "eventStream", p.logger)
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
				gzipWriter.Close()
				bw.Flush()
				file.Close()
				os.Rename(file.Name(), fmt.Sprintf("data/data-%s.json.gz", time.Now().Format(time.RFC3339)))
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

func (p *JsonWriter) Stop() {
	close(p.channel)
	p.wg.Wait()
}
