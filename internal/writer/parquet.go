package writer

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

type WriterRow struct {
	Browser           string
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
}

type ParquetWriter struct {
	channel chan WriterRow
	wg      *sync.WaitGroup
}

func NewParquetWriter() *ParquetWriter {
	return &ParquetWriter{
		channel: make(chan WriterRow),
		wg:      &sync.WaitGroup{},
	}
}

func (p *ParquetWriter) Start() chan WriterRow {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			file, err := os.Create(fmt.Sprintf("data/data-%d.temp", time.Now().Unix()))
			if err != nil {
				log.Fatal(err)
			}
			writer := parquet.NewGenericWriter[WriterRow](file, parquet.Compression(&parquet.Zstd))
			i := 0
			closed := false

		loop:
			for {
				context, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
				defer cancel()
				select {
				case <-context.Done():
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

				if i > 100000 {
					break
				}
			}

			if i > 0 {
				writer.Close()
				file.Close()
				os.Rename(file.Name(), fmt.Sprintf("data/data-%s.parquet", time.Now().Format(time.RFC3339)))
			} else {
				log.Println("Removing empty file")
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
