package cleanup

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/amir20/drain/internal"
	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"
)

func Cleanup(logger *zap.SugaredLogger) error {
	buckets := make(map[time.Time][]string)
	paths, err := filepath.Glob("data/data-*.parquet")
	if err != nil {
		return err
	}

	midnight := time.Now().Truncate(24 * time.Hour)
	for _, path := range paths {
		base := filepath.Base(path)
		if len(base) < 25 {
			logger.Debugf("skipping file %s", path)
			continue
		}
		datetime := base[5:25]
		t, err := time.Parse(time.RFC3339, datetime)
		if err != nil {
			logger.Errorf("failed to parse datetime %s: %v", datetime, err)
			continue
		}

		if t.After(midnight) {
			logger.Debugf("skipping data for today for file %s", path)
			continue
		}

		bucket := t.Truncate(24 * time.Hour)
		buckets[bucket] = append(buckets[bucket], path)
	}

	for bucket, files := range buckets {
		logger.Infof("processing bucket %s", bucket)
		var readers []parquet.RowReader
		for _, path := range files {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			reader := parquet.NewGenericReader[internal.Event](file)
			readers = append(readers, reader)
		}

		dest, err := os.Create(fmt.Sprintf("data/day-%s.parquet", bucket.Format(time.DateOnly)))
		if err != nil {
			return err
		}
		writer := parquet.NewWriter(dest, parquet.Compression(&parquet.Zstd))

		for _, reader := range readers {
			parquet.CopyRows(writer, reader)
		}
		writer.Close()
		dest.Close()

		for _, file := range files {
			err := os.Rename(file, fmt.Sprintf("data/%s.merged", filepath.Base(file)))
			if err != nil {
				return err
			}
		}
	}

	return nil
}
