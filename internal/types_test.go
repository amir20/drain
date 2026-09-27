package internal

import (
	"testing"

	"github.com/parquet-go/parquet-go"
)

// Every Parquet file has to share one schema, or a glob scan over old and new files
// breaks. FileAgents and Raw go to Postgres only.
func TestParquetSchemaUnchanged(t *testing.T) {
	schema := parquet.SchemaOf(Event{})
	if len(schema.Fields()) != 21 {
		t.Errorf("parquet schema has %d columns, want 21", len(schema.Fields()))
	}
	for _, f := range schema.Fields() {
		if f.Name() == "FileAgents" || f.Name() == "Raw" {
			t.Errorf("%s must not be a parquet column", f.Name())
		}
	}
}
