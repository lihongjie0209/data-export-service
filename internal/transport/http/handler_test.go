package httptransport

import (
	"encoding/json"
	"testing"

	platformexport "github.com/lihongjie0209/data-export-service/internal/export"
)

func TestExportTransportEmitsStructuredJSON(t *testing.T) {
	t.Parallel()
	encoded, err := json.Marshal(exportJobBody(platformexport.Job{
		QueryJSON:           `{"status":"paid"}`,
		SelectedColumnsJSON: `["id","total_minor"]`,
	}))
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	if _, ok := value["query"].(map[string]any); !ok {
		t.Fatalf("query was encoded as a string: %s", encoded)
	}
	if columns, ok := value["selected_columns"].([]any); !ok || len(columns) != 2 {
		t.Fatalf("selected_columns was not an array: %s", encoded)
	}
}

func TestExportRequestAcceptsLegacyJSONString(t *testing.T) {
	t.Parallel()
	got := rawObject(json.RawMessage(`"{\"status\":\"paid\"}"`))
	if string(got) != `{"status":"paid"}` {
		t.Fatalf("rawObject() = %s", got)
	}
}
