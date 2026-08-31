package export

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestCSVFormatterWritesSelectedColumnsInStableOrder(t *testing.T) {
	var output bytes.Buffer
	formatter, contentType, err := NewFormatter(FormatCSV, &output)
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "text/csv; charset=utf-8" {
		t.Fatalf("content type = %q", contentType)
	}
	columns := []Column{{Key: "id", Title: "编号"}, {Key: "name", Title: "名称"}}
	if err := formatter.WriteHeader(columns); err != nil {
		t.Fatal(err)
	}
	if err := formatter.WriteRows(columns, []map[string]any{{"name": "A,公司", "id": int64(7), "ignored": true}}); err != nil {
		t.Fatal(err)
	}
	if err := formatter.Close(); err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(strings.NewReader(output.String())).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"编号", "名称"}, {"7", "A,公司"}}
	if !equalRecords(records, want) {
		t.Fatalf("records = %#v, want %#v", records, want)
	}
}

func TestJSONLFormatterOmitsUnselectedFields(t *testing.T) {
	var output bytes.Buffer
	formatter, _, err := NewFormatter(FormatJSONL, &output)
	if err != nil {
		t.Fatal(err)
	}
	columns := []Column{{Key: "id"}, {Key: "active"}}
	if err := formatter.WriteHeader(columns); err != nil {
		t.Fatal(err)
	}
	if err := formatter.WriteRows(columns, []map[string]any{{"id": "1", "active": true, "secret": "hidden"}}); err != nil {
		t.Fatal(err)
	}
	if err := formatter.Close(); err != nil {
		t.Fatal(err)
	}
	var row map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &row); err != nil {
		t.Fatal(err)
	}
	if row["id"] != "1" || row["active"] != true {
		t.Fatalf("row = %#v", row)
	}
	if _, exists := row["secret"]; exists {
		t.Fatal("unselected secret field was exported")
	}
}

func TestXLSXFormatterProducesValidWorkbook(t *testing.T) {
	var output bytes.Buffer
	formatter, _, err := NewFormatter(FormatXLSX, &output)
	if err != nil {
		t.Fatal(err)
	}
	columns := []Column{{Key: "id", Title: "ID"}}
	if err := formatter.WriteHeader(columns); err != nil {
		t.Fatal(err)
	}
	if err := formatter.WriteRows(columns, []map[string]any{{"id": "42"}}); err != nil {
		t.Fatal(err)
	}
	if err := formatter.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := zip.NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatalf("invalid xlsx zip: %v", err)
	}
	if len(reader.File) == 0 {
		t.Fatal("empty xlsx archive")
	}
}

func TestFormatterPropagatesWriterFailure(t *testing.T) {
	formatter, _, err := NewFormatter(FormatCSV, failingWriter{})
	if err != nil {
		t.Fatal(err)
	}
	if err := formatter.WriteHeader([]Column{{Key: "id", Title: "ID"}}); err != nil {
		t.Fatal(err)
	}
	if err := formatter.Close(); err == nil {
		t.Fatal("expected writer failure")
	}
}

func TestNewFormatterRejectsUnknownFormat(t *testing.T) {
	_, _, err := NewFormatter("pdf", io.Discard)
	if !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("error = %v", err)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func equalRecords(left, right [][]string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if len(left[i]) != len(right[i]) {
			return false
		}
		for j := range left[i] {
			if left[i][j] != right[i][j] {
				return false
			}
		}
	}
	return true
}
