package export

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/xuri/excelize/v2"
)

var ErrUnsupportedFormat = errors.New("unsupported export format")

type Formatter interface {
	WriteHeader([]Column) error
	WriteRows([]Column, []map[string]any) error
	Close() error
}

func NewFormatter(format string, dst io.Writer) (Formatter, string, error) {
	switch format {
	case FormatCSV:
		return &csvFormatter{writer: csv.NewWriter(dst)}, "text/csv; charset=utf-8", nil
	case FormatJSONL:
		return &jsonlFormatter{encoder: json.NewEncoder(dst)}, "application/x-ndjson", nil
	case FormatXLSX:
		f := excelize.NewFile()
		stream, err := f.NewStreamWriter("Sheet1")
		if err != nil {
			_ = f.Close()
			return nil, "", fmt.Errorf("create xlsx stream: %w", err)
		}
		return &xlsxFormatter{file: f, stream: stream, dst: dst, row: 1}, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", nil
	default:
		return nil, "", fmt.Errorf("%w: %s", ErrUnsupportedFormat, format)
	}
}

type csvFormatter struct{ writer *csv.Writer }

func (f *csvFormatter) WriteHeader(columns []Column) error {
	record := make([]string, len(columns))
	for i := range columns {
		record[i] = columns[i].Title
	}
	return f.writer.Write(record)
}
func (f *csvFormatter) WriteRows(columns []Column, rows []map[string]any) error {
	record := make([]string, len(columns))
	for _, row := range rows {
		for i := range columns {
			record[i] = scalarString(row[columns[i].Key])
		}
		if err := f.writer.Write(record); err != nil {
			return err
		}
	}
	return nil
}
func (f *csvFormatter) Close() error {
	f.writer.Flush()
	return f.writer.Error()
}

type jsonlFormatter struct{ encoder *json.Encoder }

func (*jsonlFormatter) WriteHeader([]Column) error { return nil }
func (f *jsonlFormatter) WriteRows(columns []Column, rows []map[string]any) error {
	for _, row := range rows {
		selected := make(map[string]any, len(columns))
		for i := range columns {
			selected[columns[i].Key] = row[columns[i].Key]
		}
		if err := f.encoder.Encode(selected); err != nil {
			return err
		}
	}
	return nil
}
func (*jsonlFormatter) Close() error { return nil }

type xlsxFormatter struct {
	file   *excelize.File
	stream *excelize.StreamWriter
	dst    io.Writer
	row    int
	closed bool
}

func (f *xlsxFormatter) WriteHeader(columns []Column) error {
	values := make([]any, len(columns))
	for i := range columns {
		values[i] = columns[i].Title
	}
	return f.write(values)
}
func (f *xlsxFormatter) WriteRows(columns []Column, rows []map[string]any) error {
	values := make([]any, len(columns))
	for _, row := range rows {
		for i := range columns {
			values[i] = spreadsheetValue(row[columns[i].Key])
		}
		if err := f.write(values); err != nil {
			return err
		}
	}
	return nil
}
func (f *xlsxFormatter) write(values []any) error {
	cell, err := excelize.CoordinatesToCellName(1, f.row)
	if err != nil {
		return err
	}
	if err := f.stream.SetRow(cell, values); err != nil {
		return err
	}
	f.row++
	return nil
}
func (f *xlsxFormatter) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	if err := f.stream.Flush(); err != nil {
		_ = f.file.Close()
		return err
	}
	if err := f.file.Write(f.dst); err != nil {
		_ = f.file.Close()
		return err
	}
	return f.file.Close()
}

func scalarString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case bool:
		return strconv.FormatBool(v)
	case json.Number:
		return v.String()
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case int:
		return strconv.Itoa(v)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	default:
		encoded, err := json.Marshal(v)
		if err == nil {
			return string(encoded)
		}
		return fmt.Sprint(v)
	}
}

func spreadsheetValue(value any) any {
	switch value.(type) {
	case nil, string, bool, float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, time.Time:
		return value
	default:
		return scalarString(value)
	}
}
