package commands

import (
	"fmt"
	"os"
	"reflect"
	"strconv"

	"github.com/olekukonko/tablewriter"
)

// FormatValue converts a reflect.Value to a string representation
func FormatValue(fieldValue reflect.Value) string {
	switch fieldValue.Kind() {
	case reflect.String:
		return fieldValue.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(fieldValue.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(fieldValue.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(fieldValue.Float(), 'f', -1, 64)
	case reflect.Bool:
		return strconv.FormatBool(fieldValue.Bool())
	case reflect.Ptr:
		if fieldValue.IsNil() {
			return "<nil>"
		}
		return FormatValue(fieldValue.Elem())
	default:
		// Fallback for complex types
		return fmt.Sprintf("%v", fieldValue.Interface())
	}
}

func Tableizer(data interface{}) {
	// 1. Ensure the input is a slice or array
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		fmt.Printf("Error: tableize requires a slice or array, got %s\n", v.Kind())
		return
	}

	if v.Len() == 0 {
		fmt.Println("No data to display.")
		return
	}

	// Get the type of the elements in the slice
	elemType := v.Type().Elem()

	// 2. Initialize the tablewriter
	table := tablewriter.NewWriter(os.Stdout)

	// --- Extract Headers ---
	var headers []string
	var numFields int
	var isStruct bool

	// Ensure we are working with a struct pointer or struct value
	actualType := elemType
	if actualType.Kind() == reflect.Ptr {
		actualType = actualType.Elem()
	}

	// Check if the element type is a struct
	if actualType.Kind() == reflect.Struct {
		isStruct = true
		numFields = actualType.NumField()
		for i := 0; i < numFields; i++ {
			field := actualType.Field(i)
			// Use the field name as the column header
			headers = append(headers, field.Name)
		}
		// Convert headers to []any
		headerVals := make([]any, len(headers))
		for i, h := range headers {
			headerVals[i] = h
		}
		table.Header(headerVals...)
	} else {
		// For non-struct types, use the type name as the header
		isStruct = false
		headerName := actualType.Name()
		if headerName == "" {
			headerName = "Value"
		}
		table.Header(headerName)
	}

	// --- Extract Data Rows ---
	for i := 0; i < v.Len(); i++ {
		// Get the current value
		item := v.Index(i)

		// If the slice contained pointers, dereference it
		if item.Kind() == reflect.Ptr {
			if item.IsNil() {
				// Handle nil pointers
				if isStruct {
					rowData := make([]interface{}, numFields)
					for j := range rowData {
						rowData[j] = "<nil>"
					}
					table.Append(rowData...)
				} else {
					table.Append("<nil>")
				}
				continue
			}
			item = item.Elem()
		}

		var rowData []interface{}
		if isStruct {
			// Handle struct types
			for j := 0; j < numFields; j++ {
				fieldValue := item.Field(j)
				rowData = append(rowData, FormatValue(fieldValue))
			}
		} else {
			// Handle non-struct types
			rowData = append(rowData, FormatValue(item))
		}
		table.Append(rowData...)
	}

	// 3. Render the table
	table.Render()
}
