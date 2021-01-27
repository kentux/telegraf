package directory_monitor

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/require"
)

func TestCSVGZImport(t *testing.T) {
	acc := testutil.Accumulator{}
	testCsvFile := "test.csv"
	testCsvGzFile := "test.csv.gz"

	// Establish process directory and finished directory.
	finishedDirectory, err := ioutil.TempDir("", "finished")
	require.NoError(t, err)
	processDirectory, err := ioutil.TempDir("", "test")
	require.NoError(t, err)
	defer os.RemoveAll(processDirectory)
	defer os.RemoveAll(finishedDirectory)

	// Init plugin.
	r := DirectoryMonitor{
		Directory:          processDirectory,
		FinishedDirectory:  finishedDirectory,
		MaxBufferedMetrics: 1000,
		MaxConcurrentFiles: 1000,
	}
	err = r.Init()
	require.NoError(t, err)

	parserConfig := parsers.Config{
		DataFormat:        "csv",
		CSVHeaderRowCount: 1,
	}
	nParser, err := parsers.NewParser(&parserConfig)
	require.NoError(t, err)
	r.parser = nParser
	r.Log = testutil.Logger{}

	// Write csv file to process into the 'process' directory.
	f, err := os.Create(filepath.Join(processDirectory, testCsvFile))
	require.NoError(t, err)
	f.WriteString("thing,color\nsky,blue\ngrass,green\nclifford,red\n")
	f.Close()

	// Write csv.gz file to process into the 'process' directory.
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte("thing,color\nsky,blue\ngrass,green\nclifford,red\n"))
	w.Close()
	err = ioutil.WriteFile(filepath.Join(processDirectory, testCsvGzFile), b.Bytes(), 0666)

	// Start plugin before adding file.
	err = r.Start(&acc)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	r.Stop()

	// Verify that we read both files once.
	require.Equal(t, len(acc.Metrics), 6)

	// File should have gone back to the test directory, as we configured.
	_, err = os.Stat(filepath.Join(finishedDirectory, testCsvFile))
	_, err = os.Stat(filepath.Join(finishedDirectory, testCsvGzFile))

	require.NoError(t, err)
}

// For JSON data.
type event struct {
	Name   string
	Speed  float64
	Length float64
}

func TestMultipleJSONFileImports(t *testing.T) {
	acc := testutil.Accumulator{}

	// Establish process directory and finished directory.
	finishedDirectory, err := ioutil.TempDir("", "finished")
	require.NoError(t, err)
	processDirectory, err := ioutil.TempDir("", "test")
	require.NoError(t, err)
	defer os.RemoveAll(processDirectory)
	defer os.RemoveAll(finishedDirectory)

	// Init plugin.
	r := DirectoryMonitor{
		Directory:          processDirectory,
		FinishedDirectory:  finishedDirectory,
		MaxBufferedMetrics: 1000,
		MaxConcurrentFiles: 1000,
	}
	err = r.Init()
	require.NoError(t, err)

	parserConfig := parsers.Config{
		DataFormat:  "json",
		JSONNameKey: "Name",
	}
	nParser, err := parsers.NewParser(&parserConfig)
	require.NoError(t, err)
	r.parser = nParser
	err = r.Start(&acc)
	r.Log = testutil.Logger{}

	// Let's drop 5 JSONs.
	fileData := []event{
		{
			Name:   "event1",
			Speed:  100.1,
			Length: 20.1,
		},
		{
			Name:   "event2",
			Speed:  500,
			Length: 1.4,
		},
		{
			Name:   "event3",
			Speed:  200,
			Length: 10.23,
		},
		{
			Name:   "event4",
			Speed:  80,
			Length: 250,
		},
		{
			Name:   "event5",
			Speed:  120.77,
			Length: 25.97,
		},
	}

	for count, data := range fileData {
		writeJSONFile(data, filepath.Join(processDirectory, "test"+fmt.Sprint(count)+".json"))
	}

	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	r.Stop()

	// Verify that we read each JSON file once.
	require.Equal(t, len(acc.Metrics), 5)

	// Verify all fields read.
	for _, data := range fileData {
		acc.AssertContainsFields(t, data.Name, map[string]interface{}{"Length": data.Length, "Speed": data.Speed})
	}
}

func writeJSONFile(data event, filePath string) (int, error) {
	//write data as buffer to json encoder
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.SetIndent("", "\t")

	err := encoder.Encode(data)
	if err != nil {
		return 0, err
	}
	f, err := os.Create(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	n, err := f.Write(buffer.Bytes())
	if err != nil {
		return 0, err
	}
	return n, nil
}
