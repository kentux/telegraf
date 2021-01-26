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

func TestMoveCSVFileToImport(t *testing.T) {
	acc := testutil.Accumulator{}
	wd, err := os.Getwd()
	require.NoError(t, err)
	testFile := "colors.csv"

	// Establish process directory and finished directory.
	testFileDirectory := filepath.Join(wd, "testfiles")
	processDirectory, err := ioutil.TempDir(testFileDirectory, "process")
	require.NoError(t, err)
	defer os.Remove(processDirectory)

	// Init plugin.
	r := DirectoryMonitor{
		Directory:          processDirectory,
		FinishedDirectory:  testFileDirectory,
		MaxBufferedMetrics: 1000,
	}
	r.Log = testutil.Logger{}
	err = r.Init()
	require.NoError(t, err)

	parserConfig := parsers.Config{
		DataFormat:        "csv",
		CSVHeaderRowCount: 1,
	}
	nParser, err := parsers.NewParser(&parserConfig)
	require.NoError(t, err)
	r.parser = nParser

	r.Start(&acc)
	require.NoError(t, err)

	// Move file to process into the 'process' directory.
	os.Rename(filepath.Join(testFileDirectory, testFile), filepath.Join(processDirectory, testFile))

	// Ensure that a stop event still allows for the file that's already processing to finish.
	r.Stop()
	time.Sleep(10 * time.Millisecond)

	// Verify that we read the CSV once.
	require.Equal(t, len(acc.Metrics), 2)

	// File should have been moved back to the test directory, as we configured.
	_, err = os.Stat(filepath.Join(testFileDirectory, testFile))
	require.NoError(t, err)
}

func TestCSVAddedLive(t *testing.T) {
	acc := testutil.Accumulator{}
	wd, err := os.Getwd()
	require.NoError(t, err)
	testFile := "test.csv"

	// Establish process directory and finished directory.
	testFileDirectory := filepath.Join(wd, "testfiles")
	processDirectory, err := ioutil.TempDir(testFileDirectory, "process")
	require.NoError(t, err)
	defer os.Remove(processDirectory)

	// Init plugin.
	r := DirectoryMonitor{
		Directory:          processDirectory,
		FinishedDirectory:  testFileDirectory,
		MaxBufferedMetrics: 1000,
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

	// Start plugin before adding file.
	r.Start(&acc)

	// Write file to process into the 'process' directory.
	testFileName := filepath.Join(processDirectory, testFile)
	f, err := os.Create(testFileName)
	require.NoError(t, err)
	f.WriteString("thing,color\nsky,blue\ngrass,green\nclifford,red\n")
	f.Close()
	defer os.Remove(filepath.Join(testFileDirectory, testFile))

	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Verify that we read the CSV once.
	require.Equal(t, len(acc.Metrics), 3)

	// File should have back to the test directory, as we configured.
	_, err = os.Stat(filepath.Join(testFileDirectory, testFile))
	require.NoError(t, err)
}

func TestGZFileImport(t *testing.T) {
	acc := testutil.Accumulator{}
	wd, err := os.Getwd()
	require.NoError(t, err)
	testFile := "test.csv.gz"

	// Establish process directory and finished directory.
	testFileDirectory := filepath.Join(wd, "testfiles")
	processDirectory, err := ioutil.TempDir(testFileDirectory, "process")
	require.NoError(t, err)
	defer os.Remove(processDirectory)

	// Init plugin.
	r := DirectoryMonitor{
		Directory:          processDirectory,
		FinishedDirectory:  testFileDirectory,
		MaxBufferedMetrics: 1000,
	}

	fmt.Println(processDirectory)
	fmt.Println(testFileDirectory)
	r.Log = testutil.Logger{}
	err = r.Init()
	require.NoError(t, err)

	parserConfig := parsers.Config{
		DataFormat:        "csv",
		CSVHeaderRowCount: 1,
	}
	nParser, err := parsers.NewParser(&parserConfig)
	require.NoError(t, err)
	r.parser = nParser

	r.Start(&acc)
	require.NoError(t, err)

	// Write gz file to process into the 'process' directory.
	testFileName := filepath.Join(processDirectory, testFile)
	require.NoError(t, err)
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte("thing,color\nsky,blue\ngrass,green\nclifford,red\n"))
	w.Close()
	err = ioutil.WriteFile(testFileName, b.Bytes(), 0666)
	defer os.Remove(filepath.Join(testFileDirectory, testFile))

	time.Sleep(50 * time.Millisecond)

	// Verify that we read the CSV once.
	require.Equal(t, len(acc.Metrics), 3)

	// File should have back to the test directory, as we configured.
	_, err = os.Stat(filepath.Join(testFileDirectory, testFile))
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
	wd, err := os.Getwd()
	require.NoError(t, err)

	// Establish process directory and finished directory.
	testFileDirectory := filepath.Join(wd, "testfiles")
	processDirectory, err := ioutil.TempDir(testFileDirectory, "process")
	require.NoError(t, err)
	defer os.Remove(processDirectory)

	// Init plugin.
	r := DirectoryMonitor{
		Directory:          processDirectory,
		FinishedDirectory:  testFileDirectory,
		MaxBufferedMetrics: 1000,
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
	r.Start(&acc)
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
		defer os.Remove(filepath.Join(testFileDirectory, "test"+fmt.Sprint(count)+".json"))
	}

	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

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
