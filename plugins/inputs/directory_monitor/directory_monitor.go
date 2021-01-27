package directory_monitor

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/dimchansky/utfbom"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/common/encoding"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/influxdata/telegraf/selfstat"
	cmap "github.com/orcaman/concurrent-map"
	"gopkg.in/djherbis/times.v1"

	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

const sampleConfig = `
## The directory to monitor and read files from.
directory = ""
#
## The directory to move finished files to.
finished_directory = "5s"
#
## Whether or not to move files that error out to an error directory.
use_error_directory = "true"
#
## The directory to move files to upon file error, given that 'use_error_directory' is enabled.
## If not is given, the error directory will be auto-generated.
# error_directory = ""
#
## The interval at which to check the directory for new files.
# monitor_interval = "50ms"
#
## The amount of time a file is allowed to sit in the directory before it is picked up.
## This time can generally be low but if you choose to have a very large file written to the directory and it's potentially slow,
## set this higher so that the plugin will wait until the file is fully copied to the directory.
# directory_duration_threshold = "50ms"
#
## Character encoding to use when interpreting the file contents. Invalid
## characters are replaced using the unicode replacement character. Defaults to utf-8.
##   ex: character_encoding = "utf-8"
##       character_encoding = "utf-16le"
##       character_encoding = "utf-16be"
# character_encoding = "utf-8"
#
## A list of the only file names to monitor, if necessary. Supports regex. If left blank, all files are ingested.
# files_to_monitor = [".*.csv"]
#
## A list of files to ignore, if necessary. Supports regex.
# files_to_ignore = [".DS_Store"]
#
## Maximum lines of the file to process that have not yet be written by the
## output. For best throughput set based on the number of metrics and the size of the output's metric_batch_size.
## Warning: setting this number too high can lead to a high number of dropped metrics.
# max_buffered_metrics = 1000
#
## The maximum amount of files to process at once. A very high number can lead to bigger memory use and potential file system errors.
# max_concurrent_files = 3000
#
## The dataformat to be read from the files.
## Each data format has its own unique set of configuration options, read
## more about them here:
## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
data_format = "influx"
`

var (
	defaultFilesToMonitor             = []string{}
	defaultFilesToIgnore              = []string{}
	defaultMaxBufferedMetrics         = 1000
	defaultMaxConcurrentFiles         = 3000
	defaultMonitorInterval            = internal.Duration{Duration: 50 * time.Millisecond}
	defaultDirectoryDurationThreshold = internal.Duration{Duration: 50 * time.Millisecond}
)

type empty struct{}
type semaphore chan empty

type DirectoryMonitor struct {
	Directory         string `toml:"directory"`
	FinishedDirectory string `toml:"finished_directory"`
	UseErrorDirectory bool   `toml:"use_error_directory"`
	ErrorDirectory    string `toml:"error_directory"`

	CharacterEncoding          string            `toml:"character_encoding"`
	FilesToMonitor             []string          `toml:"files_to_monitor"`
	FilesToIgnore              []string          `toml:"files_to_ignore"`
	MaxBufferedMetrics         int               `toml:"max_buffered_metrics"`
	MonitorInterval            internal.Duration `toml:"monitor_interval"`
	DirectoryDurationThreshold internal.Duration `toml:"directory_duration_threshold"`
	MaxConcurrentFiles         int               `toml:"max_concurrent_files"`

	filesInUse          cmap.ConcurrentMap
	Log                 telegraf.Logger
	parser              parsers.Parser
	decoder             *encoding.Decoder
	filesProcessed      selfstat.Stat
	filesDropped        selfstat.Stat
	waitGroup           *sync.WaitGroup
	acc                 telegraf.TrackingAccumulator
	sem                 semaphore
	quit                chan bool
	fileRegexesToMatch  []*regexp.Regexp
	fileRegexesToIgnore []*regexp.Regexp
}

func (monitor *DirectoryMonitor) SampleConfig() string {
	return sampleConfig
}

func (monitor *DirectoryMonitor) Description() string {
	return "Ingests files in a directory and then moves them to a target directory."
}

func (monitor *DirectoryMonitor) Start(acc telegraf.Accumulator) error {
	// Use tracking to determine when more metrics can be added without overflowing the outputs.
	monitor.acc = acc.WithTracking(monitor.MaxBufferedMetrics)
	go func() {
		for {
			select {
			case <-monitor.acc.Delivered():
				<-monitor.sem
			}
		}
	}()

	// Check the directory at intervals and send off any and all files to be read.
	monitor.waitGroup.Add(1)
	go monitor.Monitor(acc, monitor.waitGroup)

	return nil
}

func (monitor *DirectoryMonitor) Stop() {
	// Before stopping, wrap up all file-reading routines.
	monitor.quit <- true
	monitor.Log.Warnf("Exiting the Directory Monitor plugin. Waiting to quit until all current files are finished.")
	monitor.waitGroup.Wait()
}

func (monitor *DirectoryMonitor) Monitor(acc telegraf.Accumulator, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	for {
		// Get all files sitting in the directory.
		files, err := ioutil.ReadDir(monitor.Directory)
		if err != nil {
			monitor.Log.Errorf("Unable to monitor the targeted directory due to the following error: " + fmt.Sprint(err))
			continue
		}

		var filesToProcess []os.FileInfo

		for _, file := range files {
			filePath := monitor.Directory + "/" + file.Name()

			// Errors here indicate the file has been removed or we can't access it at the moment. Retry later.
			fileInfo, err := os.Stat(filePath)
			if err != nil {
				continue
			}

			stat, err := times.Stat(filePath)
			if err != nil {
				continue
			}

			enoughRoomForFile := monitor.filesInUse.Count() < monitor.MaxConcurrentFiles
			timeThresholdExceeded := time.Since(stat.AccessTime()) > monitor.DirectoryDurationThreshold.Duration
			_, fileAlreadyProcessing := monitor.filesInUse.Get(filePath)

			// If file is decaying, process it.
			if enoughRoomForFile && timeThresholdExceeded && !fileAlreadyProcessing {
				// Set the file as 'in use' so that subsequent Monitor runs won't possibly pick it up again.
				monitor.filesInUse.Set(filePath, struct{}{})
				filesToProcess = append(filesToProcess, fileInfo)
			}
		}

		monitor.processFileBatch(filesToProcess, acc)

		select {
		// Monitor in intervals.
		case <-time.After(monitor.MonitorInterval.Duration):
		// Allow the monitor to be quit.
		case <-monitor.quit:
			return
		}
	}
}

func (monitor *DirectoryMonitor) processFileBatch(files []os.FileInfo, acc telegraf.Accumulator) {
	// Process each valid file with a new goroutine.
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			continue
		}

		filePath := monitor.Directory + "/" + fileInfo.Name()

		// File must be configured to be monitored, if any configuration...
		if !monitor.isMonitoredFile(fileInfo.Name()) {
			continue
		}

		// ...and should not be configured to be ignored.
		if monitor.isIgnoredFile(fileInfo.Name()) {
			continue
		}

		monitor.waitGroup.Add(1)
		go monitor.read(acc, filePath, monitor.waitGroup)
	}
}

func (monitor *DirectoryMonitor) read(acc telegraf.Accumulator, filePath string, waitGroup *sync.WaitGroup) {
	// Remove the file from the set of files in use when it's finished.
	defer monitor.filesInUse.Remove(filePath)
	defer waitGroup.Done()

	// Open, read, and parse the contents of the file.
	metrics, err := monitor.readFileToMetrics(filePath)

	// Handle a file read error. We don't halt execution but do document, log, and move the problematic file.
	if err != nil {
		monitor.Log.Errorf("Error while reading file: '" + filePath + "'. " + err.Error())
		monitor.filesDropped.Incr(1)
		if monitor.UseErrorDirectory {
			monitor.moveFile(filePath, monitor.ErrorDirectory)
		}
		return
	}

	// Report the metrics for the file.
	for _, m := range metrics {
		// Try writing out metric first without blocking.
		select {
		case monitor.sem <- empty{}:
			monitor.acc.AddTrackingMetricGroup([]telegraf.Metric{m})
			continue
		default:
		}

		// Block until room is available to add metrics.
		select {
		case monitor.sem <- empty{}:
			monitor.acc.AddTrackingMetricGroup([]telegraf.Metric{m})
		}
	}

	// File is finished, move it to the 'finished' directory.
	monitor.moveFile(filePath, monitor.FinishedDirectory)
	monitor.filesProcessed.Incr(1)
}

func (monitor *DirectoryMonitor) readFileToMetrics(filePath string) ([]telegraf.Metric, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Handle gzipped files.
	var reader io.Reader
	if filepath.Ext(filePath) == ".gz" {
		reader, err = gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
	} else {
		reader, _ = utfbom.Skip(monitor.decoder.Reader(file))
	}

	// Read the file and parse with the configured parse method.
	fileContents, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("E! Error file: %v could not be read, %s", filePath, err)
	}

	return monitor.parser.Parse(fileContents)
}

func (monitor *DirectoryMonitor) moveFile(filePath string, directory string) {
	err := os.Rename(filePath, directory+"/"+filepath.Base(filePath))

	if err != nil {
		monitor.Log.Errorf("Error while moving file '" + filePath + "' to another directory. Error: " + err.Error())
	}
}

func (monitor *DirectoryMonitor) isMonitoredFile(fileName string) bool {
	if len(monitor.fileRegexesToMatch) == 0 {
		return true
	}

	// Only monitor matching files.
	for _, regex := range monitor.fileRegexesToMatch {
		if regex.MatchString(fileName) {
			return true
		}
	}

	return false
}

func (monitor *DirectoryMonitor) isIgnoredFile(fileName string) bool {
	// Skip files that are set to be ignored.
	for _, regex := range monitor.fileRegexesToIgnore {
		if regex.MatchString(fileName) {
			return true
		}
	}

	return false
}

func (monitor *DirectoryMonitor) SetParser(p parsers.Parser) {
	monitor.parser = p
}

func (monitor *DirectoryMonitor) Gather(acc telegraf.Accumulator) error {
	return nil
}

func (monitor *DirectoryMonitor) Init() error {
	if monitor.Directory == "" || monitor.FinishedDirectory == "" {
		return errors.New("Missing one of the following required config options: directory, finished_directory.")
	}

	// Finished directory can be created if not exists for convenience.
	if _, err := os.Stat(monitor.FinishedDirectory); os.IsNotExist(err) {
		os.Mkdir(monitor.FinishedDirectory, 0777)
	}

	var err error
	monitor.decoder, err = encoding.NewDecoder(monitor.CharacterEncoding)
	if err != nil {
		return err
	}

	monitor.filesDropped = selfstat.Register("directory_monitor", "files_dropped", map[string]string{})
	monitor.filesProcessed = selfstat.Register("directory_monitor", "files_processed", map[string]string{})

	// If an error directory should be used but has not been configured yet, create one ourselves.
	if monitor.ErrorDirectory == "" && monitor.UseErrorDirectory {
		errorDirectoryName := monitor.Directory + "/telegraf_error"
		if _, err := os.Stat(errorDirectoryName); os.IsNotExist(err) {
			os.Mkdir(errorDirectoryName, 0777)
		}

		monitor.ErrorDirectory = errorDirectoryName
	}

	if monitor.CharacterEncoding == "" {
		monitor.CharacterEncoding = "utf-8"
	}

	monitor.waitGroup = new(sync.WaitGroup)
	monitor.sem = make(semaphore, monitor.MaxBufferedMetrics)
	monitor.filesInUse = cmap.New()
	monitor.quit = make(chan bool)

	// Establish file matching / exclusion regexes.
	for _, matcher := range monitor.FilesToMonitor {
		regex, err := regexp.Compile(matcher)
		if err != nil {
			return err
		}
		monitor.fileRegexesToMatch = append(monitor.fileRegexesToMatch, regex)
	}

	for _, matcher := range monitor.FilesToIgnore {
		regex, err := regexp.Compile(matcher)
		if err != nil {
			return err
		}
		monitor.fileRegexesToIgnore = append(monitor.fileRegexesToIgnore, regex)
	}

	return err
}

func init() {
	inputs.Add("directory_monitor", func() telegraf.Input {
		return &DirectoryMonitor{
			FilesToMonitor:             defaultFilesToMonitor,
			FilesToIgnore:              defaultFilesToIgnore,
			MaxBufferedMetrics:         defaultMaxBufferedMetrics,
			MonitorInterval:            defaultMonitorInterval,
			DirectoryDurationThreshold: defaultDirectoryDurationThreshold,
			MaxConcurrentFiles:         defaultMaxConcurrentFiles,
		}
	})
}
