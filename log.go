package lib

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type LogClient struct {
}

var mapLogLevels = map[string]logrus.Level{
	"error": 2,
	"warn":  3,
	"info":  4,
	"debug": 5,
}

func GetLogClient(level string) *LogClient {
	logrus.SetFormatter(newGCEFormatter(true))
	logrus.SetLevel(mapLogLevels[level])
	logClient := LogClient{}
	return &logClient
}

func (client *LogClient) Error(args ...interface{}) {
	logrus.Error(args)
}

func (client *LogClient) Warn(args ...interface{}) {
	logrus.Warn(args)
}

func (client *LogClient) Info(args ...interface{}) {
	logrus.Info(args)
}

func (client *LogClient) Debug(args ...interface{}) {
	logrus.Debug(args)
}

type severity string

const (
	severityDEBUG     severity = "DEBUG"
	severityINFO      severity = "INFO"
	severityNOTICE    severity = "NOTICE"
	severityWARNING   severity = "WARNING"
	severityERROR     severity = "ERROR"
	severityCRITICAL  severity = "CRITICAL"
	severityALERT     severity = "ALERT"
	severityEMERGENCY severity = "EMERGENCY"
)

var (
	levelsLogrusToGCE = map[logrus.Level]severity{
		logrus.DebugLevel: severityDEBUG,
		logrus.InfoLevel:  severityINFO,
		logrus.WarnLevel:  severityWARNING,
		logrus.ErrorLevel: severityERROR,
		logrus.FatalLevel: severityCRITICAL,
		logrus.PanicLevel: severityALERT,
	}
)

var (
	stackSkips   = map[logrus.Level]int{}
	stackSkipsMu = sync.RWMutex{}
)

var (
	ErrSkipNotFound = errors.New("could not find skips for log level")
)

func getSkipLevel(level logrus.Level) (int, error) {
	stackSkipsMu.RLock()
	if skip, ok := stackSkips[level]; ok {
		defer stackSkipsMu.RUnlock()
		return skip, nil
	}
	stackSkipsMu.RUnlock()

	stackSkipsMu.Lock()
	defer stackSkipsMu.Unlock()
	if skip, ok := stackSkips[level]; ok {
		return skip, nil
	}

	// detect until we escape logrus back to the client package
	// skip out of runtime and logrusgce package, hence 3
	stackSkipsCallers := make([]uintptr, 20)
	runtime.Callers(3, stackSkipsCallers)
	for i, pc := range stackSkipsCallers {
		f := runtime.FuncForPC(pc)
		if strings.HasPrefix(f.Name(), "github.com/sirupsen/logrus") {
			continue
		}
		stackSkips[level] = i + 1
		return i + 1, nil
	}
	return 0, ErrSkipNotFound
}

type GCEFormatter struct {
	withSourceInfo bool
}

func newGCEFormatter(withSourceInfo bool) *GCEFormatter {
	return &GCEFormatter{withSourceInfo: withSourceInfo}
}

func (f *GCEFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	data := make(logrus.Fields, len(entry.Data)+3)
	for k, v := range entry.Data {
		switch v := v.(type) {
		case error:
			// Otherwise errors are ignored by `encoding/json`
			// https://github.com/Sirupsen/logrus/issues/137
			data[k] = v.Error()
		default:
			data[k] = v
		}
	}

	data["time"] = entry.Time.Format(time.RFC3339Nano)
	data["severity"] = levelsLogrusToGCE[entry.Level]
	data["message"] = entry.Message

	// if f.withSourceInfo {
	// 	skip, err := getSkipLevel(entry.Level)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	if pc, file, line, ok := runtime.Caller(skip); ok {
	// 		f := runtime.FuncForPC(pc)
	// 		data["sourceLocation"] = map[string]interface{}{
	// 			"file":         file,
	// 			"line":         line,
	// 			"functionName": f.Name(),
	// 		}
	// 	}
	// }

	serialized, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal fields to JSON, %v", err)
	}
	return append(serialized, '\n'), nil
}
