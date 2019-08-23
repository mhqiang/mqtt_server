package mqtt_server

import (
	"fmt"
	"utils"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LogStructT struct {
	Log *zap.Logger
}

func InitLog(fileName string, level int8) *LogStruct {
	Log := utils.NewLogger("test.log", zapcore.InfoLevel, 128, 100, 7, true, "v3")
	curLog := &LogStruct{
		Log: Log,
	}
	return curLog
}

func (logger *LogStruct) Warning(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a)
	logger.Log.Warn("Warn", zap.String("msg", msg))
}
func (logger *LogStruct) Info(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	fmt.Sprintf(format, a)
	logger.Log.Info("Info", zap.String("msg", msg))
}
func (logger *LogStruct) Error(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a)
	logger.Log.Error("Error", zap.String("msg", msg))
}
