package util

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"time"
	_ "time/tzdata"

	"github.com/parquet-go/parquet-go"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func CreateLoggerDocker(prefix string, useStderr bool) *log.Logger {
	var output *os.File
	if useStderr {
		output = os.Stderr
	} else {
		output = os.Stdout
	}

	logger := log.New(output, prefix, log.LstdFlags|log.Lmicroseconds)
	return logger
}

func CreateSubsetMap(originalMap map[string]interface{}, fields []string) map[string]interface{} {
	subset := make(map[string]interface{})
	for _, field := range fields {
		if value, exists := originalMap[field]; exists {
			subset[field] = value
		}
	}
	return subset
}

func RecoverPanic(loggerErrorTrigger *log.Logger, restartFn func()) {
	if r := recover(); r != nil {
		var err error
		switch x := r.(type) {
		case string:
			err = fmt.Errorf("%s", x)
		case error:
			err = x
		default:
			err = fmt.Errorf("unknown panic")
		}
		_, file, line, ok := runtime.Caller(2) // Note: Changed to 2 to capture the direct caller
		if ok {
			loggerErrorTrigger.Printf("Recovered from panic - %s:%d: %v\n", file, line, err)
		} else {
			loggerErrorTrigger.Printf("Recovered from panic: %v\n", err)
		}
		loggerErrorTrigger.Printf("Stack trace:\n%s\n", string(debug.Stack()))

		// Check if a restart function was provided
		if restartFn != nil {
			// Log the restart attempt
			loggerErrorTrigger.Println("Attempting to restart the function...")
			go restartFn()
		}
	}
}

const defaultMaxSizeInBytes = int64(1024 * 1024) // 1 MB

func CreateLogger(fileName string, options ...interface{}) (*log.Logger, *os.File, error) {
	var logFilePath string
	var path string
	var maxSizeInBytes int64

	// Process options
	for _, option := range options {
		switch opt := option.(type) {
		case string:
			path = opt
		case int64:
			maxSizeInBytes = opt
		}
	}

	// If path is not provided, use the project path
	if path == "" {
		projectPath, err := os.Getwd()
		if err != nil {
			return nil, nil, err
		}
		logFilePath = filepath.Join(projectPath, fileName)
	} else {
		logFilePath = filepath.Join(path, fileName)
	}

	// Check if maxSizeInBytes is provided by the user
	if maxSizeInBytes == 0 {
		// Use the default size if maxSizeInBytes is not provided
		maxSizeInBytes = defaultMaxSizeInBytes
	}

	// Check if the log file exists
	if _, err := os.Stat(logFilePath); os.IsNotExist(err) {
		// Log file doesn't exist, create it
		logFile, err := os.Create(logFilePath)
		if err != nil {
			return nil, nil, err
		}
		logger := log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
		return logger, logFile, nil
	}

	// Log file already exists, open it for appending
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return nil, nil, err
	}

	// Check if the log file exceeds the specified size limit
	fileInfo, err := logFile.Stat()
	if err != nil {
		return nil, nil, err
	}
	if fileInfo.Size() > maxSizeInBytes {
		// Truncate the log file
		err = logFile.Truncate(0)
		if err != nil {
			return nil, nil, err
		}
		_, err = logFile.Seek(0, 0)
		if err != nil {
			return nil, nil, err
		}
	}

	logger := log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)

	return logger, logFile, nil
}

func TimeUntilNext5Minutes() time.Duration {
	now := time.Now()
	roundedTime := now.Truncate(5 * time.Minute)
	next5Minutes := roundedTime.Add(5 * time.Minute)
	remainingTime := next5Minutes.Sub(now)
	return remainingTime
}
func ExtractTimestampFromErrorMessage(errorMessage string) (int64, error) {
	// Define a regular expression to find the timestamp in the error message
	re := regexp.MustCompile(`banned until (\d+)\.`)

	// Find the matches in the error message
	matches := re.FindStringSubmatch(errorMessage)
	if len(matches) < 2 {
		return 0, fmt.Errorf("timestamp not found in error message")
	}

	// Extract the timestamp string
	timestampString := matches[1]

	// Convert the timestamp string to an integer
	timestamp, err := strconv.ParseInt(timestampString, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error converting timestamp to integer: %v", err)
	}

	return timestamp, nil
}

func GetParentDirectory() (string, error) {
	path, err := os.Executable()
	if err != nil {
		fmt.Println("Error:", err)
		return "", err
	}
	return filepath.Abs(filepath.Dir(filepath.Dir(path)))
}

func ReadParquet(fileName string, slicePtr interface{}) error {
	rf, err := os.Open(fileName)

	if err != nil {
		return err
	}

	var pf *parquet.Reader

	// Use defer to recover from a panic and handle it gracefully
	defer func() {
		if r := recover(); r != nil {
			//fmt.Println("Panic:", r)
			if pf != nil {
				pf.Close() // Close the parquet.Reader if it was created before the panic
			}
		}
	}()

	pf = parquet.NewReader(rf)

	defer rf.Close()
	defer pf.Close()

	// Ensure that slicePtr is a pointer to a slice
	sliceValue := reflect.ValueOf(slicePtr)
	if sliceValue.Kind() != reflect.Ptr || sliceValue.Elem().Kind() != reflect.Slice {
		return errors.New("slicePtr must be a pointer to a slice")
	}

	for {
		// Create a new element of the slice's type
		elemType := sliceValue.Elem().Type().Elem()
		elem := reflect.New(elemType).Elem()

		// Read a row into the element
		err := pf.Read(elem.Addr().Interface())
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Append the element to the slice
		sliceValue.Elem().Set(reflect.Append(sliceValue.Elem(), elem))
	}

	return nil
}
func GenerateParquet(data []map[string]interface{}, fileName string) error {
	// Get the sample map from the first element
	if len(data) == 0 {
		return errors.New("empty data slice")
	}
	sampleMap := data[0]

	// Generate struct type dynamically
	structType := mapToStructWithTags(sampleMap)

	// Create a new struct slice with the appropriate type
	structSlice := reflect.MakeSlice(reflect.SliceOf(structType), len(data), len(data))

	// Populate the struct instances
	for i, mapData := range data {
		structInstance := reflect.New(structType).Elem()

		// Set values for each field
		for key, value := range mapData {
			// Use title case mapping for the key
			fieldName := cases.Title(language.English).String(key)

			field := structInstance.FieldByName(fieldName)
			if field.IsValid() {
				// Convert the value to the field's type before setting
				fieldValue := reflect.ValueOf(value)
				if fieldValue.Type() == field.Type() {
					field.Set(fieldValue)
				}
			} else {
				fmt.Printf("Invalid field: %s\n", key)
			}
		}

		// Assign the struct instance to the slice
		structSlice.Index(i).Set(structInstance)
	}

	// Create a regular file
	f, err := os.Create(fileName)

	if err != nil {
		return err
	}
	writer := parquet.NewWriter(f)

	// Write each struct instance to Parquet
	for i := 0; i < structSlice.Len(); i++ {
		if err := writer.Write(structSlice.Index(i).Interface()); err != nil {
			return err
		}
	}

	_ = writer.Close()
	_ = f.Close()

	return nil
}

// mapToStructWithTags creates a struct dynamically from a sample map with JSON tags.
func mapToStructWithTags(sampleMap map[string]interface{}) reflect.Type {
	// Create a slice to store struct fields
	var fields []reflect.StructField
	// Create a Title Case mapper for field names
	mapper := cases.Title(language.English)

	// Iterate over the map and add fields to the slice with types
	for key, value := range sampleMap {
		// Ensure that the generated struct field is exported
		fieldName := mapper.String(key)

		fields = append(fields, reflect.StructField{
			Name: fieldName,
			Type: reflect.TypeOf(value),
			Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"name=%s, type=%s, encoding=PLAIN_DICTIONARY" json:"%s"`, key, reflect.TypeOf(value), key)),
		})
	}

	// Create a new struct type
	return reflect.StructOf(fields)
}

func MapsToStructs(sampleMap map[string]interface{}, data []map[string]interface{}) interface{} {
	// Get the struct type based on the sample map
	structType := mapToStructWithTags(sampleMap)

	// Create a slice type for the struct type
	sliceType := reflect.SliceOf(structType)

	// Create a new slice with the appropriate type
	structSlice := reflect.MakeSlice(sliceType, len(data), len(data))

	// Iterate over the data and populate the struct instances
	for i, mapData := range data {
		structInstance := reflect.New(structType).Elem()

		// Set values for each field
		for key, value := range mapData {
			field := structInstance.FieldByName(key)
			field.Set(reflect.ValueOf(value))
		}

		// Append the struct instance to the slice
		structSlice.Index(i).Set(structInstance)
	}

	// Return the slice of struct instances
	return structSlice.Interface()
}

func FormatCorrectTypes(data []map[string]interface{}) []map[string]interface{} {
	if len(data) == 0 {
		return nil
	}
	// Infer column order from the first row
	var columnOrder []string
	for col := range data[0] {
		columnOrder = append(columnOrder, col)
	}

	newData := make([]map[string]interface{}, len(data))

	for i, row := range data {
		newRow := make(map[string]interface{}, len(row))
		for _, col := range columnOrder {
			if timestamp, ok := row[col].(time.Time); ok {
				newRow[col] = timestamp
			} else if strVal, ok := row[col].(string); ok {
				// Try to parse string as time.Time
				if parsedTime, err := time.Parse(time.RFC3339, strVal); err == nil {
					newRow[col] = parsedTime.Format(time.RFC3339)
				} else if num, err := strconv.ParseFloat(strVal, 64); err == nil {
					// If conversion fails, try to convert string number to float64
					newRow[col] = num
				} else {
					// If all conversions fail, keep the original string value
					newRow[col] = row[col]
				}
			} else {
				newRow[col] = row[col]
			}
		}
		newData[i] = newRow
	}
	return newData
}

// ResampleOHLCV takes a slice of OHLCV data (as maps) and resamples it to the specified time frame
func ResampleOHLCV(data []map[string]interface{}, targetTimeFrame time.Duration) []map[string]interface{} {
	resampledData := make(map[string]map[string]interface{})

	for _, entry := range data {
		entryTime, ok := entry["time"].(time.Time)
		if !ok {
			// Handle error or skip entry
			continue
		}

		symbol, ok := entry["symbol"].(string)
		if !ok {
			// Handle error or skip entry without symbol
			continue
		}

		// Round the entry time to the nearest targetTimeFrame interval
		roundedTime := roundToInterval(entryTime, targetTimeFrame)

		// Create a composite key using symbol and roundedTime
		key := symbol + roundedTime.Format(time.RFC3339)

		// Check if the current interval has started
		if _, exists := resampledData[key]; !exists {
			// Create a new OHLCV entry for the next targetTimeFrame interval
			resampledData[key] = map[string]interface{}{"time": roundedTime, "high": entry["high"], "low": entry["low"], "close": entry["close"], "volume": entry["volume"], "symbol": symbol}
		} else {
			// Update the current OHLCV with the new data
			lastOHLCV := resampledData[key]
			lastOHLCV["high"] = max(lastOHLCV["high"].(float64), entry["high"].(float64))
			lastOHLCV["low"] = min(lastOHLCV["low"].(float64), entry["low"].(float64))
			lastOHLCV["close"] = entry["close"]

			// Cast existing volume to float64 before updating
			existingVolume, _ := lastOHLCV["volume"].(float64)
			lastOHLCV["volume"] = existingVolume + entry["volume"].(float64)
		}
	}

	// Convert the map to a slice
	resampledSlice := make([]map[string]interface{}, 0, len(resampledData))
	for _, value := range resampledData {
		resampledSlice = append(resampledSlice, value)
	}

	return resampledSlice
}

func roundToInterval(t time.Time, interval time.Duration) time.Time {
	return t.Add(-time.Duration(t.UnixNano()) % interval)
}
func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func CalculatePercentageDifference(a, b float64) float64 {
	if a == 0 {
		// Avoid division by zero
		return 0
	}

	percentageDifference := ((b - a) / a)
	return percentageDifference
}

func CalculateMovingAverage(values []float64, window int) []float64 {
	if len(values) == 0 || window <= 0 {
		return nil
	}

	var maValues []float64

	for i := 0; i < len(values)-window+1; i++ {
		sum := 0.0
		for j := i; j < i+window; j++ {
			sum += values[j]
		}
		maValues = append(maValues, sum/float64(window))
	}

	return maValues
}

func IsMultipleOf5Minutes() bool {
	currentTime := time.Now()
	currentMinute := currentTime.Minute()

	// Check if the current minute is a multiple of 5
	return currentMinute%5 == 0
}

func Convert_ts_to_tz(x int64, tz string) (time.Time, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.Time{}, err
	}

	// Divide x por 1000 para obter os segundos e usa o resto para os milissegundos
	timestampSeconds := x / 1000
	millisRemainder := x % 1000

	// Converte os milissegundos em nanossegundos para o segundo parâmetro de time.Unix
	t := time.Unix(timestampSeconds, millisRemainder*1000000).In(loc)

	return t, nil
}

func ArredondarParaMinuto(timestamp time.Time) time.Time {
	ano, mes, dia := timestamp.Date()
	hora, minuto, _ := timestamp.Clock()
	loc := timestamp.Location()

	// Arredondar para o início do minuto
	timestampArredondado := time.Date(ano, mes, dia, hora, minuto, 0, 0, loc)

	return timestampArredondado
}

func StructToMap(s interface{}) map[string]interface{} {
	valorStruct := reflect.ValueOf(s)

	if valorStruct.Kind() == reflect.Ptr {
		valorStruct = valorStruct.Elem()
	}

	tipoStruct := valorStruct.Type()
	mapa := make(map[string]interface{})

	for i := 0; i < valorStruct.NumField(); i++ {
		campo := valorStruct.Field(i)
		nomeCampo := tipoStruct.Field(i).Name
		valorCampo := campo.Interface()

		if campo.Kind() == reflect.Struct {
			// Se o campo for uma struct, chame a função recursivamente
			mapa[nomeCampo] = StructToMap(valorCampo)
		} else {
			mapa[nomeCampo] = valorCampo
		}
	}

	return mapa
}

func CalculateElapsedTime(inicio time.Time) string {
	fim := time.Now()
	tempoDecorrido := fim.Sub(inicio).Milliseconds()
	//formattedTime := fmt.Sprintf("%.3f", tempoDecorrido)
	formattedTime := fmt.Sprintf("%v", tempoDecorrido)
	return formattedTime
}

func keepRecordsLastxMinutes(data []map[string]interface{}, minutes float64) []map[string]interface{} {
	results := []map[string]interface{}{}
	now := time.Now()

	for _, record := range data {
		eventTime, ok := record["event_time"].(time.Time)
		if !ok {
			// Ignore records without a valid "event_time" field
			continue
		}

		// Calculate the difference in minutes between the current time and the "event_time"
		diff := now.Sub(eventTime).Minutes()

		if diff <= minutes {
			results = append(results, record)
		}
	}

	return results
}
func CalculateZScores(values []float64) []float64 {
	mean, stdDev := calculateMeanAndStdDev(values)
	zScores := calculateZScores(values, mean, stdDev)
	return zScores
}

func calculateMeanAndStdDev(values []float64) (float64, float64) {
	sum := 0.0
	n := float64(len(values))

	// Calculate mean
	for _, value := range values {
		sum += value
	}
	mean := sum / n

	// Calculate standard deviation
	variance := 0.0
	for _, value := range values {
		variance += math.Pow(value-mean, 2)
	}
	stdDev := math.Sqrt(variance / n)

	return mean, stdDev
}

func calculateZScores(values []float64, mean, stdDev float64) []float64 {
	zScores := make([]float64, len(values))
	for i, value := range values {
		zScores[i] = (value - mean) / stdDev
	}
	return zScores
}
func Quantile(data []float64, p float64) float64 {
	if len(data) == 0 {
		return 0
	}

	// Sort the data
	sort.Float64s(data)

	// Calculate the index for the specified quantile
	index := p * (float64(len(data)) - 1)
	lower := int(index)
	upper := lower + 1

	// Check if upper index is within bounds
	if upper >= len(data) {
		upper = len(data) - 1
	}

	// Interpolate between the two nearest data points
	fracPart := index - float64(lower)
	return data[lower] + fracPart*(data[upper]-data[lower])
}

func KeepHistoryMinute(data []map[string]interface{}, keepMinutes float64) []map[string]interface{} {
	// Use a map to track the most recent record for each minute and symbol
	latestRecords := make(map[string]map[string]interface{})

	// Iterate over the data and update the records
	for _, record := range data {
		symbol := record["symbol"].(string)
		eventTime, ok := record["event_time"].(time.Time)
		if !ok {
			// Ignore records without a valid "event_time" field
			continue
		}
		timeField, ok := record["time"].(time.Time)
		if !ok {
			// Ignore records without a valid "event_time" field
			continue
		}

		// Generate a key based on symbol, minute, and truncate seconds
		key := fmt.Sprintf("%s_%s", symbol, timeField.Format("2006-01-02 15:04"))

		// Check if the record already exists and if the new record is more recent
		if latestRecord, ok := latestRecords[key]; !ok || eventTime.After(latestRecord["event_time"].(time.Time)) {
			latestRecords[key] = record
		}
	}

	// Convert the map of records to a slice of maps
	var results []map[string]interface{}
	for _, record := range latestRecords {
		results = append(results, record)
	}

	results = keepRecordsLastxMinutes(results, keepMinutes)

	return results
}

func GetMaxTime(data []map[string]interface{}) (maxTime time.Time, err error) {
	for _, entry := range data {
		// Assuming "time" key is present in each map
		timeValue, ok := entry["time"].(time.Time)
		if !ok {
			return maxTime, fmt.Errorf("failed to convert time to time.Time")
		}

		if timeValue.After(maxTime) {
			maxTime = timeValue
		}
	}

	return maxTime, nil
}

func HasDuplicate(data []map[string]interface{}, keys ...string) bool {
	seen := make(map[string]map[interface{}]interface{})

	for _, key := range keys {
		seen[key] = make(map[interface{}]interface{})
	}

	for _, entry := range data {
		var keyValues []interface{}

		for _, key := range keys {
			val, ok := entry[key]
			if !ok {
				// If a map doesn't contain the specified key, consider it a duplicate
				return true
			}

			keyValues = append(keyValues, val)
		}

		if _, exists := seen[fmt.Sprint(keyValues...)]; exists {
			// If the key combination has already been seen, consider it a duplicate
			return true
		}

		seen[fmt.Sprint(keyValues...)] = make(map[interface{}]interface{})
	}

	// No duplicates found
	return false
}

func GetMinTime(data []map[string]interface{}) (minTime time.Time, err error) {
	if len(data) == 0 {
		return minTime, errors.New("empty data slice")
	}

	// Initialize minTime with the time from the first entry
	minTime, ok := data[0]["time"].(time.Time)
	if !ok {
		return minTime, fmt.Errorf("failed to convert time to time.Time")
	}

	// Iterate over the remaining entries
	for _, entry := range data[1:] {
		// Assuming "time" key is present in each map
		timeValue, ok := entry["time"].(time.Time)
		if !ok {
			return minTime, fmt.Errorf("failed to convert time to time.Time")
		}

		if timeValue.Before(minTime) {
			minTime = timeValue
		}
	}

	return minTime, nil
}
