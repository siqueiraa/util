package util

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"
)

func CalculatePercentageDifference(a, b float64) float64 {
	if a == 0 {
		// Avoid division by zero
		return 0
	}

	percentageDifference := ((b - a) / a) * 100
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

	timestampSeconds := x / 1000
	t := time.Unix(timestampSeconds, 0).In(loc)

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

func keepRecordsLast5Minutes(data []map[string]interface{}) []map[string]interface{} {
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

		if diff <= 2 {
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

func frac(x float64) float64 {
	return x - float64(int(x))
}

func KeepHistoryMinute(data []map[string]interface{}) []map[string]interface{} {
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

	results = keepRecordsLast5Minutes(results)

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
