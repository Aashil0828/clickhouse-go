package query

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"math/rand"
	"strings"
	"time"
)

const (
	address = "localhost:9000"

	databaseName = "clickhouse_go"

	startTime = "2024-12-29 12:00:00"

	endTime = "2024-12-30 12:00:00"

	systemCpuPercent = "system_cpu_percent"

	systemMemoryFreeBytes = "system_memory_free_bytes"

	pingMinLatencyMS = "ping_min_latency_ms"

	systemMemoryInstalledBytes = "system_memory_installed_bytes"

	createTableQuery = `
		CREATE TABLE IF NOT EXISTS metric_data 
		(
		monitor_id UInt32,
		metric String,
		timestamp DateTime,
		value Float32
		)
		ENGINE = MergeTree()
		PRIMARY KEY (monitor_id, metric, timestamp)
	`

	insertQuery = `
		INSERT INTO metric_data(monitor_id, metric, timestamp, value) VALUES
	`

	gaugeWithoutFilterQuery = `
		SELECT avg(value) as avg_system_cpu_percent
		FROM metric_data
		WHERE metric = 'system_cpu_percent'
	`

	gaugeWithFilterQuery = `
		SELECT avg(value) as avg_system_cpu_percent
		FROM metric_data
		WHERE metric = 'system_cpu_percent'
		AND monitor_id in (1, 4, 6)
	`

	gridWithFilterQuery = `
		SELECT 
		    monitor_id,
		    avgIf(value, metric='system_cpu_percent') as avg_system_cpu_percent,
			avgIf(value, metric='system_memory_free_bytes') as avg_system_memory_free_bytes
		FROM metric_data
		WHERE metric IN ('system_cpu_percent', 'system_memory_free_bytes')
		AND monitor_id in (1, 4, 6)
		GROUP BY monitor_id
	`

	gridWithoutFilterQuery = `
		SELECT 
		    monitor_id,
		    avgIf(value, metric='system_cpu_percent') as avg_system_cpu_percent,
			avgIf(value, metric='system_memory_free_bytes') as avg_system_memory_free_bytes
		FROM metric_data
		WHERE metric IN ('system_cpu_percent', 'system_memory_free_bytes')
		GROUP BY monitor_id
	`

	topNWithFilterQuery = `
		SELECT 
		    monitor_id,
		    avgIf(value, metric='system_cpu_percent') as avg_system_cpu_percent,
			avgIf(value, metric='system_memory_free_bytes') as avg_system_memory_free_bytes
		FROM metric_data
		WHERE metric IN ('system_cpu_percent', 'system_memory_free_bytes')
		AND monitor_id in (1, 4, 6, 8, 10)
		GROUP BY monitor_id
		ORDER BY 
		    avg_system_cpu_percent desc, 
		    avg_system_memory_free_bytes desc
		LIMIT ?
	`

	topNWithoutFilterQuery = `
		SELECT 
		    monitor_id,
		    avgIf(value, metric='system_cpu_percent') as avg_system_cpu_percent,
			avgIf(value, metric='system_memory_free_bytes') as avg_system_memory_free_bytes
		FROM metric_data
		WHERE metric IN ('system_cpu_percent', 'system_memory_free_bytes')
		GROUP BY monitor_id
		ORDER BY 
		    avg_system_cpu_percent desc, 
		    avg_system_memory_free_bytes desc
		LIMIT ?
	`

	histogramWithFilterWithoutGroupByQuery = `
		SELECT 
		    timestamp,
		    avgIf(value, metric='system_cpu_percent') as avg_system_cpu_percent,
			avgIf(value, metric='system_memory_free_bytes') as avg_system_memory_free_bytes
		FROM metric_data
		WHERE metric IN ('system_cpu_percent', 'system_memory_free_bytes')
		AND monitor_id in (1, 4, 6)
		GROUP BY timestamp
		ORDER BY timestamp
		LIMIT 20
	`

	histogramWithoutFilterWithoutGroupByQuery = `
		SELECT
		    timestamp,
		    avgIf(value, metric='system_cpu_percent') as avg_system_cpu_percent,
			avgIf(value, metric='system_memory_free_bytes') as avg_system_memory_free_bytes
		FROM metric_data
		WHERE metric IN ('system_cpu_percent', 'system_memory_free_bytes')
		GROUP BY timestamp
		ORDER BY timestamp
		LIMIT 20
	`

	histogramWithFilterWithGroupByQuery = `
		SELECT
		    monitor_id,
		    timestamp, 
		    avgIf(value, metric='system_cpu_percent') as avg_system_cpu_percent,
			avgIf(value, metric='system_memory_free_bytes') as avg_system_memory_free_bytes
		FROM metric_data
		WHERE metric IN ('system_cpu_percent', 'system_memory_free_bytes')
		AND monitor_id in (1, 4, 6)
		GROUP BY timestamp, monitor_id
		ORDER BY monitor_id, timestamp
		LIMIT 20
	`

	histogramWithoutFilterWithGroupByQuery = `
		SELECT
		    monitor_id,
		    timestamp, 
		    avgIf(value, metric='system_cpu_percent') as avg_system_cpu_percent,
			avgIf(value, metric='system_memory_free_bytes') as avg_system_memory_free_bytes
		FROM metric_data
		WHERE metric IN ('system_cpu_percent', 'system_memory_free_bytes')
		GROUP BY timestamp, monitor_id
		ORDER BY monitor_id, timestamp
		LIMIT 20
	`
)

var (
	monitors = 10

	metrics = []string{

		systemCpuPercent,

		systemMemoryFreeBytes,

		pingMinLatencyMS,

		systemMemoryInstalledBytes,
	}

	timestamp time.Time
)

func InitClickhouseDBConnection(ctx context.Context) (connection clickhouse.Conn, err error) {

	connection, err = clickhouse.Open(&clickhouse.Options{

		Addr: []string{address},
	})

	if err != nil {

		return nil, err

	}

	err = connection.Ping(ctx)

	if err != nil {

		return nil, err

	}

	err = connection.Exec(ctx, "use "+databaseName)

	if err != nil {

		return nil, err

	}

	return connection, nil

}

func CreateTable(ctx context.Context, connection clickhouse.Conn) (err error) {

	return connection.Exec(ctx, createTableQuery)

}

func InsertData(ctx context.Context, connection clickhouse.Conn) (err error) {

	start, err := time.Parse(time.DateTime, startTime)

	if err != nil {

		return err

	}

	end, err := time.Parse(time.DateTime, endTime)

	if err != nil {

		return err

	}

	var queryBuilder strings.Builder

	queryBuilder.WriteString(insertQuery)

	values := make([]interface{}, 0, 4*288)

	for monitorID := range monitors {

		for _, metric := range metrics {

			timestamp = start

			for timestamp.Before(end) {

				queryBuilder.WriteString(" (?, ?, ?, ?),")

				values = append(values, monitorID, metric, timestamp, 100*rand.Float64())

				timestamp = timestamp.Add(5 * time.Minute)

			}

		}

	}

	query := strings.TrimRight(queryBuilder.String(), ",")

	err = connection.Exec(ctx, query, values...)

	return err

}

func GaugeWithoutFilter(ctx context.Context, connection clickhouse.Conn) (err error) {

	fmt.Println("--------------------- Gauge Without Filter ------------------------")

	fmt.Println()

	rows, err := connection.Query(ctx, gaugeWithoutFilterQuery)

	if err != nil {

		return err
	}

	defer rows.Close()

	fmt.Println("avg_" + systemCpuPercent)

	for rows.Next() {

		var avgValue float64

		err = rows.Scan(&avgValue)

		if err != nil {

			return err

		}

		fmt.Println(avgValue)

	}

	fmt.Println()

	return nil

}

func GaugeWithFilter(ctx context.Context, connection clickhouse.Conn) (err error) {

	fmt.Println("--------------------- Gauge With Filter ------------------------")

	fmt.Println()

	rows, err := connection.Query(ctx, gaugeWithFilterQuery)

	if err != nil {

		return err
	}

	defer rows.Close()

	fmt.Println("avg_" + systemCpuPercent)

	for rows.Next() {

		var avgValue float64

		err = rows.Scan(&avgValue)

		if err != nil {

			return err

		}

		fmt.Println(avgValue)

	}

	fmt.Println()

	return nil

}

func GridWithFilter(ctx context.Context, connection clickhouse.Conn) (err error) {

	fmt.Println("--------------------- Grid With Filter ------------------------")

	fmt.Println()

	rows, err := connection.Query(ctx, gridWithFilterQuery)

	if err != nil {

		return err
	}

	defer rows.Close()

	fmt.Println("monitor_id" + " | " + "avg_" + systemCpuPercent + " | " + "avg_" + systemMemoryFreeBytes)

	for rows.Next() {

		var monitorId uint32

		var avgSystemCpuPercent float64

		var avgSystemMemoryFreeBytes float64

		err = rows.Scan(&monitorId, &avgSystemCpuPercent, &avgSystemMemoryFreeBytes)

		if err != nil {

			return err

		}

		fmt.Println(fmt.Sprintf("%v    |   %v  |   %v   ", monitorId, avgSystemCpuPercent, avgSystemMemoryFreeBytes))

	}

	fmt.Println()

	return nil

}

func GridWithoutFilter(ctx context.Context, connection clickhouse.Conn) (err error) {

	fmt.Println("--------------------- Grid Without Filter ------------------------")

	fmt.Println()

	rows, err := connection.Query(ctx, gridWithoutFilterQuery)

	if err != nil {

		return err
	}

	defer rows.Close()

	fmt.Println("monitor_id" + " | " + "avg_" + systemCpuPercent + " | " + "avg_" + systemMemoryFreeBytes)

	for rows.Next() {

		var monitorId uint32

		var avgSystemCpuPercent float64

		var avgSystemMemoryFreeBytes float64

		err = rows.Scan(&monitorId, &avgSystemCpuPercent, &avgSystemMemoryFreeBytes)

		if err != nil {

			return err

		}

		fmt.Println(fmt.Sprintf("%v    |   %v  |   %v   ", monitorId, avgSystemCpuPercent, avgSystemMemoryFreeBytes))

	}

	fmt.Println()

	return nil

}

func TopNWithoutFilter(ctx context.Context, connection clickhouse.Conn, num int) (err error) {

	fmt.Printf("--------------------- Top %v Without Filter ------------------------\n", num)

	fmt.Println()

	rows, err := connection.Query(ctx, topNWithoutFilterQuery, num)

	if err != nil {

		return err
	}

	defer rows.Close()

	fmt.Println("monitor_id" + " | " + "avg_" + systemCpuPercent + " | " + "avg_" + systemMemoryFreeBytes)

	for rows.Next() {

		var monitorId uint32

		var avgSystemCpuPercent float64

		var avgSystemMemoryFreeBytes float64

		err = rows.Scan(&monitorId, &avgSystemCpuPercent, &avgSystemMemoryFreeBytes)

		if err != nil {

			return err

		}

		fmt.Println(fmt.Sprintf("%v    |   %v  |   %v   ", monitorId, avgSystemCpuPercent, avgSystemMemoryFreeBytes))

	}

	fmt.Println()

	return nil

}

func TopNWithFilter(ctx context.Context, connection clickhouse.Conn, num int) (err error) {

	fmt.Printf("--------------------- Top %v With Filter ------------------------\n", num)

	fmt.Println()

	rows, err := connection.Query(ctx, topNWithFilterQuery, num)

	if err != nil {

		return err
	}

	defer rows.Close()

	fmt.Println("monitor_id" + " | " + "avg_" + systemCpuPercent + " | " + "avg_" + systemMemoryFreeBytes)

	for rows.Next() {

		var monitorId uint32

		var avgSystemCpuPercent float64

		var avgSystemMemoryFreeBytes float64

		err = rows.Scan(&monitorId, &avgSystemCpuPercent, &avgSystemMemoryFreeBytes)

		if err != nil {

			return err

		}

		fmt.Println(fmt.Sprintf("%v    |   %v  |   %v   ", monitorId, avgSystemCpuPercent, avgSystemMemoryFreeBytes))

	}

	fmt.Println()

	return nil

}

func HistogramWithFilterWithoutGroupBy(ctx context.Context, connection clickhouse.Conn) (err error) {

	fmt.Println("--------------------- Histogram With Filter Without GroupBy ------------------------")

	fmt.Println()

	rows, err := connection.Query(ctx, histogramWithFilterWithoutGroupByQuery)

	if err != nil {

		return err
	}

	defer rows.Close()

	fmt.Println("timestamp" + " | " + "avg_" + systemCpuPercent + " | " + "avg_" + systemMemoryFreeBytes)

	for rows.Next() {

		var timestamp time.Time

		var avgSystemCpuPercent float64

		var avgSystemMemoryFreeBytes float64

		err = rows.Scan(&timestamp, &avgSystemCpuPercent, &avgSystemMemoryFreeBytes)

		if err != nil {

			return err

		}

		fmt.Println(fmt.Sprintf("%v    |   %v  |   %v   ", timestamp.UTC().String(), avgSystemCpuPercent, avgSystemMemoryFreeBytes))

	}

	fmt.Println()

	return nil

}

func HistogramWithoutFilterWithoutGroupBy(ctx context.Context, connection clickhouse.Conn) (err error) {

	fmt.Println("--------------------- Histogram Without Filter Without GroupBy ------------------------")

	fmt.Println()

	rows, err := connection.Query(ctx, histogramWithoutFilterWithoutGroupByQuery)

	if err != nil {

		return err
	}

	defer rows.Close()

	fmt.Println("timestamp" + " | " + "avg_" + systemCpuPercent + " | " + "avg_" + systemMemoryFreeBytes)

	for rows.Next() {

		var timestamp time.Time

		var avgSystemCpuPercent float64

		var avgSystemMemoryFreeBytes float64

		err = rows.Scan(&timestamp, &avgSystemCpuPercent, &avgSystemMemoryFreeBytes)

		if err != nil {

			return err

		}

		fmt.Println(fmt.Sprintf("%v    |   %v  |   %v   ", timestamp.UTC().String(), avgSystemCpuPercent, avgSystemMemoryFreeBytes))

	}

	fmt.Println()

	return nil

}

func HistogramWithFilterWithGroupBy(ctx context.Context, connection clickhouse.Conn) (err error) {

	fmt.Println("--------------------- Histogram With Filter With GroupBy ------------------------")

	fmt.Println()

	rows, err := connection.Query(ctx, histogramWithFilterWithGroupByQuery)

	if err != nil {

		return err
	}

	defer rows.Close()

	fmt.Println("monitor_id" + " | " + "timestamp" + " | " + "avg_" + systemCpuPercent + " | " + "avg_" + systemMemoryFreeBytes)

	for rows.Next() {

		var monitorId uint32

		var timestamp time.Time

		var avgSystemCpuPercent float64

		var avgSystemMemoryFreeBytes float64

		err = rows.Scan(&monitorId, &timestamp, &avgSystemCpuPercent, &avgSystemMemoryFreeBytes)

		if err != nil {

			return err

		}

		fmt.Println(fmt.Sprintf("%v    |    %v    |   %v  |   %v   ", monitorId, timestamp.UTC().String(), avgSystemCpuPercent, avgSystemMemoryFreeBytes))

	}

	fmt.Println()

	return nil

}

func HistogramWithoutFilterWithGroupBy(ctx context.Context, connection clickhouse.Conn) (err error) {

	fmt.Println("--------------------- Histogram Without Filter With GroupBy ------------------------")

	fmt.Println()

	rows, err := connection.Query(ctx, histogramWithoutFilterWithGroupByQuery)

	if err != nil {

		return err
	}

	defer rows.Close()

	fmt.Println("monitor_id" + " | " + "timestamp" + " | " + "avg_" + systemCpuPercent + " | " + "avg_" + systemMemoryFreeBytes)

	for rows.Next() {

		var monitorId uint32

		var timestamp time.Time

		var avgSystemCpuPercent float64

		var avgSystemMemoryFreeBytes float64

		err = rows.Scan(&monitorId, &timestamp, &avgSystemCpuPercent, &avgSystemMemoryFreeBytes)

		if err != nil {

			return err

		}

		fmt.Println(fmt.Sprintf("%v    |    %v    |   %v  |   %v   ", monitorId, timestamp.UTC().String(), avgSystemCpuPercent, avgSystemMemoryFreeBytes))

	}

	fmt.Println()

	return nil

}
