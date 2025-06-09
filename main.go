package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

var (
	mem runtime.MemStats

	wait sync.WaitGroup
)

func reportProcessMemory() {
	pid := os.Getpid()
	var ru syscall.Rusage
	err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	if err == nil {
		fmt.Printf("Max RSS: %d KB\n", ru.Maxrss)
	}

	// Or read from /proc (Linux only)
	statm := fmt.Sprintf("/proc/%d/statm", pid)
	data, err := os.ReadFile(statm)
	if err == nil {
		fmt.Printf("/proc/%d/statm: %s\n", pid, data)
	}
}

func estimateParquetSize(db *sql.DB, file string) {
	query := fmt.Sprintf(`
		SELECT
			SUM(total_uncompressed_size),
			SUM(total_compressed_size),
			SUM(num_values) / COUNT(DISTINCT column_id)
		FROM parquet_metadata('%s');`, file)

	row := db.QueryRow(query)

	var uncompressed, compressed, estRows float64
	if err := row.Scan(&uncompressed, &compressed, &estRows); err != nil {
		log.Printf("Error querying metadata for %s: %v", file, err)
		return
	}

	fmt.Printf("File: %s\nCompressed: %.2f MB | Uncompressed: %.2f MB | Estimated Rows: %.0f\n",
		file,
		compressed/1024/1024,
		uncompressed/1024/1024,
		estRows)
}

func logMemoryUsage(stage string) {
	runtime.ReadMemStats(&mem)
	fmt.Printf("[%s] Go heap: mallocs(%d), heapallocs(%d)\r\n", stage, mem.Mallocs, mem.Frees)
}

func checkSize(db *sql.DB) {
	// Run memory usage query
	rows, err := db.Query(`SELECT * FROM duckdb_memory();`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}
	fmt.Println("Columns:", cols)

	for rows.Next() {
		var tag string
		var memBytes int64
		var tempBytes int64

		if err := rows.Scan(&tag, &memBytes, &tempBytes); err != nil {
			log.Fatalf("Failed to scan: %v", err)
		}
		fmt.Printf("%-30s : %10d bytes (%.2f MB) | Temp: %10d bytes\n",
			tag, memBytes, float64(memBytes)/1024/1024, tempBytes)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Row iteration error: %v", err)
	}
}

func main() {

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer db.Close()

	parquetFile := "heavy_expand.parquet"
	//	tableName := "my_loaded_table"
	go func() {
		logMemoryUsage("Before loading")
		fmt.Println("Loading Parquet file into DuckDB...")
		estimateParquetSize(db, parquetFile)
		//ivz	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM '%s'", tableName, parquetFile))
		//ivz	if err != nil {
		//ivz		log.Fatalf("Failed to load Parquet: %v", err)
		//ivz	}
	}()

	//		time.Sleep(1 * time.Second)
	go func() {
		for {
			checkSize(db)
			time.Sleep(1 * time.Second)
		}
	}()
	select {}
}
