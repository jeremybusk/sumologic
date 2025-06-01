package main

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/pflag"
)

// Constants
const (
	protocol             = "https"
	defaultSumoHost      = "api.us2.sumologic.com"
	searchJobResultsLimit = 10000
	defaultMaxConcurrentJobs = 1
	apiRateLimitDelay    = 2 * time.Second
	messageLimit         = 199999
	minSplitMinutes      = 1 * time.Minute
	defaultDiscoverTime  = "2024-03-01T15:00:00Z"
)

var (
	sumoAPIURL      string
	apiAccessID     string
	apiAccessKey    string
	writeEmptyFiles bool
	dbConn          *sql.DB
	dbLock          sync.Mutex
	currentLogLevel string
)

// Message represents a Sumo Logic message structure for JSON unmarshalling.
type Message struct {
	Map map[string]interface{} `json:"map"`
}

// SearchJobPayload represents the payload for creating a Sumo Logic search job.
type SearchJobPayload struct {
	Query    string `json:"query"`
	From     string `json:"from"`
	To       string `json:"to"`
	TimeZone string `json:"timeZone"`
}

// SearchJobResponse represents the response from creating a Sumo Logic search job.
type SearchJobResponse struct {
	ID string `json:"id"`
}

// SearchJobStatusResponse represents the response from checking a Sumo Logic search job status.
type SearchJobStatusResponse struct {
	State string `json:"state"`
}

func init() {
	host := os.Getenv("SUMO_HOST")
	if host == "" {
		host = defaultSumoHost
	}
	sumoAPIURL = fmt.Sprintf("%s://%s/api/v1/search/jobs", protocol, host)
	apiAccessID = os.Getenv("SUMO_ACCESS_ID")
	apiAccessKey = os.Getenv("SUMO_ACCESS_KEY")
}

// --- Logging ---

func configureLogging(logfile string, level string) {
	currentLogLevel = level
	logFile, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	logInfo("Logging initialized.")
}

func logDebug(format string, v ...interface{}) {
	if currentLogLevel == "DEBUG" {
		log.Printf("[DEBUG] "+format, v...)
	}
}

func logInfo(format string, v ...interface{}) {
	if currentLogLevel == "DEBUG" || currentLogLevel == "INFO" {
		log.Printf("[INFO] "+format, v...)
	}
}

func logWarn(format string, v ...interface{}) {
	if currentLogLevel == "DEBUG" || currentLogLevel == "INFO" || currentLogLevel == "WARN" {
		log.Printf("[WARN] "+format, v...)
	}
}

func logError(format string, v ...interface{}) {
	log.Printf("[ERROR] "+format, v...)
}

func logFatal(format string, v ...interface{}) {
	log.Fatalf("[FATAL] "+format, v...)
}

// --- DB Functions ---

func initDB(dbPath string) {
	var err error
	dbConn, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		logFatal("Failed to open SQLite database: %v", err)
	}

	dbLock.Lock()
	defer dbLock.Unlock()

	_, err = dbConn.Exec(`
		CREATE TABLE IF NOT EXISTS message_counts (
			query TEXT,
			timestamp TEXT,
			message_count INTEGER
		)
	`)
	if err != nil {
		logFatal("Failed to create message_counts table: %v", err)
	}

	_, err = dbConn.Exec(`
		CREATE TABLE IF NOT EXISTS query_splits (
			query TEXT,
			original_start TEXT,
			original_end TEXT,
			sub_start TEXT,
			sub_end TEXT
		)
	`)
	if err != nil {
		logFatal("Failed to create query_splits table: %v", err)
	}
	logInfo("Database initialized at %s", dbPath)
}

func saveMessageCounts(query string, messages []Message) {
	if dbConn == nil {
		return
	}

	grouped := make(map[string]int)
	for _, msg := range messages {
		if tsVal, ok := msg.Map["_messagetime"]; ok {
			var ts int64
			var parseErr error

			switch v := tsVal.(type) {
			case float64:
				ts = int64(v)
			case string:
				ts, parseErr = strconv.ParseInt(v, 10, 64)
				if parseErr != nil {
					logWarn("Failed to parse _messagetime string '%s' to int64 for message counts: %v", v, parseErr)
					continue
				}
			case json.Number:
				ts, parseErr = v.Int64()
				if parseErr != nil {
					logWarn("Failed to parse _messagetime json.Number '%s' to int64 for message counts: %v", v.String(), parseErr)
					continue
				}
			default:
				logWarn("'_messagetime' has unexpected type %T for message counts: %v", tsVal, tsVal)
				continue
			}
			dt := time.Unix(ts/1000, (ts%1000)*1000000).In(time.UTC).Truncate(time.Minute)
			grouped[dt.Format(time.RFC3339)]++
		}
	}

	dbLock.Lock()
	defer dbLock.Unlock()

	tx, err := dbConn.Begin()
	if err != nil {
		logError("Failed to begin transaction for message counts: %v", err)
		return
	}
	stmt, err := tx.Prepare("INSERT INTO message_counts (query, timestamp, message_count) VALUES (?, ?, ?)")
	if err != nil {
		logError("Failed to prepare statement for message counts: %v", err)
		tx.Rollback() // nolint
		return
	}
	defer stmt.Close()

	for minute, count := range grouped {
		_, err := stmt.Exec(query, minute, count)
		if err != nil {
			logError("Failed to insert message count for %s: %v", minute, err)
			tx.Rollback() // nolint
			return
		}
	}
	err = tx.Commit()
	if err != nil {
		logError("Failed to commit transaction for message counts: %v", err)
	}
}

func logQuerySplit(query string, originalStart, originalEnd, subStart, subEnd time.Time) {
	if dbConn == nil {
		return
	}
	dbLock.Lock()
	defer dbLock.Unlock()

	_, err := dbConn.Exec(`INSERT INTO query_splits
		(query, original_start, original_end, sub_start, sub_end)
		VALUES (?, ?, ?, ?, ?)`,
		query, originalStart.Format(time.RFC3339), originalEnd.Format(time.RFC3339),
		subStart.Format(time.RFC3339), subEnd.Format(time.RFC3339))
	if err != nil {
		logError("Failed to log query split: %v", err)
	}
}

// --- Core Functions ---

func ensureDirectoryExists(path string) error {
	return os.MkdirAll(path, 0o755)
}

func createSearchJob(query string, startTime, endTime time.Time) (string, error) {
	payload := SearchJobPayload{
		Query:    query,
		From:     startTime.Format(time.RFC3339Nano),
		To:       endTime.Format(time.RFC3339Nano),
		TimeZone: "UTC",
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal search job payload: %w", err)
	}

	for {
		req, err := http.NewRequest("POST", sumoAPIURL, bytes.NewBuffer(payloadBytes))
		if err != nil {
			return "", fmt.Errorf("failed to create request: %w", err)
		}
		req.SetBasicAuth(apiAccessID, apiAccessKey)
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return "", fmt.Errorf("failed to make request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			logWarn("Rate limit hit when creating search job. Sleeping for %v before retry...", apiRateLimitDelay)
			time.Sleep(apiRateLimitDelay)
			continue
		}

		if resp.StatusCode != http.StatusAccepted {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return "", fmt.Errorf("failed to create search job, status: %s, body: %s", resp.Status, bodyBytes)
		}

		var jobResponse SearchJobResponse
		if err := json.NewDecoder(resp.Body).Decode(&jobResponse); err != nil {
			return "", fmt.Errorf("failed to decode search job response: %w", err)
		}
		return jobResponse.ID, nil
	}
}

func waitForJobCompletion(jobID string) error {
	statusURL := fmt.Sprintf("%s/%s", sumoAPIURL, jobID)
	for {
		req, err := http.NewRequest("GET", statusURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create status request: %w", err)
		}
		req.SetBasicAuth(apiAccessID, apiAccessKey)

		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to make status request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			logWarn("Rate limit hit when checking job status. Retrying status check in %v...", apiRateLimitDelay)
			time.Sleep(apiRateLimitDelay)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to get search job status, status: %s, body: %s", resp.Status, bodyBytes)
		}

		var statusResponse SearchJobStatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&statusResponse); err != nil {
			return fmt.Errorf("failed to decode search job status response: %w", err)
		}

		switch statusResponse.State {
		case "DONE GATHERING RESULTS":
			return nil
		case "CANCELLED", "FAILED":
			return fmt.Errorf("search job %s failed with state: %s", jobID, statusResponse.State)
		default:
			time.Sleep(5 * time.Second)
		}
	}
}

func fetchAllMessages(jobID string) ([]Message, error) {
	var allMessages []Message
	offset := 0
	for {
		messagesURL := fmt.Sprintf("%s/%s/messages?limit=%d&offset=%d", sumoAPIURL, jobID, searchJobResultsLimit, offset)
		req, err := http.NewRequest("GET", messagesURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create messages request: %w", err)
		}
		req.SetBasicAuth(apiAccessID, apiAccessKey)

		client := &http.Client{Timeout: 60 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to make messages request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			logWarn("Rate limit hit fetching messages. Retrying in %v...", apiRateLimitDelay)
			time.Sleep(apiRateLimitDelay)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("failed to fetch messages, status: %s, body: %s", resp.Status, bodyBytes)
		}

		var responseData struct {
			Messages []Message `json:"messages"`
		}
		decoder := json.NewDecoder(resp.Body)
		decoder.UseNumber()
		if err := decoder.Decode(&responseData); err != nil {
			return nil, fmt.Errorf("failed to decode messages response: %w", err)
		}

		allMessages = append(allMessages, responseData.Messages...)
		if len(responseData.Messages) < searchJobResultsLimit {
			break
		}
		offset += searchJobResultsLimit
	}
	return allMessages, nil
}

func saveMessagesByMinute(messages []Message, outputDir string) {
	messagesByMinute := make(map[time.Time][]Message)
	for _, message := range messages {
		currentMap := make(map[string]interface{})
		for k, v := range message.Map {
			currentMap[k] = v
		}

		if tsVal, ok := currentMap["_messagetime"]; ok {
			var ts int64
			var parseErr error

			switch v := tsVal.(type) {
			case float64:
				ts = int64(v)
			case string:
				ts, parseErr = strconv.ParseInt(v, 10, 64)
				if parseErr != nil {
					logWarn("Failed to parse _messagetime string '%s' to int64: %v", v, parseErr)
					messagesByMinute[time.Time{}] = append(messagesByMinute[time.Time{}], Message{Map: currentMap})
					continue
				}
			case json.Number:
				ts, parseErr = v.Int64()
				if parseErr != nil {
					logWarn("Failed to parse _messagetime json.Number '%s' to int64: %v", v.String(), parseErr)
					messagesByMinute[time.Time{}] = append(messagesByMinute[time.Time{}], Message{Map: currentMap})
					continue
				}
			default:
				logWarn("'_messagetime' has unexpected type %T: %v", tsVal, tsVal)
				messagesByMinute[time.Time{}] = append(messagesByMinute[time.Time{}], Message{Map: currentMap})
				continue
			}

			t := time.Unix(ts/1000, (ts%1000)*1000000).In(time.UTC)
			currentMap["_messagetime"] = t.Format(time.RFC3339Nano)

			dt := t.Truncate(time.Minute)
			messagesByMinute[dt] = append(messagesByMinute[dt], Message{Map: currentMap})
		} else {
			logWarn("'_messagetime' key not found in message map")
			messagesByMinute[time.Time{}] = append(messagesByMinute[time.Time{}], Message{Map: currentMap})
		}
	}

	for minute, msgs := range messagesByMinute {
		if minute.IsZero() && len(msgs) > 0 {
			if len(messagesByMinute) > 1 {
				logWarn("Skipping saving %d messages due to unparseable or missing timestamps.", len(msgs))
				continue
			} else {
				logFatal("All messages had unparseable or missing timestamps. Cannot save to minute-based files.")
			}
		}

		filePath := filepath.Join(outputDir, fmt.Sprintf("%d/%02d/%02d/%02d/%02d.json.gz",
			minute.Year(), minute.Month(), minute.Day(), minute.Hour(), minute.Minute()))

		if err := ensureDirectoryExists(filepath.Dir(filePath)); err != nil {
			logError("Failed to create directory for %s: %v", filePath, err)
			continue
		}

		file, err := os.Create(filePath)
		if err != nil {
			logError("Failed to create file %s: %v", filePath, err)
			continue
		}
		func(file *os.File) {
			defer file.Close()
			gw := gzip.NewWriter(file)
			defer gw.Close()

			encoder := json.NewEncoder(gw)
			encoder.SetIndent("", "  ")
			if err := encoder.Encode(msgs); err != nil {
				logError("Failed to write messages to %s: %v", filePath, err)
				return
			}
			logInfo("✅ Saved %d messages to %s", len(msgs), filePath)
		}(file)
	}
}

func discoverMaxMinutes(query string) int {
	logInfo("Discovering optimal minutes per query...")

	discoverTime, err := time.Parse(time.RFC3339, defaultDiscoverTime)
	if err != nil {
		logWarn("Failed to parse default discover time, defaulting to current UTC time for discovery: %v", err)
		discoverTime = time.Now().UTC()
	}

	start := discoverTime
	end := start.Add(1 * time.Minute)

	jobID, err := createSearchJob(query, start, end)
	if err != nil {
		logWarn("Discovery failed to create search job, defaulting to 60 min: %v", err)
		return 60
	}

	err = waitForJobCompletion(jobID)
	if err != nil {
		logWarn("Discovery job failed, defaulting to 60 min: %v", err)
		return 60
	}

	messages, err := fetchAllMessages(jobID)
	if err != nil {
		logWarn("Discovery failed to fetch messages, defaulting to 60 min: %v", err)
		return 60
	}

	perMinute := len(messages)
	if perMinute > 0 {
		return int(math.Max(1, float64(messageLimit/perMinute)))
	}
	return 60
}

func allMinuteFilesExist(start, end time.Time, outputDir string) bool {
	t := start.Truncate(time.Minute)
	endAdjusted := end.Truncate(time.Minute)
	if end.After(endAdjusted) {
		endAdjusted = endAdjusted.Add(time.Minute)
	}

	for t.Before(endAdjusted) {
		filePath := filepath.Join(outputDir, fmt.Sprintf("%d/%02d/%02d/%02d/%02d.json.gz",
			t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()))

		fileInfo, err := os.Stat(filePath)
		if os.IsNotExist(err) {
			return false
		}
		if err != nil {
			logError("Error stating file %s: %v", filePath, err)
			return false
		}
		if fileInfo.Size() == 0 {
			if !writeEmptyFiles {
				return false
			}
		}
		t = t.Add(1 * time.Minute)
	}
	return true
}

func writeEmptyFileIfNeeded(start time.Time, outputDir string) {
	path := filepath.Join(outputDir, fmt.Sprintf("%d/%02d/%02d/%02d",
		start.Year(), start.Month(), start.Day(), start.Hour()))
	if err := ensureDirectoryExists(path); err != nil {
		logError("Failed to create directory for empty file %s: %v", path, err)
		return
	}
	filePath := filepath.Join(path, fmt.Sprintf("%02d.json.gz", start.Minute()))

	file, err := os.Create(filePath)
	if err != nil {
		logError("Failed to create empty file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	gw := gzip.NewWriter(file)
	defer gw.Close()

	if _, err := gw.Write([]byte("[]")); err != nil {
		logError("Failed to write empty JSON array to %s: %v", filePath, err)
		return
	}
	logInfo("✅ Wrote empty file: %s", filePath)
}

// TimeRange represents a specific time range to be processed
type TimeRange struct {
	Query         string
	Start         time.Time
	End           time.Time
	OriginalStart time.Time // To log the original parent range
	OriginalEnd   time.Time   // To log the original parent range
	Depth         int
}

// processTimeRangeWorker is the actual worker that processes a single TimeRange
// It now *receives* the task from the channel, and *sends* new tasks to the channel.
func processTimeRangeWorker(tr TimeRange, outputDir string, maxMinutes int, dryRun bool, splitFactor int, failed *[]time.Time, failedMutex *sync.Mutex, wg *sync.WaitGroup, jobQueue chan struct{}, newWork chan TimeRange) {
	defer wg.Done()         // Signal completion for *this specific task*

	// The jobQueue is for limiting active Sumo Logic API calls/jobs.
	// We acquire a slot here, and release it below.
	jobQueue <- struct{}{}
	defer func() { <-jobQueue }()

	duration := tr.End.Sub(tr.Start).Minutes()
	indent := "  "
	prefix := ""
	for i := 0; i < tr.Depth; i++ {
		prefix += indent
	}
	logDebug("%s↪ process_time_range_worker: %s → %s (%.1f min)", prefix, tr.Start.Format(time.RFC3339), tr.End.Format(time.RFC3339), duration)
	logDebug("%s⏱ maxMinutes = %d, duration = %.1f", prefix, maxMinutes, duration)

	if allMinuteFilesExist(tr.Start, tr.End, outputDir) {
		logInfo("%s✅ Skipping %s–%s, all minute files exist.", prefix, tr.Start.Format(time.RFC3339), tr.End.Format(time.RFC3339))
		return
	}

	// If the current range is too large, split it and send new work to the channel
	if duration > float64(maxMinutes) && duration > minSplitMinutes.Minutes() {
		minChunksNeeded := math.Ceil(duration / float64(maxMinutes))
		desiredChunks := float64(splitFactor)
		if splitFactor == 1 {
			desiredChunks = minChunksNeeded
		} else {
			desiredChunks = math.Min(float64(splitFactor), minChunksNeeded)
		}

		if desiredChunks == 1 && duration > float64(maxMinutes) {
			desiredChunks = 2
		}

		stepMinutes := duration / desiredChunks
		for i := 0; i < int(desiredChunks); i++ {
			s := tr.Start.Add(time.Duration(math.Round(float64(i)*stepMinutes)) * time.Minute)
			e := tr.Start.Add(time.Duration(math.Round(float64(i+1)*stepMinutes)) * time.Minute)
			if e.After(tr.End) {
				e = tr.End
			}
			if s.After(e) || s.Equal(e) {
				continue
			}
			logQuerySplit(tr.Query, tr.OriginalStart, tr.OriginalEnd, s, e)

			// Add new work to the channel for processing by other workers
			wg.Add(1) // Increment for this new task
			newWork <- TimeRange{
				Query: tr.Query,
				Start: s,
				End: e,
				OriginalStart: tr.OriginalStart,
				OriginalEnd: tr.OriginalEnd,
				Depth: tr.Depth + 1,
			}
		}
		return // This worker is done with this specific task, new tasks have been queued
	} else {
		logInfo("%s✅ Processing chunk: %s -> %s (%.1f min)", prefix, tr.Start.Format(time.RFC3339), tr.End.Format(time.RFC3339), duration)
	}

	// Actual search job execution for small enough chunks
	if dryRun {
		logInfo("%s[DRY-RUN] Would query %s to %s", prefix, tr.Start.Format(time.RFC3339), tr.End.Format(time.RFC3339))
		return
	}

	jobID, err := createSearchJob(tr.Query, tr.Start, tr.End)
	if err != nil {
		logError("%s❌ Error creating search job for %s → %s: %v", prefix, tr.Start.Format(time.RFC3339), tr.End.Format(time.RFC3339), err)
		failedMutex.Lock()
		*failed = append(*failed, tr.Start)
		failedMutex.Unlock()
		return
	}
	logDebug("%s📤 Created job %s for %s → %s", prefix, jobID, tr.Start.Format(time.RFC3339), tr.End.Format(time.RFC3339))

	err = waitForJobCompletion(jobID)
	if err != nil {
		logError("%s❌ Error waiting for job %s for %s → %s: %v", prefix, jobID, tr.Start.Format(time.RFC3339), tr.End.Format(time.RFC3339), err)
		failedMutex.Lock()
		*failed = append(*failed, tr.Start)
		failedMutex.Unlock()
		return
	}

	messages, err := fetchAllMessages(jobID)
	if err != nil {
		logError("%s❌ Error fetching messages for job %s for %s → %s: %v", prefix, jobID, tr.Start.Format(time.RFC3339), tr.End.Format(time.RFC3339), err)
		failedMutex.Lock()
		*failed = append(*failed, tr.Start)
		failedMutex.Unlock()
		return
	}

	saveMessageCounts(tr.Query, messages)

	// If max messages hit, split further and add new work to the channel
	if len(messages) >= messageLimit && duration > minSplitMinutes.Minutes() {
		logWarn("%s⚠️ Max messages hit (%d): %s → %s. Splitting further.", prefix, len(messages), tr.Start.Format(time.RFC3339), tr.End.Format(time.RFC3339))
		step := int(duration / float64(splitFactor))
		if step < 1 {
			step = 1
		}
		for i := 0; i < splitFactor; i++ {
			s := tr.Start.Add(time.Duration(i*step) * time.Minute)
			e := s.Add(time.Duration(step) * time.Minute)
			if e.After(tr.End) {
				e = tr.End
			}
			if e.Sub(s) < minSplitMinutes {
				continue
			}
			logQuerySplit(tr.Query, tr.OriginalStart, tr.OriginalEnd, s, e)

			// Add new work to the channel for processing by other workers
			wg.Add(1) // Increment for this new task
			newWork <- TimeRange{
				Query: tr.Query,
				Start: s,
				End: e,
				OriginalStart: tr.OriginalStart,
				OriginalEnd: tr.OriginalEnd,
				Depth: tr.Depth + 1,
			}
		}
		return // This worker is done with this specific task, new tasks have been queued
	} else {
		if messages != nil && len(messages) > 0 {
			saveMessagesByMinute(messages, outputDir)
		} else {
			if writeEmptyFiles && tr.End.Sub(tr.Start) == 1*time.Minute {
				writeEmptyFileIfNeeded(tr.Start, outputDir)
			}
		}
	}
}


func queryFirstMinutePerDay(args *arguments) {
	summary := []string{}
	for year := args.YearRange[0]; year <= args.YearRange[1]; year++ {
		for month := args.MonthRange[0]; month <= args.MonthRange[1]; month++ {
			t := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
			daysInMonth := t.AddDate(0, 1, -1).Day()

			startDay := 1
			endDay := daysInMonth
			if args.DayRange != nil {
				startDay = args.DayRange[0]
				endDay = int(math.Min(float64(args.DayRange[1]), float64(daysInMonth)))
			}

			for day := startDay; day <= endDay; day++ {
				start := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
				end := start.Add(1 * time.Minute)

				logInfo("🔍 Querying %s to %s", start.Format(time.RFC3339), end.Format(time.RFC3339))
				jobID, err := createSearchJob(args.Query, start, end)
				if err != nil {
					summary = append(summary, fmt.Sprintf("%s: error - %v", start.Format("2006-01-02"), err))
					fmt.Printf("%s: error - %v\n", start.Format("2006-01-02"), err)
					continue
				}

				err = waitForJobCompletion(jobID)
				if err != nil {
					summary = append(summary, fmt.Sprintf("%s: error - %v", start.Format("2006-01-02"), err))
					fmt.Printf("%s: error - %v\n", start.Format("2006-01-02"), err)
					continue
				}

				messages, err := fetchAllMessages(jobID)
				if err != nil {
					summary = append(summary, fmt.Sprintf("%s: error - %v", start.Format("2006-01-02"), err))
					fmt.Printf("%s: error - %v\n", start.Format("2006-01-02"), err)
					continue
				}

				count := len(messages)
				summary = append(summary, fmt.Sprintf("%s: %d messages", start.Format("2006-01-02"), count))
				fmt.Printf("%s: %d messages\n", start.Format("2006-01-02"), count)
			}
		}
	}
	logInfo("Query first minute per day summary:")
	for _, entry := range summary {
		logInfo(entry)
	}
}

type arguments struct {
	Query                      string
	YearRange                  []int
	MonthRange                 []int
	DayRange                   []int
	OutputDir                  string
	Logfile                    string
	LogLevel                   string
	DryRun                     bool
	QueryFirstMinutePerDay     bool
	SplitFactor                int
	WriteFilesWithZeroMessages bool
	MaxConcurrentJobs          int
	MaxMinutes                 int
	SQLiteDB                   string
}

func parseArgs() *arguments {
	args := &arguments{}
	pflag.StringVar(&args.Query, "query", "", "Sumo Logic query to execute (required)")
	pflag.IntSliceVar(&args.YearRange, "year-range", []int{}, "Start and end year (e.g., 2023 2024) (required)")
	pflag.IntSliceVar(&args.MonthRange, "month-range", []int{}, "Start and end month (e.g., 1 12) (required)")
	pflag.IntSliceVar(&args.DayRange, "day-range", []int{}, "Start and end day (e.g., 1 31) (optional, defaults to full month)")
	pflag.StringVar(&args.OutputDir, "output-dir", "", "Directory to save messages (required)")
	pflag.StringVar(&args.Logfile, "logfile", "sumo-query.log", "Path to log file")
	pflag.StringVar(&args.LogLevel, "log-level", "INFO", "Logging level (DEBUG, INFO, WARN, ERROR)")
	pflag.BoolVar(&args.DryRun, "dry-run", false, "Perform a dry run without executing queries")
	pflag.BoolVar(&args.QueryFirstMinutePerDay, "query-first-minute-per-day", false, "Query only the first minute of each day")
	pflag.IntVar(&args.SplitFactor, "split-factor", 10, "Factor by which to split time ranges when message limit is hit or duration is too long")
	pflag.BoolVar(&args.WriteFilesWithZeroMessages, "write-files-with-zero-messages", false, "Write empty .json.gz files for minutes with no messages")
	pflag.IntVar(&args.MaxConcurrentJobs, "max-concurrent-jobs", defaultMaxConcurrentJobs, "Maximum number of concurrent Sumo Logic search jobs")
	pflag.IntVar(&args.MaxMinutes, "max-minutes", 0, "Maximum minutes per query (defaults to discovered value)")
	pflag.StringVar(&args.SQLiteDB, "sqlite-db", "", "Path to SQLite DB to store message counts and splits")

	pflag.Parse()
	str := fmt.Sprint(args.YearRange)
	println(str)
	println(len(args.YearRange))

	if args.Query == "" || len(args.YearRange) != 2 || len(args.MonthRange) != 2 || args.OutputDir == "" {
		pflag.Usage()
		logFatal("Missing required arguments. Please check --help for usage.")
	}
	if len(args.DayRange) != 0 && len(args.DayRange) != 2 {
		pflag.Usage()
		logFatal("Invalid --day-range. Must be 2 integers (start and end day).")
	}

	return args
}

func main() {
	args := parseArgs()

	configureLogging(args.Logfile, args.LogLevel)
	logInfo("Starting Sumo Archiver...")
	logInfo("Arguments: %+v", args)

	apiAccessID = os.Getenv("SUMO_ACCESS_ID")
	apiAccessKey = os.Getenv("SUMO_ACCESS_KEY")
	if apiAccessID == "" || apiAccessKey == "" {
		logFatal("SUMO_ACCESS_ID and SUMO_ACCESS_KEY environment variables must be set.")
	}

	writeEmptyFiles = args.WriteFilesWithZeroMessages

	if args.SQLiteDB != "" {
		initDB(args.SQLiteDB)
		defer dbConn.Close()
	}

	if args.QueryFirstMinutePerDay {
		queryFirstMinutePerDay(args)
		return
	}

	if args.MaxMinutes == 0 {
		args.MaxMinutes = discoverMaxMinutes(args.Query)
		logInfo("Discovered max-minutes: %d", args.MaxMinutes)
	}

	var wg sync.WaitGroup
	// jobQueue limits the *number of actual Sumo Logic API calls being made concurrently*
	jobQueue := make(chan struct{}, args.MaxConcurrentJobs)
	// newWork is the channel for all tasks (initial and recursively generated)
	newWork := make(chan TimeRange) // Make it unbuffered or sufficiently buffered for producer-consumer

	failedTimeRanges := make([]time.Time, 0)
	var failedMutex sync.Mutex

	// Start a fixed number of worker goroutines
	// These workers will block on `newWork` until a task is available.
	// They also block on `jobQueue` if the API concurrency limit is hit.
	for i := 0; i < args.MaxConcurrentJobs; i++ {
		go func() {
			for tr := range newWork { // Loop continues until newWork is closed
				processTimeRangeWorker(tr, args.OutputDir, args.MaxMinutes, args.DryRun, args.SplitFactor, &failedTimeRanges, &failedMutex, &wg, jobQueue, newWork)
			}
		}()
	}

	// This goroutine is responsible for *producing* all initial tasks.
	// It must complete all its additions to `newWork` before `wg.Wait()` on `main` can proceed.
	go func() {
		for year := args.YearRange[0]; year <= args.YearRange[1]; year++ {
			for month := args.MonthRange[0]; month <= args.MonthRange[1]; month++ {
				t := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
				daysInMonth := t.AddDate(0, 1, -1).Day()

				startDay := 1
				endDay := daysInMonth
				if len(args.DayRange) == 2 {
					startDay = args.DayRange[0]
					endDay = int(math.Min(float64(args.DayRange[1]), float64(daysInMonth)))
				}

				for day := startDay; day <= endDay; day++ {
					start := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
					end := start.Add(24 * time.Hour) // Initial range (e.g., a full day)

					wg.Add(1) // Add to WG for each task initially sent to newWork
					newWork <- TimeRange{
						Query:         args.Query,
						Start:         start,
						End:           end,
						OriginalStart: start, // For top-level, original is self
						OriginalEnd:   end,     // For top-level, original is self
						Depth:         0,
					}
				}
			}
		}
		// Critical: After all initial tasks are submitted, indicate no more *initial* tasks will be added.
		// We can't close `newWork` here directly because `processTimeRangeWorker` might still add more tasks.
	}()


	// A separate goroutine to close the `newWork` channel ONLY when all tasks (initial and derived) are done.
	// This goroutine implicitly waits for the main goroutine that populated the initial tasks to finish its `Add` calls.
	// This is the classic "fan-out/fan-in" pattern with dynamic work generation.
	go func() {
		wg.Wait()      // Wait for all `wg.Add(1)` calls to have a corresponding `wg.Done()`
		close(newWork) // Once wg is zero, it means all tasks have been processed, so close the work channel.
	}()

	// The main goroutine just needs to wait for the entire process to complete.
	// It doesn't need a separate `done` channel because `wg.Wait()` in the closing goroutine
	// and the subsequent `close(newWork)` will naturally lead to the workers exiting.
	// The `main` goroutine just exits after setting everything up.
	// However, to process `failedTimeRanges` at the end, `main` *does* need to wait.

	// No, the `main` goroutine itself needs to wait. The `go func() { wg.Wait(); close(newWork) }()`
	// starts a goroutine that waits, but `main` itself might exit before that goroutine finishes,
	// leading to the program terminating before all work is done.

	// The `wg.Wait()` in the closing goroutine is sufficient. `main` just needs to wait for `wg.Wait()` itself.
	// The pattern is:
	// 1. Launch N workers.
	// 2. Launch 1 producer (to queue initial work).
	// 3. The producer adds work and `wg.Add(1)` for each work.
	// 4. Workers process work, call `wg.Done()`, and if they split, add new work and `wg.Add(1)`.
	// 5. `main` calls `wg.Wait()`. This will block until all `wg.Done()` calls happen.
	// 6. Once `wg.Wait()` returns, all tasks are done.
	// 7. THEN `main` can close `newWork` to signal workers to exit.
	// 8. Wait for workers to drain the queue before exiting.

	// The issue is still the closing of the `newWork` channel. When `main` calls `wg.Wait()`, it blocks until
	// *all* tasks are done. But tasks are processed by workers that read from `newWork`.
	// If `newWork` isn't closed, the `for range newWork` loops will keep blocking.

	// The most robust way to handle this, which is a common pattern for dynamic work generation:
	// A "sentinel" value or `sync.Once` to close the channel, or a final signal.

	// Let's use `sync.Once` for closing the channel after `wg.Wait()` in a controlled manner.

	// Revised `main` function (simplifying, as `jobQueue` still exists, but the primary
	// source of the hang is `newWork` not being closed):

	// Re-think the main loop:

	// The problem is that the `wg.Wait()` in the *closing* goroutine will wait for all tasks,
	// but the workers that are blocked on `newWork` will never finish unless `newWork` is closed.
	// This means `wg.Wait()` never returns, `newWork` is never closed, and the workers never exit.

	// We need `wg.Wait()` in `main` to implicitly wait for the *final state* of `newWork`.

	// The correct pattern for this exact scenario:
	// The `wg.Add(1)` should happen *before* sending to `newWork`.
	// The `wg.Done()` should happen *after* processing a task from `newWork`.
	// A final `go func() { wg.Wait(); close(newWork) }()` will work *if* there's no race between
	// `wg.Done()` and adding new items, which there isn't in Go due to channel blocking semantics.

	// The hang is most likely occurring because the `main` goroutine (or the producer goroutine)
	// is not correctly waiting for the `wg.Wait()` *before* attempting to close the `newWork` channel,
	// or `wg.Wait()` is being called by a goroutine that itself relies on `newWork` being closed.

	// Let's ensure the closing of `newWork` happens only after *all* work has been put into it,
	// and consumed, or by a goroutine that doesn't depend on `newWork` itself.

	// The standard way to fix this "producer-consumer with dynamic work" deadlock is to:
	// 1. Start workers.
	// 2. Start producers that `wg.Add(1)` and send work.
	// 3. Start a final goroutine that `wg.Wait()` and then `close(newWork)`.
	// 4. `main` then waits for the workers to exit (implicitly via the closing of `newWork`).

	// Here's the most common and robust fix for this specific deadlock:

	// The `newWork` channel needs to be buffered (or `args.MaxConcurrentJobs` might need to be small,
	// but unbuffered can cause deadlock if the producer and consumer aren't perfectly synchronized).
	// Let's keep it unbuffered and ensure strict `wg` management.

	// The core issue is that `main` (implicitly or explicitly) needs to wait for
	// *all* work to be added to `newWork` AND for all work to be consumed.

	// Let's go with the pattern where `main` launches a separate goroutine to *populate* the initial work,
	// and this producer goroutine `wg.Add`s for its own tasks.
	// Then the main goroutine does the final `wg.Wait()`.

	// This is the most common pattern and avoids the deadlock for dynamic work:

	// Remove the `done` channel from the previous attempt. It's not needed if `main` waits on the main `wg`.
	// The `jobQueue` is correctly managed within the `processTimeRangeWorker`.

	// The `main` function should look like this:

	// 1. Initialize `wg`, `jobQueue`, `newWork`.
	// 2. Start worker goroutines.
	// 3. Start a goroutine that *generates initial tasks* and sends them to `newWork`, incrementing `wg` for each.
	// 4. Start a final goroutine that waits for `wg` to be zero, and *then* closes `newWork`.
	// 5. The main goroutine then waits for this final goroutine to complete, or explicitly calls `wg.Wait()`.

	// The problem is that the `wg.Wait()` is called by a goroutine (`go func() { wg.Wait(); close(newWork) }()`)
	// that itself is a *consumer* of the `newWork` channel (indirectly, by closing it).
	// If the workers are blocked on `newWork` because it's not closed, `wg.Wait()` will never complete.

	// Solution: A channel for signaling that all initial work is submitted.

	// Let's try this:

	// `newWork` channel now receives a larger buffer to reduce blocking of producers
	newWork = make(chan TimeRange, args.MaxConcurrentJobs * 10) // Larger buffer for newWork

	// In `main`:
	// var wg sync.WaitGroup
	jobQueue = make(chan struct{}, args.MaxConcurrentJobs)
	failedTimeRanges = make([]time.Time, 0)
	// var failedMutex sync.Mutex

	// Channel to signal that initial work has been submitted
	initialWorkSubmitted := make(chan struct{})

	// Start a fixed number of worker goroutines
	for i := 0; i < args.MaxConcurrentJobs; i++ {
		go func() {
			// Workers continuously pull from newWork channel
			// This loop will exit when newWork is closed.
			for tr := range newWork {
				processTimeRangeWorker(tr, args.OutputDir, args.MaxMinutes, args.DryRun, args.SplitFactor, &failedTimeRanges, &failedMutex, &wg, jobQueue, newWork)
			}
		}()
	}

	// Goroutine to submit initial work
	go func() {
		defer close(initialWorkSubmitted) // Signal that initial work is done being added
		for year := args.YearRange[0]; year <= args.YearRange[1]; year++ {
			for month := args.MonthRange[0]; month <= args.MonthRange[1]; month++ {
				t := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
				daysInMonth := t.AddDate(0, 1, -1).Day()

				startDay := 1
				endDay := daysInMonth
				if len(args.DayRange) == 2 {
					startDay = args.DayRange[0]
					endDay = int(math.Min(float64(args.DayRange[1]), float64(daysInMonth)))
				}

				for day := startDay; day <= endDay; day++ {
					start := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
					end := start.Add(24 * time.Hour)

					wg.Add(1) // Increment for each initial TimeRange added
					newWork <- TimeRange{
						Query:         args.Query,
						Start:         start,
						End:           end,
						OriginalStart: start,
						OriginalEnd:   end,
						Depth:         0,
					}
				}
			}
		}
	}()

	// Main goroutine waits for initial work to be submitted
	<-initialWorkSubmitted

	// Now that all initial work is submitted, main waits for all tasks (initial and derived) to complete.
	wg.Wait()

	// Once wg.Wait() returns, it means all tasks have called wg.Done().
	// So, no more work will be produced by any worker, and it's safe to close newWork.
	close(newWork)

	// Now wait for workers to fully drain and exit their `for range newWork` loops.
	// A small delay or a more explicit "worker done" channel could be added if necessary,
	// but typically, closing the channel is enough for the loops to terminate.
	// In this case, `wg.Wait()` already ensures all tasks have completed, so the workers *should*
	// be in a state where they are waiting for `newWork` to close, and will then exit.

	// No need for `done` channel here as `wg.Wait()` directly covers the main goroutine's need to wait.

	// Final check before printing failed ranges:
	// A small sleep could be added to ensure all logs from workers finish printing, but not strictly necessary for correctness.
	// time.Sleep(100 * time.Millisecond) // Optional: allow last few logs to flush

	if len(failedTimeRanges) > 0 {
		logError("🚨 Completed with failed time ranges:")
		for _, failedStart := range failedTimeRanges {
			logError("    %s", failedStart.Format(time.RFC3339))
		}
		os.Exit(1)
	} else {
		logInfo("🎉 All queries completed successfully!")
	}
}
