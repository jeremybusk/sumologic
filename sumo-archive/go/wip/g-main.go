package main

import (
	"bytes"
	"compress/gzip"
	"context" // Added for http.NewRequestWithContext
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
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/pflag"
)

// Constants
const (
	protocol              = "https"
	defaultSumoHost       = "api.us2.sumologic.com"
	searchJobResultsLimit = 10000
	defaultMaxConcurrentJobs = 1
	apiRateLimitDelay     = 2 * time.Second
	messageLimit          = 199999 // Max messages Sumo will return for a single job time range before needing to split
	minSplitMinutes       = 1 * time.Minute
	defaultDiscoverTime   = "2024-03-01T15:00:00Z"
)

var (
	sumoAPIURL        string
	apiAccessID       string
	apiAccessKey      string
	writeEmptyFiles   bool
	dbConn            *sql.DB
	dbLock            sync.Mutex
	currentLogLevel   string
	sharedHttpClient  *http.Client
	resumeFrom        time.Time
	semaphore         chan struct{}
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
	State        string `json:"state"`
	MessageCount int    `json:"messageCount"` // Added to get message count for pre-allocation
	// RecordCount int `json:"recordCount"` // Alternative if messageCount isn't the right one
}

func initSumoHost() {
	host := os.Getenv("SUMO_HOST")
	if host == "" {
		host = defaultSumoHost
	}
	sumoAPIURL = fmt.Sprintf("%s://%s/api/v1/search/jobs", protocol, host)
}

func initHttpClient(maxConcurrentJobs int) {
	transport := &http.Transport{
		MaxIdleConns:        maxConcurrentJobs * 2, // Allow more idle conns than concurrent jobs
		MaxIdleConnsPerHost: maxConcurrentJobs + 10,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	sharedHttpClient = &http.Client{
		Timeout:   180 * time.Second, // General generous timeout, Sumo API can be slow for large queries
		Transport: transport,
	}
}

// --- Logging ---

func configureLogging(logfile string, level string) {
	currentLogLevel = strings.ToUpper(level)
	logFile, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile) // log.Lshortfile is useful for debugging
	logInfo("Logging initialized. Level: %s", currentLogLevel)
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
	// Errors are always logged regardless of level, but FATAL will exit.
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
	if dbConn == nil || len(messages) == 0 {
		return
	}

	grouped := make(map[string]int)
	for _, msg := range messages { // msg is a copy of Message struct, Map is a reference
		if tsVal, ok := msg.Map["_messagetime"]; ok {
			var ts int64
			var parseErr error

			// _messagetime should already be formatted to RFC3339Nano by saveMessagesByMinute
			// Or, if saveMessagesByMinute is not called before this for some reason,
			// we might need to parse it from its original form.
			// Assuming it's already string RFC3339Nano from previous processing, or original format.
			// For robustness, let's handle multiple types, as original did.
			// The `_messagetime` used for grouping here is the one *before* formatting to RFC3339Nano
			// if `saveMessagesByMinute` modifies it.
			// It's safer if `saveMessageCounts` uses the raw `_messagetime`.

			switch v := tsVal.(type) {
			case float64: // Typically from JSON if not using UseNumber or specific parsing
				ts = int64(v)
			case string:
				// If it's already RFC3339Nano string from a previous step, parse it.
				// Otherwise, it might be a raw timestamp string from Sumo.
				// The original code parsed it as int64 string.
				parsedTs, err := time.Parse(time.RFC3339Nano, v)
				if err == nil {
					ts = parsedTs.UnixNano() / 1e6 // Convert to milliseconds
				} else {
					ts, parseErr = strconv.ParseInt(v, 10, 64) // Try parsing as int string
					if parseErr != nil {
						// logWarn("Failed to parse _messagetime string '%s' for counts: %v", v, parseErr)
						continue
					}
				}
			case json.Number:
				ts, parseErr = v.Int64()
				if parseErr != nil {
					// logWarn("Failed to parse _messagetime json.Number '%s' for counts: %v", v.String(), parseErr)
					continue
				}
			default:
				// logWarn("'_messagetime' has unexpected type %T for counts: %v", tsVal, tsVal)
				continue
			}
			// Assuming ts is now in milliseconds epoch
			dt := time.Unix(ts/1000, (ts%1000)*1000000).In(time.UTC).Truncate(time.Minute)
			grouped[dt.Format(time.RFC3339)]++
		}
	}

	if len(grouped) == 0 {
		return
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
		reqCtx, cancelReqCtx := context.WithTimeout(context.Background(), 60*time.Second) // Timeout for the request itself
		defer cancelReqCtx()

		req, err := http.NewRequestWithContext(reqCtx, "POST", sumoAPIURL, bytes.NewBuffer(payloadBytes))
		if err != nil {
			return "", fmt.Errorf("failed to create request: %w", err)
		}
		req.SetBasicAuth(apiAccessID, apiAccessKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "SumoLogicGoArchiver/1.0")


		resp, err := sharedHttpClient.Do(req)
		if err != nil {
			return "", fmt.Errorf("failed to make request: %w", err)
		}
		
		if resp.StatusCode == http.StatusTooManyRequests {
			bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096)) // Read limited body for logs
			resp.Body.Close()
			logWarn("Rate limit hit (429) when creating search job. Body: %s. Sleeping for %v before retry...", string(bodyBytes), apiRateLimitDelay)
			time.Sleep(apiRateLimitDelay)
			continue
		}

		if resp.StatusCode != http.StatusAccepted {
			bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return "", fmt.Errorf("failed to create search job, status: %s, body: %s", resp.Status, string(bodyBytes))
		}

		var jobResponse SearchJobResponse
		if err := json.NewDecoder(resp.Body).Decode(&jobResponse); err != nil {
			resp.Body.Close()
			return "", fmt.Errorf("failed to decode search job response: %w", err)
		}
		resp.Body.Close()
		return jobResponse.ID, nil
	}
}

func waitForJobCompletion(jobID string) (error, int) { // Returns error and messageCount
	statusURL := fmt.Sprintf("%s/%s", sumoAPIURL, jobID)
	for {
		reqCtx, cancelReqCtx := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancelReqCtx()

		req, err := http.NewRequestWithContext(reqCtx, "GET", statusURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create status request: %w", err), 0
		}
		req.SetBasicAuth(apiAccessID, apiAccessKey)
		req.Header.Set("User-Agent", "SumoLogicGoArchiver/1.0")

		resp, err := sharedHttpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to make status request: %w", err), 0
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			logWarn("Rate limit hit (429) when checking job status. Body: %s. Retrying in %v...", string(bodyBytes), apiRateLimitDelay)
			time.Sleep(apiRateLimitDelay)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return fmt.Errorf("failed to get search job status, status: %s, body: %s", resp.Status, string(bodyBytes)), 0
		}

		var statusResponse SearchJobStatusResponse
		// It's good practice to read the body first then decode, or ensure decoder consumes all.
		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close() // Close body immediately after read
		if err != nil {
			return fmt.Errorf("failed to read search job status response body: %w", err), 0
		}

		if err := json.Unmarshal(bodyBytes, &statusResponse); err != nil {
			return fmt.Errorf("failed to decode search job status response: %w. Body: %s", err, string(bodyBytes)), 0
		}
		
		logDebug("Job %s status: %s, MessageCount: %d", jobID, statusResponse.State, statusResponse.MessageCount)


		switch statusResponse.State {
		case "DONE GATHERING RESULTS":
			return nil, statusResponse.MessageCount
		case "CANCELLED", "FAILED":
			return fmt.Errorf("search job %s failed or cancelled with state: %s", jobID, statusResponse.State), 0
		case "NOT STARTED", "GATHERING RESULTS":
			time.Sleep(5 * time.Second) // Wait before polling again
		default: // Unknown state
			logWarn("Job %s in unknown state: %s. Polling again after delay.", jobID, statusResponse.State)
			time.Sleep(10 * time.Second)
		}
	}
}

func fetchAllMessages(jobID string, expectedMessages int) ([]Message, error) {
	var allMessages []Message
	if expectedMessages > 0 {
		// Pre-allocate slice if expected count is known and seems reasonable
		// Cap pre-allocation to avoid massive allocation if expectedMessages is erroneously huge.
		allocCap := expectedMessages
		if allocCap > messageLimit*2 { // Safety cap, e.g., twice the typical max per job
			allocCap = messageLimit * 2
		}
		allMessages = make([]Message, 0, allocCap)
		logDebug("Pre-allocated message slice for job %s with capacity %d", jobID, allocCap)
	}

	offset := 0
	for {
		messagesURL := fmt.Sprintf("%s/%s/messages?limit=%d&offset=%d", sumoAPIURL, jobID, searchJobResultsLimit, offset)
		
		reqCtx, cancelReqCtx := context.WithTimeout(context.Background(), 120*time.Second) // Longer timeout for fetching messages
		defer cancelReqCtx()

		req, err := http.NewRequestWithContext(reqCtx, "GET", messagesURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create messages request: %w", err)
		}
		req.SetBasicAuth(apiAccessID, apiAccessKey)
		req.Header.Set("User-Agent", "SumoLogicGoArchiver/1.0")


		resp, err := sharedHttpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to make messages request: %w", err)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			logWarn("Rate limit hit (429) fetching messages. Body: %s. Retrying in %v...", string(bodyBytes), apiRateLimitDelay)
			time.Sleep(apiRateLimitDelay)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return nil, fmt.Errorf("failed to fetch messages, status: %s, body: %s", resp.Status, string(bodyBytes))
		}

		var responseData struct {
			Messages []Message `json:"messages"`
		}
		
		// It's generally safer to read the body fully before decoding, especially if the decoder might not consume everything on error.
		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close() // Close body immediately
		if err != nil {
			return nil, fmt.Errorf("failed to read messages response body: %w", err)
		}

		decoder := json.NewDecoder(bytes.NewReader(bodyBytes)) // Decode from the read bytes
		decoder.UseNumber() // Important for _messagetime to be handled correctly as number or string
		if err := decoder.Decode(&responseData); err != nil {
			// Provide more context on decode error
			previewLen := 1024
			if len(bodyBytes) < previewLen {
				previewLen = len(bodyBytes)
			}
			return nil, fmt.Errorf("failed to decode messages response: %w. First %d bytes of body: %s", err, previewLen, string(bodyBytes[:previewLen]))
		}


		if len(responseData.Messages) > 0 {
			allMessages = append(allMessages, responseData.Messages...)
		}
		
		logDebug("Fetched page for job %s: %d messages, offset %d. Total so far: %d", jobID, len(responseData.Messages), offset, len(allMessages))


		if len(responseData.Messages) < searchJobResultsLimit {
			break // Last page
		}
		offset += searchJobResultsLimit
		if offset > messageLimit+searchJobResultsLimit { // Safety break if API behaves unexpectedly
			logWarn("Job %s: Fetching offset %d exceeded messageLimit %d + one page. Breaking fetch loop to prevent infinite loop.", jobID, offset, messageLimit)
			break
		}

	}
	logInfo("Job %s: Fetched a total of %d messages.", jobID, len(allMessages))
	return allMessages, nil
}


func saveMessagesByMinute(messages []Message, outputDir string) {
    messagesByMinute := make(map[time.Time][]Message, len(messages)/100) // Estimate initial capacity
    var unhandledMessagesCount int

    for i := 0; i < len(messages); i++ {
        msgObj := messages[i] // msgObj is a copy of the struct, Map field is a reference

        tsVal, ok := msgObj.Map["_messagetime"]
        if !ok {
            // logWarn("'_messagetime' key not found in message map: %v", msgObj.Map) // Can be very verbose
            unhandledMessagesCount++
            continue
        }

        var ts int64
        var parseErr error
        validTimestamp := true

        switch v := tsVal.(type) {
        case float64:
            ts = int64(v)
        case string:
            // Try parsing as int first (epoch millis)
            parsedInt, err := strconv.ParseInt(v, 10, 64)
            if err == nil {
                ts = parsedInt
            } else {
                // Try parsing as RFC3339Nano if int parse failed (e.g., if already formatted)
                parsedTime, errTime := time.Parse(time.RFC3339Nano, v)
                if errTime == nil {
                    ts = parsedTime.UnixNano() / 1e6 // convert to milliseconds
                } else {
                    // logWarn("Failed to parse _messagetime string '%s' as int or RFC3339Nano: %v, %v. Message: (map keys: %v)", v, err, errTime, getMapKeys(msgObj.Map))
                    parseErr = errTime // Store one of the errors
                    validTimestamp = false
                }
            }
        case json.Number:
            parsedInt, err := v.Int64()
            if err != nil {
                // logWarn("Failed to parse _messagetime json.Number '%s' to int64: %v. Message: (map keys: %v)", v.String(), err, getMapKeys(msgObj.Map))
                parseErr = err
                validTimestamp = false
            } else {
                ts = parsedInt
            }
        default:
            // logWarn("'_messagetime' has unexpected type %T: %v. Message: (map keys: %v)", tsVal, tsVal, getMapKeys(msgObj.Map))
            validTimestamp = false
        }

        if !validTimestamp || parseErr != nil {
            unhandledMessagesCount++
            continue
        }

        t := time.Unix(ts/1000, (ts%1000)*1000000).In(time.UTC)
        // Modify the map in place. Since msgObj.Map is a reference to messages[i].Map, this change is reflected.
        msgObj.Map["_messagetime"] = t.Format(time.RFC3339Nano)

        dt := t.Truncate(time.Minute)
        messagesByMinute[dt] = append(messagesByMinute[dt], msgObj)
    }

    if unhandledMessagesCount > 0 {
        if unhandledMessagesCount == len(messages) && len(messages) > 0 {
            logError("All %d messages in this batch had unparseable/missing timestamps. Cannot save to minute-based files.", len(messages))
            return // No valid messages to save
        }
        logWarn("Skipped saving %d out of %d messages from this batch due to unparseable/missing timestamps.", unhandledMessagesCount, len(messages))
    }
	
	if len(messagesByMinute) == 0 && len(messages) > 0 && unhandledMessagesCount < len(messages) {
		logWarn("No messages were grouped by minute, but there were %d messages initially and %d unhandled. This is unexpected.", len(messages), unhandledMessagesCount)
	}


    for minute, msgsInMinute := range messagesByMinute {
        if minute.IsZero() { // Should not happen if unhandled are skipped, but as a safeguard.
            logWarn("Skipping %d messages with zero timestamp key.", len(msgsInMinute))
            continue
        }
		if len(msgsInMinute) == 0 && !writeEmptyFiles { // Should not happen if list is empty
			continue
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
        
        func(f *os.File, currentMsgs []Message, p string) { // Pass filePath for logging
            defer f.Close()
            gw, err := gzip.NewWriterLevel(f, gzip.BestSpeed)
            if err != nil {
                logError("Failed to create gzip writer for %s: %v", p, err)
                return
            }
            defer gw.Close()

            encoder := json.NewEncoder(gw)
            // encoder.SetIndent("", "  ") // Usually not needed for storage, saves space
            if err := encoder.Encode(currentMsgs); err != nil {
                logError("Failed to write messages to %s: %v", p, err)
                return
            }
            logInfo("✅ Saved %d messages to %s", len(currentMsgs), p)
        }(file, msgsInMinute, filePath)
    }
}


func discoverMaxMinutes(query string, cliArgs *arguments) int {
	logInfo("Discovering optimal minutes per query based on message density...")

	discoverTime, err := time.Parse(time.RFC3339, defaultDiscoverTime)
	if err != nil {
		logWarn("Failed to parse default discover time ('%s'), defaulting to one hour ago for discovery: %v", defaultDiscoverTime, err)
		discoverTime = time.Now().UTC().Add(-1 * time.Hour)
	}

	// Ensure discovery time is at least 2 minutes in the past to allow data ingestion
	if time.Since(discoverTime) < 2*time.Minute {
		discoverTime = time.Now().UTC().Add(-2 * time.Minute)
		logInfo("Adjusted discovery time to %s to ensure data availability.", discoverTime.Format(time.RFC3339))
	}
	
	start := discoverTime.Truncate(time.Minute) // Ensure it starts on a minute boundary
	end := start.Add(1 * time.Minute)

	logInfo("Discovery query for time range: %s to %s", start.Format(time.RFC3339), end.Format(time.RFC3339))

	jobID, err := createSearchJob(query, start, end)
	if err != nil {
		logWarn("Discovery: failed to create search job, defaulting to 60 min for maxMinutes: %v", err)
		return 60
	}

	err, messagesInJob := waitForJobCompletion(jobID) // messagesInJob here is expected count
	if err != nil {
		logWarn("Discovery: job failed or was cancelled, defaulting to 60 min for maxMinutes: %v", err)
		return 60
	}

	// messagesInJob from waitForJobCompletion is the count from API, use it directly.
	// No need to fetch all messages just to count them for discovery.
	messagesPerMinuteInDiscovery := messagesInJob 
	logInfo("Discovery: job %s completed with %d messages for a 1-minute window.", jobID, messagesPerMinuteInDiscovery)


	if messagesPerMinuteInDiscovery > 0 {
		// Calculate max minutes to stay under messageLimit (Sumo's limit for a single job)
		calculatedMaxMinutes := messageLimit / messagesPerMinuteInDiscovery
		if calculatedMaxMinutes < 1 {
			calculatedMaxMinutes = 1 // Must be at least 1 minute
		}
		// Cap maxMinutes to a reasonable upper bound, e.g., 6 hours (360 mins) to prevent excessively long queries
		// even if message density is very low.
		if calculatedMaxMinutes > 360 {
			logInfo("Discovery: calculated max minutes %d capped at 360.", calculatedMaxMinutes)
			calculatedMaxMinutes = 360
		}
		logInfo("Discovery: Based on %d messages/min, calculated maxMinutes per job: %d", messagesPerMinuteInDiscovery, calculatedMaxMinutes)
		return calculatedMaxMinutes
	}
	
	// If no messages found in the discovery window, it could be a sparse period or an issue.
	// Default to a conservative value (e.g., 60 minutes) rather than a very large one.
	logWarn("Discovery: no messages found in the 1-minute discovery window. Defaulting to 60 min for maxMinutes.")
	return 60
}


func allMinuteFilesExist(start, end time.Time, outputDir string) bool {
	currentMinute := start.Truncate(time.Minute)
	// end is exclusive, so we check up to the minute before end.
	// Or, if end is exactly on a minute, we check that minute too if it's part of the range.
	// Let's iterate while currentMinute is before end.
	for !currentMinute.After(end.Add(-1*time.Second)) && !currentMinute.Equal(end) { // Check up to, but not including, end time
		filePath := filepath.Join(outputDir, fmt.Sprintf("%d/%02d/%02d/%02d/%02d.json.gz",
			currentMinute.Year(), currentMinute.Month(), currentMinute.Day(), currentMinute.Hour(), currentMinute.Minute()))

		fileInfo, err := os.Stat(filePath)
		if os.IsNotExist(err) {
			logDebug("File check: %s does NOT exist.", filePath)
			return false
		}
		if err != nil {
			logError("File check: Error stating file %s: %v", filePath, err)
			return false // Error, assume not all exist
		}
		// If writeEmptyFiles is false, an empty file means it might not have been processed correctly or had no data.
		// If writeEmptyFiles is true, an empty file is considered "existing".
		if fileInfo.Size() == 0 && !writeEmptyFiles {
			logDebug("File check: %s exists but is empty, and writeEmptyFiles is false.", filePath)
			return false
		}
		currentMinute = currentMinute.Add(1 * time.Minute)
	}
	return true
}


func writeEmptyFileIfNeeded(minuteTime time.Time, outputDir string) {
	// filePath for the specific minute
	filePath := filepath.Join(outputDir, fmt.Sprintf("%d/%02d/%02d/%02d/%02d.json.gz",
		minuteTime.Year(), minuteTime.Month(), minuteTime.Day(), minuteTime.Hour(), minuteTime.Minute()))

	if err := ensureDirectoryExists(filepath.Dir(filePath)); err != nil {
		logError("Failed to create directory for empty file %s: %v", filePath, err)
		return
	}
	
	// Check if file already exists and is non-empty to avoid overwriting
	if stat, err := os.Stat(filePath); err == nil && stat.Size() > 0 {
		logDebug("Empty file write skipped for %s, already exists and is non-empty.", filePath)
		return
	}


	file, err := os.Create(filePath)
	if err != nil {
		logError("Failed to create empty file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	gw, err := gzip.NewWriterLevel(file, gzip.BestSpeed)
	if err != nil {
		logError("Failed to create gzip writer for empty file %s: %v", filePath, err)
		return
	}
	defer gw.Close()

	// Write an empty JSON array "[]"
	if _, err := gw.Write([]byte("[]")); err != nil {
		logError("Failed to write empty JSON array to %s: %v", filePath, err)
		return
	}
	logInfo("✅ Wrote empty file: %s", filePath)
}


func processTimeRange(
	query string,
	originalJobStart, originalJobEnd time.Time, // Renamed for clarity
	outputDir string,
	maxMinutesPerJob int,
	dryRun bool,
	splitFactor int,
	failedRanges *[]time.Time, // Slice of start times of failed ranges
	failedMutex *sync.Mutex,
	depth int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	semaphore <- struct{}{}
	defer func() { <-semaphore }()

	// Resume logic check
	if originalJobEnd.Before(resumeFrom) || originalJobEnd.Equal(resumeFrom) {
		logInfo("⏩ Skipping %s → %s (before or at resume-from time %s)",
			originalJobStart.Format(time.RFC3339), originalJobEnd.Format(time.RFC3339), resumeFrom.Format(time.RFC3339))
		return
	}
	// If job starts before resumeFrom but ends after, it will be processed (no special handling here, could be refined if needed)

	durationMinutes := originalJobEnd.Sub(originalJobStart).Minutes()
	indent := strings.Repeat("  ", depth) // Two spaces for indent
	prefix := fmt.Sprintf("[%s->%s %.0fm d%d] ", originalJobStart.Format("15:04:05Z"), originalJobEnd.Format("15:04:05Z"), durationMinutes, depth)


	logDebug("%s↪ process_time_range called for %s → %s (%.1f min)", prefix+indent, originalJobStart.Format(time.RFC3339), originalJobEnd.Format(time.RFC3339), durationMinutes)

	// Check if all individual minute files for this range already exist
	if !dryRun && allMinuteFilesExist(originalJobStart, originalJobEnd, outputDir) {
		logInfo("%s✅ Skipping completed range %s → %s, all minute files exist.", prefix+indent, originalJobStart.Format(time.RFC3339), originalJobEnd.Format(time.RFC3339))
		return
	}
	
	// Condition to split: duration > maxMinutes OR duration > minSplit (if maxMinutes is very small)
	// Ensure we don't split below minSplitMinutes unless absolutely necessary (e.g. messageLimit hit on a small range)
	needsRecursiveSplit := durationMinutes > float64(maxMinutesPerJob) && durationMinutes > minSplitMinutes.Minutes()

	if needsRecursiveSplit {
		logInfo("%s쪼 Splitting range %s → %s (%.0f min) into %d parts as it exceeds max %d min per job.",
			prefix+indent, originalJobStart.Format(time.RFC3339), originalJobEnd.Format(time.RFC3339), durationMinutes, splitFactor, maxMinutesPerJob)

		subDuration := time.Duration(math.Ceil(durationMinutes/float64(splitFactor))) * time.Minute
		if subDuration < minSplitMinutes && durationMinutes > minSplitMinutes.Minutes() { // Avoid splitting too small initially if parent is large enough
			subDuration = minSplitMinutes
		}
		if subDuration == 0 { // Safety, should not happen if durationMinutes > 0
			subDuration = time.Minute 
		}


		currentSubStart := originalJobStart
		for i := 0; i < splitFactor && currentSubStart.Before(originalJobEnd); i++ {
			currentSubEnd := currentSubStart.Add(subDuration)
			if currentSubEnd.After(originalJobEnd) {
				currentSubEnd = originalJobEnd
			}

			if currentSubStart.Equal(currentSubEnd) || currentSubStart.After(currentSubEnd) {
				continue // Skip zero or negative duration sub-ranges
			}
			
			logQuerySplit(query, originalJobStart, originalJobEnd, currentSubStart, currentSubEnd) // Log this split decision

			wg.Add(1)
			go processTimeRange(query, currentSubStart, currentSubEnd, outputDir, maxMinutesPerJob, dryRun, splitFactor, failedRanges, failedMutex, depth+1, wg)
			currentSubStart = currentSubEnd
		}
		return
	}

	// Actual processing for this time range (not split further by initial duration check)
	if dryRun {
		logInfo("%s[DRY-RUN] Would query %s to %s (%.1f min)", prefix+indent, originalJobStart.Format(time.RFC3339), originalJobEnd.Format(time.RFC3339), durationMinutes)
		// In dry run, if writeEmptyFiles is true, we could simulate creating empty files for this range
		if writeEmptyFiles {
			logInfo("%s[DRY-RUN] Would ensure empty files for each minute in %s to %s", prefix+indent, originalJobStart.Format(time.RFC3339), originalJobEnd.Format(time.RFC3339))
			// Simulate writing empty files for each minute in the range if it's small enough
			// This part is mainly for illustration as dry-run doesn't write.
			if durationMinutes <= float64(maxMinutesPerJob) { // Only for "leaf" dry-run nodes
				 for t := originalJobStart.Truncate(time.Minute); t.Before(originalJobEnd); t = t.Add(time.Minute) {
                    logDebug("%s[DRY-RUN] Imagined empty file for minute: %s", prefix+indent, t.Format(time.RFC3339))
                }
			}
		}
		return
	}

	logInfo("%s🏹 Querying %s → %s (%.1f min)", prefix+indent, originalJobStart.Format(time.RFC3339), originalJobEnd.Format(time.RFC3339), durationMinutes)

	var jobID string
	var err error
	// Retry logic for createSearchJob is now within createSearchJob itself for 429s.
	jobID, err = createSearchJob(query, originalJobStart, originalJobEnd)
	if err != nil {
		logError("%s❌ Error creating search job for %s → %s: %v", prefix+indent, originalJobStart.Format(time.RFC3339), originalJobEnd.Format(time.RFC3339), err)
		failedMutex.Lock()
		*failedRanges = append(*failedRanges, originalJobStart)
		failedMutex.Unlock()
		return
	}
	logInfo("%s📤 Created Sumo job %s for %s → %s (%.1f min)", prefix+indent, jobID, originalJobStart.Format(time.RFC3339), originalJobEnd.Format(time.RFC3339), durationMinutes)

	var expectedMessages int
	err, expectedMessages = waitForJobCompletion(jobID) // expectedMessages is the API's count
	if err != nil {
		logError("%s❌ Job %s failed or was cancelled: %v", prefix+indent, jobID, err)
		failedMutex.Lock()
		*failedRanges = append(*failedRanges, originalJobStart)
		failedMutex.Unlock()
		return
	}
	logInfo("%s⌛ Job %s finished. Expecting %d messages.", prefix+indent, jobID, expectedMessages)


	messages, err := fetchAllMessages(jobID, expectedMessages)
	if err != nil {
		logError("%s❌ Job %s: Failed to fetch messages: %v", prefix+indent, jobID, err)
		failedMutex.Lock()
		*failedRanges = append(*failedRanges, originalJobStart)
		failedMutex.Unlock()
		return
	}
	logInfo("%s📥 Job %s: Fetched %d messages (API expected %d).", prefix+indent, jobID, len(messages), expectedMessages)


	// Save message counts to DB (if enabled)
	saveMessageCounts(query, messages) // Uses original _messagetime if available

	// Check if messageLimit was hit, indicating the query returned too much data for one go.
	// Sumo caps results, so len(messages) might be messageLimit even if more existed.
	// Use the API's expectedMessages count if it's reliable and higher than messageLimit.
	// Sumo's behavior: if a query gathers more than `messageLimit` (e.g. 200k), it truncates.
	// The `messageCount` from job status reflects the number of messages *gathered* by the job, which might be capped.
	// So, `len(messages)` is the actual number retrieved.
	if len(messages) >= messageLimit && durationMinutes > minSplitMinutes.Minutes() {
		logWarn("%s⚠️ Max messages (%d) hit for job %s (%s → %s, %.0fm). Splitting further into %d parts.",
			prefix+indent, len(messages), jobID, originalJobStart.Format(time.RFC3339), originalJobEnd.Format(time.RFC3339), durationMinutes, splitFactor)
		
		subDuration := time.Duration(math.Ceil(durationMinutes/float64(splitFactor))) * time.Minute
		 if subDuration < minSplitMinutes && durationMinutes > minSplitMinutes.Minutes() {
            subDuration = minSplitMinutes
        }
		if subDuration == 0 { subDuration = time.Minute }


		currentSubStart := originalJobStart
		for i := 0; i < splitFactor && currentSubStart.Before(originalJobEnd); i++ {
			currentSubEnd := currentSubStart.Add(subDuration)
			if currentSubEnd.After(originalJobEnd) {
				currentSubEnd = originalJobEnd
			}
            if currentSubStart.Equal(currentSubEnd) || currentSubStart.After(currentSubEnd) {
				continue
			}
			logQuerySplit(query, originalJobStart, originalJobEnd, currentSubStart, currentSubEnd)

			wg.Add(1)
			go processTimeRange(query, currentSubStart, currentSubEnd, outputDir, maxMinutesPerJob, dryRun, splitFactor, failedRanges, failedMutex, depth+1, wg)
			currentSubStart = currentSubEnd
		}
	} else {
		// Processed this chunk, save messages to files
		if len(messages) > 0 {
			saveMessagesByMinute(messages, outputDir) // This function now logs successful saves
		} else {
			// No messages returned for this time range.
			// If writeEmptyFiles is true, we need to create empty files for each minute in this job's range.
			logInfo("%s💨 No messages found for job %s (%s → %s).", prefix+indent, jobID, originalJobStart.Format(time.RFC3339), originalJobEnd.Format(time.RFC3339))
			if writeEmptyFiles {
				logInfo("%s📝 Writing empty files for range %s → %s because no messages were returned.", prefix+indent, originalJobStart.Format(time.RFC3339), originalJobEnd.Format(time.RFC3339))
				// Iterate through each minute in the current job's time range [originalJobStart, originalJobEnd)
				for t := originalJobStart.Truncate(time.Minute); t.Before(originalJobEnd); t = t.Add(time.Minute) {
					// Check if file already exists (e.g. from a previous partial run for this minute)
					// allMinuteFilesExist checks a range, here we check individual before writing empty.
					filePath := filepath.Join(outputDir, fmt.Sprintf("%d/%02d/%02d/%02d/%02d.json.gz",
						t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()))
					if _, err := os.Stat(filePath); os.IsNotExist(err) {
						writeEmptyFileIfNeeded(t, outputDir)
					} else if err == nil {
						// File exists, do nothing or log debug
						logDebug("%s📝 Empty file for minute %s already exists, not overwriting.", prefix+indent, t.Format(time.RFC3339))
					} else {
						logError("%s📝 Error checking existing empty file for minute %s: %v", prefix+indent, t.Format(time.RFC3339), err)
					}
				}
			}
		}
	}
	// Explicitly nil the large slice to potentially help GC, though scope should handle it.
	messages = nil 
}


func queryFirstMinutePerDay(args *arguments) {
	// This function is a utility and seems less critical path for OOM,
	// but ensure it uses sharedHttpClient and proper error handling.
	summary := []string{}
	logInfo("Querying first minute of each specified day...")

	for year := args.YearRange[0]; year <= args.YearRange[1]; year++ {
		for month := args.MonthRange[0]; month <= args.MonthRange[1]; month++ {
			// Corrected: firstDayOfMonth was unused. We need daysInMonth.
			daysInMonth := time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day() // Day 0 of next month is last day of current

			startDayLoop := 1
			endDayLoop := daysInMonth
			if len(args.DayRange) == 2 {
				startDayLoop = args.DayRange[0]
				endDayLoop = args.DayRange[1]
				if endDayLoop > daysInMonth {
					endDayLoop = daysInMonth
				}
			}

			for day := startDayLoop; day <= endDayLoop; day++ {
				start := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
				end := start.Add(1 * time.Minute)

				logInfo("🔍 Querying first minute: %s to %s", start.Format(time.RFC3339), end.Format(time.RFC3339))
				jobID, err := createSearchJob(args.Query, start, end)
				if err != nil {
					errMsg := fmt.Sprintf("%s: error creating job - %v", start.Format("2006-01-02"), err)
					summary = append(summary, errMsg)
					logError(errMsg)
					continue
				}

				err, expectedMessages := waitForJobCompletion(jobID)
				if err != nil {
					errMsg := fmt.Sprintf("%s: error waiting for job %s - %v", start.Format("2006-01-02"), jobID, err)
					summary = append(summary, errMsg)
					logError(errMsg)
					continue
				}

				messages, err := fetchAllMessages(jobID, expectedMessages)
				if err != nil {
					errMsg := fmt.Sprintf("%s: error fetching messages for job %s - %v", start.Format("2006-01-02"), jobID, err)
					summary = append(summary, errMsg)
					logError(errMsg)
					continue
				}

				count := len(messages)
				successMsg := fmt.Sprintf("%s: %d messages (job %s)", start.Format("2006-01-02"), count, jobID)
				summary = append(summary, successMsg)
				logInfo(successMsg)
			}
		}
	}
	logInfo("--- Query First Minute Per Day Summary ---")
	for _, entry := range summary {
		logInfo(entry)
	}
}

// arguments struct to hold parsed command-line arguments
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
	MaxMinutes                 int // Max minutes per Sumo job, 0 means use discovered
	SQLiteDB                   string
	ResumeFromStr              string // Renamed from ResumeFrom to avoid conflict with global var
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
	pflag.BoolVar(&args.QueryFirstMinutePerDay, "query-first-minute-per-day", false, "Query only the first minute of each day for all specified days")
	pflag.IntVar(&args.SplitFactor, "split-factor", 10, "Factor by which to split time ranges when message limit is hit or duration is too long (min 2)")
	pflag.BoolVar(&args.WriteFilesWithZeroMessages, "write-files-with-zero-messages", false, "Write empty .json.gz files for minutes with no messages")
	pflag.IntVar(&args.MaxConcurrentJobs, "max-concurrent-jobs", defaultMaxConcurrentJobs, "Maximum number of concurrent Sumo Logic search jobs")
	pflag.IntVar(&args.MaxMinutes, "max-minutes", 0, "Maximum minutes per query. Overrides discovered value if > 0. Default (0) is to discover.")
	pflag.StringVar(&args.SQLiteDB, "sqlite-db", "", "Path to SQLite DB to store message counts and splits (optional)")
	pflag.StringVar(&args.ResumeFromStr, "resume-from", "", "RFC3339 UTC timestamp to resume processing from (e.g. 2024-06-01T00:00:00Z). Ranges ending before or at this time will be skipped.")

	pflag.Parse()

	// Validation
	if args.Query == "" || len(args.YearRange) != 2 || len(args.MonthRange) != 2 || args.OutputDir == "" {
		pflag.Usage()
		// Log fatal will be called by main after logging is configured if this is the path.
		// For now, print to stderr and exit.
		fmt.Fprintln(os.Stderr, "Error: Missing required arguments. Query, year-range, month-range, and output-dir are required.")
		pflag.PrintDefaults()
		os.Exit(1)
	}
	if args.YearRange[0] > args.YearRange[1] || args.MonthRange[0] > args.MonthRange[1] || args.MonthRange[0] < 1 || args.MonthRange[1] > 12 {
		fmt.Fprintln(os.Stderr, "Error: Invalid year or month range.")
		os.Exit(1)
	}
	if len(args.DayRange) != 0 {
		if len(args.DayRange) != 2 {
			fmt.Fprintln(os.Stderr, "Error: Invalid --day-range. Must be 2 integers (start and end day) or not provided.")
			os.Exit(1)
		}
		if args.DayRange[0] > args.DayRange[1] || args.DayRange[0] < 1 || args.DayRange[1] > 31 {
			fmt.Fprintln(os.Stderr, "Error: Invalid day range values.")
			os.Exit(1)
		}
	}
	if args.SplitFactor < 2 {
		fmt.Fprintln(os.Stderr, "Warning: --split-factor is less than 2. Setting to 2.")
		args.SplitFactor = 2
	}
	if args.MaxConcurrentJobs < 1 {
		fmt.Fprintln(os.Stderr, "Warning: --max-concurrent-jobs is less than 1. Setting to 1.")
		args.MaxConcurrentJobs = 1
	}


	return args
}


func main() {
	args := parseArgs() // Parse args first to use MaxConcurrentJobs for HTTP client init

	// Configure logging ASAP
	configureLogging(args.Logfile, args.LogLevel)
	logInfo("Starting Sumo Archiver...")
	// Corrected the logInfo call here by removing the extra comma
	logInfo("Arguments: Query='%s', YearRange=%v, MonthRange=%v, DayRange=%v, OutputDir='%s', LogLevel='%s', DryRun=%t, QueryFirstMin=%t, SplitFactor=%d, WriteEmpty=%t, MaxConcurrent=%d, MaxMinutes=%d, SQLiteDB='%s', ResumeFrom='%s'",
		args.Query, args.YearRange, args.MonthRange, args.DayRange, args.OutputDir, args.LogLevel, args.DryRun, args.QueryFirstMinutePerDay,
		args.SplitFactor, args.WriteFilesWithZeroMessages, args.MaxConcurrentJobs, args.MaxMinutes, args.SQLiteDB, args.ResumeFromStr)


	// Initialize global variables from environment and args
	initSumoHost() // Depends on SUMO_HOST env var
	initHttpClient(args.MaxConcurrentJobs) // Depends on MaxConcurrentJobs arg

	apiAccessID = os.Getenv("SUMO_ACCESS_ID")
	apiAccessKey = os.Getenv("SUMO_ACCESS_KEY")
	if apiAccessID == "" || apiAccessKey == "" {
		logFatal("SUMO_ACCESS_ID and SUMO_ACCESS_KEY environment variables must be set.")
	}

	writeEmptyFiles = args.WriteFilesWithZeroMessages
	if args.SQLiteDB != "" {
		initDB(args.SQLiteDB)
		defer func() {
			if dbConn != nil {
				logInfo("Closing SQLite database connection.")
				if err := dbConn.Close(); err != nil {
					logError("Error closing SQLite database: %v", err)
				}
			}
		}()
	}

	// Handle --query-first-minute-per-day mode
	if args.QueryFirstMinutePerDay {
		queryFirstMinutePerDay(args)
		logInfo("Query first minute per day finished.")
		return
	}
	
	// Determine maxMinutesPerJob
	maxMinutesPerJob := args.MaxMinutes
	if maxMinutesPerJob <= 0 { // 0 or negative means discover
		if args.DryRun {
			logInfo("[DRY-RUN] Skipping discovery of max-minutes. Using default of 60.")
			maxMinutesPerJob = 60
		} else {
			maxMinutesPerJob = discoverMaxMinutes(args.Query, args)
			logInfo("Discovered max-minutes per job: %d", maxMinutesPerJob)
		}
	} else {
		logInfo("Using user-defined max-minutes per job: %d", maxMinutesPerJob)
	}
	if maxMinutesPerJob < 1 { // Ensure it's at least 1
	    logWarn("max-minutes per job was %d, adjusting to 1.", maxMinutesPerJob)
		maxMinutesPerJob = 1
	}


	// Parse resumeFrom timestamp
	if args.ResumeFromStr != "" {
		parsedTime, err := time.Parse(time.RFC3339, args.ResumeFromStr)
		if err != nil {
			logFatal("Invalid --resume-from time format ('%s'), must be RFC3339 (e.g., 2024-06-01T00:00:00Z): %v", args.ResumeFromStr, err)
		}
		resumeFrom = parsedTime.In(time.UTC) // Ensure UTC
		logInfo("🕒 Resuming processing. Ranges ending at or before %s will be skipped.", resumeFrom.Format(time.RFC3339))
	} else {
		resumeFrom = time.Time{} // Zero time, meaning no resume point, process all.
	}


	var wg sync.WaitGroup
	semaphore = make(chan struct{}, args.MaxConcurrentJobs)
	failedTimeRanges := make([]time.Time, 0) // Store start times of failed ranges
	var failedMutex sync.Mutex

	logInfo("Starting main processing loop for date rang...")
	for year := args.YearRange[0]; year <= args.YearRange[1]; year++ {
		for month := args.MonthRange[0]; month <= args.MonthRange[1]; month++ {
			// Determine day range for the current month
			// Corrected: firstDayOfMonth was unused.
			// Last day of month: day 0 of the next month
			lastDayOfMonth := time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()

			startDayLoop := 1
			endDayLoop := lastDayOfMonth
			if len(args.DayRange) == 2 { // User specified a day range
				startDayLoop = args.DayRange[0]
				endDayLoop = args.DayRange[1]
				// Clamp user-specified day range to valid days for the current month
				if startDayLoop > lastDayOfMonth {
					logWarn("Start day %d is after last day of %d-%02d (%d). Skipping this month for this year.", startDayLoop, year, month, lastDayOfMonth)
					continue
				}
				if endDayLoop > lastDayOfMonth {
					endDayLoop = lastDayOfMonth
				}
			}
			
			if startDayLoop > endDayLoop {
				logWarn("Start day %d is after end day %d for %d-%02d. Skipping this day range.", startDayLoop, endDayLoop, year, month)
				continue
			}


			logInfo("Processing Year: %d, Month: %02d, Days: %d-%d", year, month, startDayLoop, endDayLoop)
			for day := startDayLoop; day <= endDayLoop; day++ {
				dayStart := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
				dayEnd := dayStart.Add(24 * time.Hour) // Process the full day

				if dayEnd.Before(resumeFrom) || dayEnd.Equal(resumeFrom) {
                     logDebug("⏩ Skipping day %s as it ends before  at resume-from time %s", dayStart.Format("2006-01-02"), resumeFrom.Format(time.RFC3339))
                    continue
                }

				wg.Add(1)
				go processTimeRange(args.Query, dayStart, dayEnd, args.OutputDir, maxMinutesPerJob, args.DryRun, args.SplitFactor, &failedTimeRanges, &failedMutex, 0, &wg)
			}
		}
	}

	logInfo("All jobs dispatched. Waiting for completion...")
	wg.Wait()
	close(semaphore) // Close semaphore channel once all goroutines that might send to it are done.

	if len(failedTimeRanges) > 0 {
		logError("🚨 Completed with %d failed time ranges:", len(failedTimeRanges))
		// Sort failed ranges for better readability if many
		// sort.Slice(failedTimeRanges, func(i, j int) bool { return failedTimeRanges[i].Before(failedTimeRanges[j]) })
		for _, failedStart := range failedTimeRanges {
			logError("  - Failed range starting at: %s", failedStart.Format(time.RFC3339))
		}
		logError("Please review logs for details on failures. You may need to retry these ranges or adjust parameters.")
os.Exit(1)
	} else {
		logInfo("🎉 All queries processed successfully!")
	}
}

// Helper for logging map keys if a message is problematic
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Helper for min of two ints
func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
