package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	// "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/spf13/cobra"
)

// Message represents a generic JSON message.
type Message = any
type MessageList = []Message

// ---- Helper Functions ----

// ensureDirectoryExists creates a directory if it doesn't exist.
func ensureDirectoryExists(path string) error {
	return os.MkdirAll(path, 0755)
}

// getCompressionReader returns the appropriate decompression reader based on file extension.
func getCompressionReader(file io.Reader, ext string) (io.ReadCloser, error) {
	switch ext {
	case ".gz", ".gzip":
		return gzip.NewReader(file)
	case ".zst":
		r, err := zstd.NewReader(file)
		if err != nil {
			return nil, err
		}
		// zstd.NewReader returns a concrete type, so we wrap it to satisfy io.ReadCloser
		return r.IOReadCloser(), nil
	case ".lz4":
		return io.NopCloser(lz4.NewReader(file)), nil
	default:
		return nil, fmt.Errorf("unsupported compression extension: %s", ext)
	}
}

// loadMessagesFromFile reads and decodes a compressed JSON file, auto-detecting format.
func loadMessagesFromFile(filePath string) (MessageList, error) {
	slog.Debug("Attempting to load messages", "path", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Correctly get the final extension (e.g., .gz from file.json.gz)
	ext := filepath.Ext(filePath)

	decompressor, err := getCompressionReader(file, ext)
	if err != nil {
		return nil, err
	}
	defer decompressor.Close()

	var messages MessageList
	if err := json.NewDecoder(decompressor).Decode(&messages); err != nil {
		return nil, err
	}
	slog.Debug("Successfully loaded messages", "path", filePath, "count", len(messages))
	return messages, nil
}


// ---- Core Logic Functions ----

// generateSummaryReport scans for missing time-stamped files.
func generateSummaryReport(inputDir string, yearRange, monthRange, dayRange [2]int) {
	slog.Info("Starting summary report generation",
		"input_dir", inputDir,
		"years", fmt.Sprintf("%d-%d", yearRange[0], yearRange[1]),
		"months", fmt.Sprintf("%d-%d", monthRange[0], monthRange[1]),
		"days", fmt.Sprintf("%d-%d", dayRange[0], dayRange[1]),
	)
	missing := struct {
		Months  []string
		Days    []string
		Hours   []string
		Minutes []string
	}{}

	for year := yearRange[0]; year <= yearRange[1]; year++ {
		slog.Debug("Checking year", "year", year)
		for month := monthRange[0]; month <= monthRange[1]; month++ {
			slog.Debug("Checking month", "year", year, "month", month)
			monthDir := filepath.Join(inputDir, strconv.Itoa(year), fmt.Sprintf("%02d", month))
			if _, err := os.Stat(monthDir); os.IsNotExist(err) {
				path := filepath.ToSlash(monthDir)
				slog.Warn("Missing month directory", "path", path)
				missing.Months = append(missing.Months, path)
				continue
			}

			daysInMonth := time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
			for day := dayRange[0]; day <= dayRange[1]; day++ {
				slog.Debug("Checking day", "year", year, "month", month, "day", day)
				if day > daysInMonth {
					continue
				}

				dayDir := filepath.Join(monthDir, fmt.Sprintf("%02d", day))
				if _, err := os.Stat(dayDir); os.IsNotExist(err) {
					path := filepath.ToSlash(dayDir)
					slog.Warn("Missing day directory", "path", path)
					missing.Days = append(missing.Days, path)
					continue
				}

				for hour := 0; hour < 24; hour++ {
					hourDir := filepath.Join(dayDir, fmt.Sprintf("%02d", hour))
					if _, err := os.Stat(hourDir); os.IsNotExist(err) {
						path := filepath.ToSlash(hourDir)
						slog.Debug("Missing hour directory", "path", path)
						missing.Hours = append(missing.Hours, path)
						continue
					}

					for minute := 0; minute < 60; minute++ {
						// Check for any of the supported file types
						found := false
						for _, ext := range []string{".gz", ".gzip", ".zst", ".lz4"} { // Added .gzip for completeness
							minuteFile := filepath.Join(hourDir, fmt.Sprintf("%02d.json%s", minute, ext))
							if _, err := os.Stat(minuteFile); err == nil {
								found = true
								break
							}
						}
						if !found {
							path := filepath.ToSlash(filepath.Join(hourDir, fmt.Sprintf("%02d.json.*", minute)))
							slog.Debug("Missing minute file", "path", path)
							missing.Minutes = append(missing.Minutes, path)
						}
					}
				}
			}
		}
	}

	slog.Info("Summary report finished")
	fmt.Println("\n📊 Summary Report")
	fmt.Printf("Missing Months (%d): %v\n", len(missing.Months), missing.Months)
	fmt.Printf("Missing Days (%d): %v\n", len(missing.Days), missing.Days)
	fmt.Printf("Missing Hours (%d): %v\n", len(missing.Hours), missing.Hours)
	fmt.Printf("Missing Minutes (%d): %v\n", len(missing.Minutes), missing.Minutes)
}

// getCompressionWriter returns the correct compression writer and file extension.
func getCompressionWriter(w io.Writer, compressionType string) (io.WriteCloser, string, error) {
	switch compressionType {
	case "gzip":
		return gzip.NewWriter(w), ".gz", nil
	case "zstd":
		writer, err := zstd.NewWriter(w)
		return writer, ".zst", err
	case "lz4":
		return lz4.NewWriter(w), ".lz4", nil
	default:
		return nil, "", fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}

// aggregateMessages uses a streaming approach to aggregate files.
func aggregateMessages(inputDir, outputDir, aggregateFrom, aggregateTo, outputCompression string, inputCompression []string) {
	slog.Info("Starting message aggregation",
		"input_dir", inputDir,
		"output_dir", outputDir,
		"from", aggregateFrom,
		"to", aggregateTo,
		"output_compression", outputCompression,
		"input_compression", inputCompression,
	)

	fromLevels := map[string]int{"minute": 5, "hour": 4, "day": 3, "month": 2, "year": 1}
	toLevels := map[string]int{"hour": 4, "day": 3, "month": 2, "year": 1}

	fromLevel, fromOK := fromLevels[aggregateFrom]
	toLevel, toOK := toLevels[aggregateTo]

	if !fromOK || !toOK {
		slog.Error("Invalid 'from' or 'to' aggregation level specified", "from", aggregateFrom, "to", aggregateTo)
		os.Exit(1)
	}
	if fromLevel <= toLevel {
		slog.Error("'aggregate-from' level must be deeper than 'aggregate-to' level", "from", aggregateFrom, "to", aggregateTo)
		os.Exit(1)
	}

	// Define which compression types to look for. Default to all if none specified.
	if len(inputCompression) == 0 {
		inputCompression = []string{"gzip", "zstd", "lz4"}
	}
	slog.Debug("Will look for input files with types", "types", inputCompression)
	// Create a set of allowed canonical compression types (e.g. "gzip")
	allowedTypes := make(map[string]bool)
	for _, t := range inputCompression {
		allowedTypes[t] = true
	}
	// Map of file extensions to their canonical compression type name
	extToType := map[string]string{
		".gz":   "gzip",
		".gzip": "gzip",
		".zst":  "zstd",
		".lz4":  "lz4",
	}

	filesToProcess := make(map[string][]string)
	err := filepath.WalkDir(inputDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			slog.Warn("Error accessing path, skipping", "path", path, "error", err)
			return err
		}
		if d.IsDir() {
			return nil
		}

		ext := filepath.Ext(path)
		canonicalType, typeFound := extToType[ext]
		// Skip if the file extension is not a supported compression format OR
		// if it's not in the list of types we're allowed to process.
		if !typeFound || !allowedTypes[canonicalType] {
			return nil
		}

		relPath, err := filepath.Rel(inputDir, path)
		if err != nil {
			return err
		}

		// Clean up the path to get the parts for level calculation
		// e.g., 2025/06/11/10.json.gz -> [2025, 06, 11, 10]
		trimmedPath := strings.TrimSuffix(relPath, ".json"+ext)
		pathParts := strings.Split(trimmedPath, string(filepath.Separator))

		if len(pathParts) == fromLevel {
			slog.Debug("Found source file at correct aggregation level", "path", path, "level", len(pathParts))
			key := filepath.Join(pathParts[:toLevel]...)
			filesToProcess[key] = append(filesToProcess[key], path)
			slog.Debug("Queued file for processing", "key", key, "path", path)
		}
		return nil
	})
	if err != nil {
		slog.Error("Failed to walk input directory", "path", inputDir, "error", err)
		os.Exit(1)
	}

	slog.Info("File scan complete. Starting aggregation streaming.", "groups", len(filesToProcess))

	for key, filePaths := range filesToProcess {
		_, fileExt, err := getCompressionWriter(io.Discard, outputCompression)
		if err != nil {
			slog.Error("Invalid output compression type", "type", outputCompression, "error", err)
			continue
		}
		outputFile := filepath.Join(outputDir, key+".json"+fileExt)
		slog.Info("Aggregating group", "key", key, "output_file", outputFile, "file_count", len(filePaths))

		if err := ensureDirectoryExists(filepath.Dir(outputFile)); err != nil {
			slog.Error("Failed to create output directory", "path", filepath.Dir(outputFile), "error", err)
			continue
		}

		outFile, err := os.Create(outputFile)
		if err != nil {
			slog.Error("Failed to create output file", "path", outputFile, "error", err)
			continue
		}

		compWriter, _, _ := getCompressionWriter(outFile, outputCompression)
		jsonEncoder := json.NewEncoder(compWriter)
		jsonEncoder.SetIndent("", "  ")

		if _, err := compWriter.Write([]byte("[\n")); err != nil {
			slog.Error("Failed to write JSON opening bracket", "path", outputFile, "error", err)
			outFile.Close()
			continue
		}

		var totalMessages int64 = 0
		isFirstMessageInArray := true

		for _, filePath := range filePaths {
			messages, err := loadMessagesFromFile(filePath)
			if err != nil {
				slog.Warn("Could not load or parse file, skipping", "path", filePath, "error", err)
				continue
			}

			for _, msg := range messages {
				if !isFirstMessageInArray {
					if _, err := compWriter.Write([]byte(",\n")); err != nil {
						slog.Error("Failed to write JSON separator", "path", outputFile, "error", err)
						break
					}
				}
				if err := jsonEncoder.Encode(msg); err != nil {
					slog.Warn("Failed to encode message to output stream", "path", filePath, "error", err)
					continue
				}
				isFirstMessageInArray = false
				totalMessages++
			}
		}

		if _, err := compWriter.Write([]byte("\n]")); err != nil {
			slog.Error("Failed to write JSON closing bracket", "path", outputFile, "error", err)
		}

		if err := compWriter.Close(); err != nil {
			slog.Error("Failed to close compression writer", "path", outputFile, "error", err)
		}
		if err := outFile.Close(); err != nil {
			slog.Error("Failed to close output file", "path", outputFile, "error", err)
		}
		slog.Info("Finished aggregating group", "key", key, "total_messages", totalMessages)
	}
	slog.Info("Aggregation process finished.")
}

// uploadDirectoryToBlob uploads all files in a directory to an Azure Blob Storage container.
func uploadDirectoryToBlob(inputDir, storageAccountName, containerName, accessKey string) {
	slog.Info("Starting Azure Blob Storage upload",
		"input_dir", inputDir,
		"storage_account", storageAccountName,
		"container", containerName,
	)

	// 1. Create a credential object
	cred, err := azblob.NewSharedKeyCredential(storageAccountName, accessKey)
	if err != nil {
		slog.Error("Invalid credentials", "error", err)
		os.Exit(1)
	}

	// 2. Create a client
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", storageAccountName)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		slog.Error("Failed to create Azure Blob client", "error", err)
		os.Exit(1)
	}

	// 3. Walk the directory and upload files
	err = filepath.WalkDir(inputDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			slog.Warn("Error accessing path, skipping", "path", path, "error", err)
			return err
		}
		if d.IsDir() {
			return nil // Skip directories
		}

		// Determine the blob name from the relative path
		blobName, err := filepath.Rel(inputDir, path)
		if err != nil {
			slog.Warn("Could not determine relative path, skipping", "path", path, "error", err)
			return nil
		}
		// Use forward slashes for blob names
		blobName = filepath.ToSlash(blobName)

		slog.Debug("Preparing to upload file", "local_path", path, "blob_name", blobName)

		// Open the local file
		file, err := os.Open(path)
		if err != nil {
			slog.Warn("Failed to open file for upload, skipping", "path", path, "error", err)
			return nil
		}
		defer file.Close()

		// Upload the file to Azure Blob Storage
		_, err = client.UploadFile(context.Background(), containerName, blobName, file, &azblob.UploadFileOptions{})
		if err != nil {
			slog.Error("Failed to upload blob", "blob_name", blobName, "error", err)
			return nil // Continue walking even if one file fails
		}

		slog.Info("Successfully uploaded file", "local_path", path, "blob_name", blobName)
		return nil
	})

	if err != nil {
		slog.Error("Failed during directory walk for upload", "path", inputDir, "error", err)
		os.Exit(1)
	}

	slog.Info("Azure Blob Storage upload process finished.")
}


// ---- Cobra Commands Setup ----

func main() {
	var rootCmd = &cobra.Command{
		Use:   "sumo-tool",
		Short: "A tool for managing and analyzing Sumo Logic message files.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			logLevel, _ := cmd.Flags().GetString("log-level")
			var leveler slog.Level
			switch strings.ToLower(logLevel) {
			case "debug":
				leveler = slog.LevelDebug
			case "info":
				leveler = slog.LevelInfo
			case "warn":
				leveler = slog.LevelWarn
			case "error":
				leveler = slog.LevelError
			default:
				return fmt.Errorf("invalid log level: %s. Use debug, info, warn, or error", logLevel)
			}
			handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: leveler})
			slog.SetDefault(slog.New(handler))
			return nil
		},
	}
	rootCmd.PersistentFlags().String("log-level", "info", "Set the logging level (debug, info, warn, error)")

	var summaryCmd = &cobra.Command{
		Use:   "summary",
		Short: "Generate a summary report for specific ranges.",
		Run: func(cmd *cobra.Command, args []string) {
			slog.Info("Executing 'summary' command")
			inputDir, _ := cmd.Flags().GetString("input-dir")
			yearRange, _ := cmd.Flags().GetIntSlice("year-range")
			monthRange, _ := cmd.Flags().GetIntSlice("month-range")
			dayRange, _ := cmd.Flags().GetIntSlice("day-range")
			generateSummaryReport(inputDir, [2]int{yearRange[0], yearRange[1]}, [2]int{monthRange[0], monthRange[1]}, [2]int{dayRange[0], dayRange[1]})
		},
	}
	summaryCmd.Flags().String("input-dir", "", "Directory containing the input files (required)")
	summaryCmd.MarkFlagRequired("input-dir")
	summaryCmd.Flags().IntSlice("year-range", []int{}, "Start and end year (inclusive) (required)")
	summaryCmd.MarkFlagRequired("year-range")
	summaryCmd.Flags().IntSlice("month-range", []int{1, 12}, "Start and end month (inclusive)")
	summaryCmd.Flags().IntSlice("day-range", []int{1, 31}, "Start and end day (inclusive)")

	var aggregateCmd = &cobra.Command{
		Use:   "aggregate",
		Short: "Aggregate messages into larger files.",
		Run: func(cmd *cobra.Command, args []string) {
			slog.Info("Executing 'aggregate' command")
			inputDir, _ := cmd.Flags().GetString("input-dir")
			outputDir, _ := cmd.Flags().GetString("output-dir")
			aggregateFrom, _ := cmd.Flags().GetString("aggregate-from")
			aggregateTo, _ := cmd.Flags().GetString("aggregate-to")
			outputCompression, _ := cmd.Flags().GetString("output-compression")
			inputCompression, _ := cmd.Flags().GetStringSlice("input-compression")
			aggregateMessages(inputDir, outputDir, aggregateFrom, aggregateTo, outputCompression, inputCompression)
		},
	}
	aggregateCmd.Flags().String("input-dir", "", "Directory containing the input files (required)")
	aggregateCmd.MarkFlagRequired("input-dir")
	aggregateCmd.Flags().String("output-dir", "", "Directory to save the aggregated files (required)")
	aggregateCmd.MarkFlagRequired("output-dir")
	aggregateCmd.Flags().String("aggregate-from", "minute", "Level to aggregate files from (minute, hour, day, month)")
	aggregateCmd.Flags().String("aggregate-to", "", "Level to aggregate messages to (hour, day, month, year) (required)")
	aggregateCmd.MarkFlagRequired("aggregate-to")
	aggregateCmd.Flags().String("output-compression", "", "Output compression format (gzip, zstd, lz4) (required)")
	aggregateCmd.MarkFlagRequired("output-compression")
	aggregateCmd.Flags().StringSlice("input-compression", []string{}, "Comma-separated list of input formats to search for (gzip, zstd, lz4). Defaults to all.")

	// -- Upload Command --
	var uploadCmd = &cobra.Command{
		Use:   "upload",
		Short: "Upload a directory to an Azure Blob Storage container.",
		Run: func(cmd *cobra.Command, args []string) {
			slog.Info("Executing 'upload' command")
			inputDir, _ := cmd.Flags().GetString("input-dir")
			accountName, _ := cmd.Flags().GetString("storage-account-name")
			containerName, _ := cmd.Flags().GetString("blob-container-name")
			accessKey, _ := cmd.Flags().GetString("access-key")
			uploadDirectoryToBlob(inputDir, accountName, containerName, accessKey)
		},
	}
	uploadCmd.Flags().String("input-dir", "", "Directory containing the files to upload (required)")
	uploadCmd.MarkFlagRequired("input-dir")
	uploadCmd.Flags().String("storage-account-name", "", "Azure Storage account name (required)")
	uploadCmd.MarkFlagRequired("storage-account-name")
	uploadCmd.Flags().String("blob-container-name", "", "Azure Blob Storage container name (required)")
	uploadCmd.MarkFlagRequired("blob-container-name")
	uploadCmd.Flags().String("access-key", "", "Azure Storage account access key (required)")
	uploadCmd.MarkFlagRequired("access-key")

	rootCmd.AddCommand(summaryCmd, aggregateCmd, uploadCmd)
	if err := rootCmd.Execute(); err != nil {
		slog.Error("Command execution failed", "error", err)
		os.Exit(1)
	}
}

