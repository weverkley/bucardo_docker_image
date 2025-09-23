// bucardo-docker-entrypoint is the main application for the Bucardo Docker image.
// It provides a declarative, configuration-driven way to manage Bucardo replication.
package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

// logger is the global structured logger for the application.
var logger *slog.Logger

const (
	// bucardoLogPath is the location of the Bucardo log file.
	bucardoLogPath = "/var/log/bucardo/log.bucardo"
	// bucardoConfigPath is the location of the user-provided JSON configuration file.
	bucardoConfigPath = "/media/bucardo/bucardo.json"
	// pgpassPath is the location of the .pgpass file used for database authentication.
	pgpassPath = "/var/lib/postgresql/.pgpass"
	// bucardoUser is the system user that runs all Bucardo commands.
	bucardoUser = "postgres"
	// bucardoCmd is the Bucardo executable name.
	bucardoCmd = "bucardo"
)

// BucardoConfig represents the top-level structure of the bucardo.json file.
type BucardoConfig struct {
	Databases []Database `json:"databases"`
	Syncs     []Sync     `json:"syncs"`
	LogLevel  string     `json:"log_level,omitempty"`
}

// Database defines a PostgreSQL database connection for Bucardo.
type Database struct {
	ID     int    `json:"id"`
	DBName string `json:"dbname"`
	Host   string `json:"host"`
	User   string `json:"user"`
	Pass   string `json:"pass"`
	Port   *int   `json:"port,omitempty"`
}

// Sync defines a Bucardo synchronization task, detailing what to replicate from where to where.
type Sync struct {
	Name                  string `json:"name"`
	Sources               []int  `json:"sources,omitempty"`                  // A list of database IDs to use as sources.
	Targets               []int  `json:"targets,omitempty"`                  // A list of database IDs to use as targets.
	Bidirectional         []int  `json:"bidirectional,omitempty"`            // A list of database IDs for bidirectional (dbgroup) replication.
	Herd                  string `json:"herd,omitempty"`                     // The name of a herd (group) to sync all tables from the first source.
	Tables                string `json:"tables,omitempty"`                   // A comma-separated list of specific tables to sync.
	Onetimecopy           int    `json:"onetimecopy"`                        // Controls full-copy behavior (0=off, 1=always, 2=if target empty).
	StrictChecking        *bool  `json:"strict_checking,omitempty"`          // If false, allows schema differences like column order.
	ExitOnComplete        *bool  `json:"exit_on_complete,omitempty"`         // If true, the container will exit after this sync completes.
	ExitOnCompleteTimeout *int   `json:"exit_on_complete_timeout,omitempty"` // Timeout in seconds for run-once syncs.
	ConflictStrategy      string `json:"conflict_strategy,omitempty"`        // Defines how to resolve data conflicts (e.g., "bucardo_source").
}

// runCommand executes a shell command, streaming its stdout and stderr.
func runCommand(logCmd, name string, arg ...string) error {
	cmd := exec.Command(name, arg...)
	if logCmd == "" {
		logCmd = cmd.String()
	}
	logger.Info("Running command", "command", logCmd)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// runBucardoCommand executes a `bucardo` command as the configured bucardoUser.
func runBucardoCommand(args ...string) error {
	bucardoCmdWithArgs := bucardoCmd + " " + strings.Join(args, " ")

	return runCommand(bucardoCmdWithArgs, "su", "-", bucardoUser, "-c", bucardoCmdWithArgs)
}

// startPostgres starts the PostgreSQL service and waits for Bucardo to be ready.
func startPostgres() {
	logger.Info("Starting PostgreSQL...")
	if err := runCommand("", "service", "postgresql", "start"); err != nil {
		logger.Error("Failed to start postgresql service", "error", err)
		os.Exit(1)
	}

	logger.Info("Waiting for Bucardo to be ready...")
	const readinessTimeout = 2 * time.Minute
	deadline := time.Now().Add(readinessTimeout)

	for time.Now().Before(deadline) {
		// We check the output of `bucardo status` to see if it's ready.
		cmd := exec.Command("su", "-", bucardoUser, "-c", "bucardo status")
		if err := cmd.Run(); err == nil {
			logger.Info("Bucardo is ready.")
			return
		}
		time.Sleep(5 * time.Second)
	}
	logger.Error("Bucardo did not become ready within timeout", "timeout", readinessTimeout)
	os.Exit(1)
}

// loadConfig reads and parses the bucardo.json file.
func loadConfig() (*BucardoConfig, error) {
	jsonFile, err := os.Open(bucardoConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", bucardoConfigPath, err)
	}
	defer jsonFile.Close()

	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", bucardoConfigPath, err)
	}

	var config BucardoConfig
	if err := json.Unmarshal(byteValue, &config); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", bucardoConfigPath, err)
	}

	return &config, nil
}

// validateConfig performs a pre-check of the configuration to catch common errors
// before any commands are run, returning a list of all errors found.
func validateConfig(config *BucardoConfig) []error {
	var errors []error
	dbIDs := make(map[int]bool)
	syncNames := make(map[string]bool)

	for _, db := range config.Databases {
		if dbIDs[db.ID] {
			errors = append(errors, fmt.Errorf("database ID %d is duplicated", db.ID))
		}
		dbIDs[db.ID] = true
	}

	for _, sync := range config.Syncs {
		if sync.Name == "" {
			errors = append(errors, fmt.Errorf("a sync is missing the required 'name' property"))
			continue // Can't validate this sync further
		}
		if syncNames[sync.Name] {
			errors = append(errors, fmt.Errorf("sync name '%s' is duplicated", sync.Name))
		}
		syncNames[sync.Name] = true

		if len(sync.Bidirectional) > 0 {
			if len(sync.Bidirectional) < 2 {
				errors = append(errors, fmt.Errorf("sync '%s': 'bidirectional' requires at least two database IDs", sync.Name))
			}
			for _, id := range sync.Bidirectional {
				if !dbIDs[id] {
					errors = append(errors, fmt.Errorf("sync '%s': 'bidirectional' database ID %d is not defined in the 'databases' list", sync.Name, id))
				}
			}
			// Bucardo source/target strategies are invalid for multi-master syncs.
			if sync.ConflictStrategy == "bucardo_source" || sync.ConflictStrategy == "bucardo_target" {
				errors = append(errors, fmt.Errorf("sync '%s': invalid conflict_strategy '%s' for a bidirectional sync. Use 'bucardo_latest' instead", sync.Name, sync.ConflictStrategy))
			}
		} else { // Standard source/target sync
			if len(sync.Sources) == 0 {
				errors = append(errors, fmt.Errorf("sync '%s': must have at least one source", sync.Name))
			}
			if len(sync.Targets) == 0 {
				errors = append(errors, fmt.Errorf("sync '%s': must have at least one target", sync.Name))
			}
			if sync.Herd == "" && sync.Tables == "" {
				errors = append(errors, fmt.Errorf("sync '%s': must define either 'herd' or 'tables'", sync.Name))
			}
		}

		if sync.ConflictStrategy != "" {
			validStrategies := map[string]bool{
				"bucardo_source": true,
				"bucardo_target": true,
				"bucardo_skip":   true,
				"bucardo_random": true,
				"bucardo_latest": true,
				"bucardo_abort":  true,
			}
			if !validStrategies[sync.ConflictStrategy] {
				validKeys := make([]string, 0, len(validStrategies))
				for k := range validStrategies {
					validKeys = append(validKeys, k)
				}
				errors = append(errors, fmt.Errorf("sync '%s': invalid conflict_strategy '%s'. Must be one of: %v", sync.Name, sync.ConflictStrategy, validKeys))
			}
		}
	}

	return errors
}

// getDbPassword resolves the database password, fetching from an environment variable if `pass` is set to "env".
func getDbPassword(db Database) (string, error) {
	if db.Pass == "env" {
		envVar := fmt.Sprintf("BUCARDO_DB%d", db.ID)
		password := os.Getenv(envVar)
		if password == "" {
			return "", fmt.Errorf("environment variable %s not set for db id %d", envVar, db.ID)
		}
		return password, nil
	}
	return db.Pass, nil
}

// setupPgpassFile appends a new entry to the .pgpass file. This is the most
// robust way to handle passwords with special characters for libpq.
func setupPgpassFile(db Database, password string) error {
	// Format: hostname:port:database:username:password
	// Use '*' for port/database to match any.
	port := "*"
	if db.Port != nil {
		port = fmt.Sprintf("%d", *db.Port)
	}
	pgpassEntry := fmt.Sprintf("%s:%s:%s:%s:%s\n", db.Host, port, db.DBName, db.User, password)

	f, err := os.OpenFile(pgpassPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("failed to open .pgpass file: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteString(pgpassEntry); err != nil {
		return fmt.Errorf("failed to write to .pgpass file: %w", err)
	}

	// Ensure the file is owned by the bucardoUser.
	return runCommand("", "chown", fmt.Sprintf("%s:%s", bucardoUser, bucardoUser), pgpassPath)
}

// databaseExists checks if a Bucardo database with the given name already exists.
func databaseExists(dbName string) bool {
	cmd := exec.Command("su", "-", bucardoUser, "-c", "bucardo list dbs")
	output, err := cmd.Output()
	if err != nil {
		log.Printf("[WARNING] Could not list Bucardo databases to check for existence: %v", err)
		return false
	}

	// The output format is "Database: <name> ...".
	searchString := fmt.Sprintf("Database: %s", dbName)
	return strings.Contains(string(output), searchString)
}

// listBucardoDbs returns a slice of all database names currently configured in Bucardo.
func listBucardoDbs() ([]string, error) {
	re := regexp.MustCompile(`Database: (\S+)`)

	cmd := exec.Command("su", "-", bucardoUser, "-c", "bucardo list dbs")
	output, err := cmd.CombinedOutput()
	outputStr := string(output)

	if err != nil {
		if strings.Contains(outputStr, "No databases found") {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to execute 'bucardo list dbs': %w. Output: %s", err, outputStr)
	}

	matches := re.FindAllStringSubmatch(outputStr, -1)
	if matches == nil {
		return []string{}, nil // No databases found
	}

	var dbs []string
	for _, match := range matches {
		dbs = append(dbs, match[1])
	}
	return dbs, nil
}

// addDatabasesToBucardo iterates through the config and adds each database to Bucardo's configuration.
func addDatabasesToBucardo(config *BucardoConfig) {
	logger.Info("Adding/updating databases in Bucardo...")

	for _, db := range config.Databases {
		dbName := fmt.Sprintf("db%d", db.ID)
		exists := databaseExists(dbName)
		command := "add"

		if exists {
			command = "update"
			logger.Info("Updating existing db", "name", dbName, "id", db.ID, "host", db.Host)
		} else {
			logger.Info("Adding new db", "name", dbName, "id", db.ID, "host", db.Host)
		}

		_, err := getDbPassword(db)
		if err != nil {
			logger.Error("Error getting password for db", "id", db.ID, "error", err)
			os.Exit(1)
		}

		// Build the arguments for bucardo; the password will be picked up from .pgpass.
		args := []string{
			command, "db", dbName,
			fmt.Sprintf("dbname=%s", db.DBName),
			fmt.Sprintf("host=%s", db.Host),
			fmt.Sprintf("user=%s", db.User),
		}
		if db.Port != nil {
			args = append(args, fmt.Sprintf("port=%d", *db.Port))
		}
		if err := runBucardoCommand(args...); err != nil {
			logger.Error("Failed to modify database", "action", command, "name", dbName, "error", err)
			os.Exit(1)
		}
	}
}

// syncExists checks if a Bucardo sync with the given name already exists.
func syncExists(syncName string) bool {
	// The `bucardo list sync <name>` command has an unreliable exit code (always 0).
	// A more reliable method is to list all syncs and check if the name is present.
	cmd := exec.Command("su", "-", bucardoUser, "-c", "bucardo list syncs")
	output, err := cmd.Output()
	if err != nil {
		// If the command itself fails, we can't determine existence. Log and assume it doesn't exist.
		logger.Warn("Could not list Bucardo syncs to check for existence", "error", err)
		return false
	}

	// The output format is 'Sync "<name>" ...'. Using quotes prevents partial matches.
	searchString := fmt.Sprintf("Sync \"%s\"", syncName)
	return strings.Contains(string(output), searchString)
}

// listBucardoSyncs returns a slice of all sync names currently configured in Bucardo.
func listBucardoSyncs() ([]string, error) {
	re := regexp.MustCompile(`Sync "([^"]+)"`)

	cmd := exec.Command("su", "-", bucardoUser, "-c", "bucardo list syncs")
	output, err := cmd.CombinedOutput()
	outputStr := string(output)

	if err != nil {
		if strings.Contains(outputStr, "No syncs found") {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to execute 'bucardo list syncs': %w. Output: %s", err, outputStr)
	}

	matches := re.FindAllStringSubmatch(outputStr, -1)
	if matches == nil {
		return []string{}, nil // No syncs found
	}

	var syncs []string
	for _, match := range matches {
		syncs = append(syncs, match[1])
	}
	return syncs, nil
}

// addSyncsToBucardo configures the replication tasks (syncs) in Bucardo based on the JSON config.
func addSyncsToBucardo(config *BucardoConfig) {
	logger.Info("Adding syncs to Bucardo...")
	for _, sync := range config.Syncs {
		exists := syncExists(sync.Name)
		command := "add"
		if exists {
			command = "update"
			logger.Info("Updating existing sync", "name", sync.Name)
		} else {
			logger.Info("Adding new sync", "name", sync.Name)
		}

		args := []string{command, "sync", sync.Name}

		// Onetimecopy should only be set on creation, not on every update.
		if command == "add" {
			args = append(args, fmt.Sprintf("onetimecopy=%d", sync.Onetimecopy))
		}

		if len(sync.Bidirectional) > 0 {
			logger.Info("Configuring bidirectional sync", "dbs", sync.Bidirectional)
			dbgroupName := fmt.Sprintf("bg_%s", sync.Name)
			if command == "add" {
				dbgroupMembers := make([]string, len(sync.Bidirectional))
				for i, dbID := range sync.Bidirectional {
					dbgroupMembers[i] = fmt.Sprintf("db%d:source", dbID)
				}
				runBucardoCommand(append([]string{"add", "dbgroup", dbgroupName}, dbgroupMembers...)...)
				args = append(args, fmt.Sprintf("dbs=%s", dbgroupName))
			} else { // command == "update"
				// To ensure idempotency, we always recreate the dbgroup with a predictable name.
				// First, delete the old one. We don't use --force, so this is safe.
				// It will fail if a sync is using it, but that's okay because we are about to re-assign it.
				// It will succeed if the group is orphaned or doesn't exist.
				runBucardoCommand("del", "dbgroup", dbgroupName)
				dbgroupMembers := make([]string, len(sync.Bidirectional))
				for i, dbID := range sync.Bidirectional {
					dbgroupMembers[i] = fmt.Sprintf("db%d:source", dbID)
				}
				runBucardoCommand(append([]string{"add", "dbgroup", dbgroupName}, dbgroupMembers...)...)
				args = append(args, fmt.Sprintf("dbs=%s", dbgroupName))
			}
		} else if len(sync.Sources) > 0 || len(sync.Targets) > 0 {
			// For source-to-target syncs, we create a dbgroup with a deterministic name
			// based on its members to ensure it's only recreated if the members change.
			var dbStrings []string
			var memberNames []string
			for _, sourceID := range sync.Sources {
				member := fmt.Sprintf("db%d:source", sourceID)
				dbStrings = append(dbStrings, member)
				memberNames = append(memberNames, member)
			}
			for _, targetID := range sync.Targets {
				member := fmt.Sprintf("db%d:target", targetID)
				dbStrings = append(dbStrings, member)
				memberNames = append(memberNames, member)
			}
			// Create a stable hash of the members to use as the group name.
			sort.Strings(memberNames)
			hash := sha1.Sum([]byte(strings.Join(memberNames, ",")))
			dbgroupName := fmt.Sprintf("sg_%s_%x", sync.Name, hash[:4]) // e.g., sg_mysync_a1b2c3d4

			// Unconditionally recreate the group to ensure it's correct.
			runBucardoCommand("del", "dbgroup", dbgroupName)
			runBucardoCommand(append([]string{"add", "dbgroup", dbgroupName}, dbStrings...)...)
			args = append(args, fmt.Sprintf("dbs=%s", dbgroupName))
		}

		// Tables/herds can only be set on 'add', not 'update'.
		if command == "add" && sync.Herd != "" {
			logger.Info("Using herd", "name", sync.Herd)
			sourceDB := fmt.Sprintf("db%d", sync.Sources[0])
			runBucardoCommand("del", "herd", sync.Herd, "--force")
			runBucardoCommand("add", "herd", sync.Herd)
			runBucardoCommand("add", "all", "tables", fmt.Sprintf("--herd=%s", sync.Herd), fmt.Sprintf("db=%s", sourceDB))
			args = append(args, fmt.Sprintf("herd=%s", sync.Herd))
		} else if command == "add" && sync.Tables != "" {
			logger.Info("Using table list")
			args = append(args, fmt.Sprintf("tables=%s", sync.Tables))
		} else if command == "update" && (sync.Herd != "" || sync.Tables != "") {
			logger.Warn("The 'herd' or 'tables' property cannot be changed on an existing sync. This setting will be ignored.", "sync", sync.Name)
		}

		// Add optional parameters to the command.
		if sync.ExitOnComplete != nil && *sync.ExitOnComplete {
			// stayalive=0 and kidsalive=0 ensure the sync does not persist after its initial run.
			args = append(args, "stayalive=0", "kidsalive=0")
		}
		if sync.StrictChecking != nil {
			args = append(args, fmt.Sprintf("strict_checking=%t", *sync.StrictChecking))
		}
		if sync.ConflictStrategy != "" {
			args = append(args, fmt.Sprintf("conflict_strategy=%s", sync.ConflictStrategy))
		}

		// Execute the final bucardo command.
		if err := runBucardoCommand(args...); err != nil {
			logger.Error("Failed to modify sync", "action", command, "name", sync.Name, "error", err)
			os.Exit(1)
		}
	}
}

// streamBucardoLog starts a `tail -F` command to stream the Bucardo log file,
// returning the command object so its lifecycle can be managed.
func streamBucardoLog() *exec.Cmd {
	time.Sleep(2 * time.Second)

	cmd := exec.Command("tail", "-F", bucardoLogPath)
	// Create a new process group to ensure no orphaned `tail` processes are left.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	logger.Info("Streaming Bucardo log file", "path", bucardoLogPath)
	if err := cmd.Start(); err != nil {
		logger.Warn("Could not start streaming Bucardo log file", "error", err)
		return nil
	}

	return cmd
}

// startBucardo starts the main Bucardo process.
func startBucardo() {
	logger.Info("Starting Bucardo...")
	logger.Info("Checking for and stopping any stale Bucardo processes...")
	stopBucardo() // Use our robust stop function.

	if err := runBucardoCommand("start"); err != nil {
		logger.Error("Failed to start bucardo", "error", err)
		os.Exit(1)
	}
}

// stopBucardo gracefully stops the Bucardo service and waits for confirmation.
func stopBucardo() {
	logger.Info("Stopping Bucardo...")
	if err := runBucardoCommand("stop"); err != nil {
		logger.Warn("'bucardo stop' command failed", "error", err)
	}

	logger.Info("Waiting for Bucardo to stop completely...")
	const shutdownTimeout = 30 * time.Second
	deadline := time.Now().Add(shutdownTimeout)

	for time.Now().Before(deadline) {
		// Absence of the MCP PID file confirms a clean stop.
		if _, err := os.Stat("/var/run/bucardo/bucardo.mcp.pid"); os.IsNotExist(err) {
			logger.Info("Bucardo has stopped.")
			return
		}
		time.Sleep(1 * time.Second)
	}

	logger.Error("Bucardo did not stop gracefully within timeout. The process may be hung.", "timeout", shutdownTimeout)
}

// monitorBucardo handles the default long-running mode, streaming logs and waiting
// for a termination signal to gracefully shut down.
func monitorBucardo() {
	tailCmd := streamBucardoLog()
	if tailCmd != nil && tailCmd.Process != nil {
		defer func() {
			logger.Info("Stopping log streamer...")
			// Kill the process group to ensure tail and any children are stopped.
			syscall.Kill(-tailCmd.Process.Pid, syscall.SIGKILL)
		}()
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-sigs:
			logger.Info("Received signal, shutting down.", "signal", sig)
			stopBucardo()
			return
		}
	}
}

// monitorSyncs handles the "run-once" mode by tailing the Bucardo log for completion
// messages. It manages timeouts and container shutdown.
func monitorSyncs(config *BucardoConfig, runOnceSyncs map[string]bool, maxTimeout *int) {
	if config.LogLevel != "VERBOSE" && config.LogLevel != "DEBUG" {
		logger.Warn("'exit_on_complete' is true, but 'log_level' is not 'VERBOSE' or 'DEBUG'.")
		logger.Warn("The completion message may not be logged, and the container might time out or run indefinitely.")
	}

	if len(runOnceSyncs) > 0 {
		logger.Info("Monitoring sync(s) for completion", "count", len(runOnceSyncs), "syncs", getMapKeys(runOnceSyncs))
	}

	allSyncsAreRunOnce := len(config.Syncs) == len(runOnceSyncs)

	var timeoutChannel <-chan time.Time

	time.Sleep(5 * time.Second)

	cmd := exec.Command("tail", "-F", bucardoLogPath)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logger.Error("Could not create pipe for tail command", "error", err)
		os.Exit(1)
	}
	cmd.Stderr = os.Stderr

	ctx, cancel := context.WithCancel(context.Background()) // For stopping the scanner goroutine.
	defer cancel()                                          // Ensure cancellation happens on function exit
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		logger.Error("Could not start log tailing command", "error", err)
		os.Exit(1)
	}
	if maxTimeout != nil && *maxTimeout > 0 {
		timeoutDuration := time.Duration(*maxTimeout) * time.Second
		logger.Info("Setting a timeout for run-once sync completion", "timeout", timeoutDuration)
		timeoutChannel = time.After(timeoutDuration)
	}

	lineChan := make(chan string)
	go func() {
		defer close(lineChan)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			select {
			case lineChan <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case line, ok := <-lineChan:
			if !ok {
				logger.Info("Log streaming finished unexpectedly.")
				syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				cmd.Wait()
				return
			}
			fmt.Println(line) // Stream the log line to the container's stdout.

			// Check for the completion message from the logs, e.g., "KID (sync0) Kid ... exiting at cleanup_kid.  Reason: Normal exit"
			if strings.Contains(line, "Reason: Normal exit") {
				for syncName := range runOnceSyncs {
					if strings.Contains(line, fmt.Sprintf("KID (%s)", syncName)) {
						logger.Info("Completion message for sync detected", "sync", syncName)
						if err := runBucardoCommand("stop", syncName); err != nil {
							logger.Warn("Failed to stop sync", "name", syncName, "error", err)
						}
						// Remove from the map of syncs we are waiting for.
						delete(runOnceSyncs, syncName)
						logger.Info("Run-once sync(s) remaining", "count", len(runOnceSyncs))
					}
				}
			}

			if len(runOnceSyncs) == 0 {
				logger.Info("All monitored syncs have completed.")
				if allSyncsAreRunOnce {
					logger.Info("All configured syncs were run-once. Shutting down container.")
					cancel()                                        // Stop the scanner goroutine.
					syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // Kill the tail process group.
					stopBucardo()
					return // Success.
				} else {
					logger.Info("Other syncs are still running. Switching to standard monitoring mode.")
					cancel()
					syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					monitorBucardo() // Hand off to the long-running monitor
					return
				}
			}
		case <-timeoutChannel:
			logger.Error("Timeout reached for run-once syncs", "timeout_seconds", *maxTimeout, "incomplete_syncs", getMapKeys(runOnceSyncs))
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			stopBucardo()
			os.Exit(1) // Exit with a non-zero status code to indicate failure.
		}
	}
}

// setLogLevel sets the global Bucardo logging level if specified in the config.
func setLogLevel(config *BucardoConfig) {
	if config.LogLevel != "" {
		logger.Info("Setting Bucardo log level", "level", config.LogLevel)
		if err := runBucardoCommand("set", fmt.Sprintf("log_level=%s", config.LogLevel)); err != nil {
			logger.Warn("Failed to set log_level", "error", err)
		}
	}
}

// getMapKeys is a helper function to get keys from a map for logging purposes.
func getMapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// setupAllPgpass creates a single .pgpass file containing credentials for all databases.
func setupAllPgpass(config *BucardoConfig) {
	os.Remove(pgpassPath)

	for _, db := range config.Databases {
		password, err := getDbPassword(db)
		if err != nil {
			logger.Error("Failed to get password for .pgpass setup", "db_id", db.ID, "error", err)
			os.Exit(1)
		}
		if err := setupPgpassFile(db, password); err != nil {
			logger.Error("Failed to write entry to .pgpass file", "db_id", db.ID, "error", err)
			os.Exit(1)
		}
	}
}

// removeOrphanedDbs compares the databases in the config with those in Bucardo
// and removes any databases from Bucardo that are not declared in the config.
func removeOrphanedDbs(config *BucardoConfig) {
	logger.Info("Checking for orphaned databases to remove...")

	// 1. Get all database names declared in the config file (e.g., "db1", "db2").
	configDbs := make(map[string]bool)
	for _, db := range config.Databases {
		dbName := fmt.Sprintf("db%d", db.ID)
		configDbs[dbName] = true
	}

	// 2. Get all database names currently existing in Bucardo.
	bucardoDbs, err := listBucardoDbs()
	if err != nil {
		logger.Error("Could not list existing Bucardo databases for cleanup", "error", err)
		return // Skip cleanup if we can't get the list.
	}

	// 3. Compare and delete any database from Bucardo that is not in the config.
	for _, bucardoDbName := range bucardoDbs {
		if !configDbs[bucardoDbName] {
			logger.Info("Removing orphaned database not found in configuration", "name", bucardoDbName)
			runBucardoCommand("del", "db", bucardoDbName)
		}
	}
}

// removeOrphanedSyncs compares the syncs in the config with those in Bucardo
// and removes any syncs from Bucardo that are not declared in the config.
func removeOrphanedSyncs(config *BucardoConfig) {
	logger.Info("Checking for orphaned syncs to remove...")

	// 1. Get all sync names declared in the config file.
	configSyncs := make(map[string]bool)
	for _, sync := range config.Syncs {
		configSyncs[sync.Name] = true
	}

	// 2. Get all sync names currently existing in Bucardo.
	bucardoSyncs, err := listBucardoSyncs()
	if err != nil {
		logger.Error("Could not list existing Bucardo syncs for cleanup", "error", err)
		return // Skip cleanup if we can't get the list.
	}

	// 3. Compare and delete any sync from Bucardo that is not in the config.
	for _, bucardoSyncName := range bucardoSyncs {
		if !configSyncs[bucardoSyncName] {
			logger.Info("Removing orphaned sync not found in configuration", "name", bucardoSyncName)
			runBucardoCommand("del", "sync", bucardoSyncName)
		}
	}
}

func main() {
	// Use a structured JSON logger for better log management.
	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	if _, err := os.Stat(bucardoConfigPath); os.IsNotExist(err) {
		logger.Error("Configuration file not found. Please mount it as a volume.", "path", bucardoConfigPath)
		os.Exit(1)
	}

	config, err := loadConfig()
	if err != nil {
		logger.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	if validationErrors := validateConfig(config); len(validationErrors) > 0 {
		logger.Error("Invalid configuration found in bucardo.json")
		for _, e := range validationErrors {
			logger.Error(e.Error())
		}
		logger.Error("Please fix the configuration errors and restart the container.")
		os.Exit(1)
	}

	startPostgres()
	setLogLevel(config)

	// Create the .pgpass file with all credentials for subsequent bucardo commands.
	setupAllPgpass(config)
	defer os.Remove(pgpassPath) // Ensure the password file is removed on exit.

	removeOrphanedDbs(config)
	removeOrphanedSyncs(config)
	addDatabasesToBucardo(config)
	addSyncsToBucardo(config)
	startBucardo()

	// --- Determine execution mode: Run-Once or Standard Long-Running ---

	runOnceSyncs := make(map[string]bool)
	var maxTimeout *int

	for _, sync := range config.Syncs {
		if sync.ExitOnComplete != nil && *sync.ExitOnComplete {
			runOnceSyncs[sync.Name] = true
			if sync.ExitOnCompleteTimeout != nil {
				if maxTimeout == nil || *sync.ExitOnCompleteTimeout > *maxTimeout {
					timeoutVal := *sync.ExitOnCompleteTimeout
					maxTimeout = &timeoutVal
				}
			}
		}
	}

	if len(runOnceSyncs) > 0 {
		monitorSyncs(config, runOnceSyncs, maxTimeout)
	} else {
		monitorBucardo()
	}
}
