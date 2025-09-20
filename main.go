package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
)

var logger *slog.Logger

const (
	// bucardoLogPath is the default location for the Bucardo log file inside the container.
	bucardoLogPath = "/var/log/bucardo/log.bucardo"
	// bucardoConfigPath is the expected location of the user-provided configuration file.
	bucardoConfigPath = "/media/bucardo/bucardo.json"
	// pgpassPath is the location of the .pgpass file for the postgres user.
	pgpassPath = "/var/lib/postgresql/.pgpass"
	// bucardoUser is the system user that runs Bucardo commands.
	bucardoUser = "postgres"
	// bucardoCmd is the name of the Bucardo executable.
	bucardoCmd = "bucardo"
)

// BucardoConfig represents the top-level structure of the bucardo.json file.
type BucardoConfig struct {
	Databases []Database `json:"databases"`           // A list of all databases involved in replication.
	Syncs     []Sync     `json:"syncs"`               // A list of all synchronization tasks to be configured.
	LogLevel  string     `json:"log_level,omitempty"` // Global log level for Bucardo (e.g., "VERBOSE", "DEBUG").
}

// Database defines a PostgreSQL database connection for Bucardo.
type Database struct {
	ID     int    `json:"id"`             // A unique integer to identify this database within the config.
	DBName string `json:"dbname"`         // The name of the database.
	Host   string `json:"host"`           // The database host.
	User   string `json:"user"`           // The username for the connection.
	Pass   string `json:"pass"`           // The password, or "env" to use an environment variable.
	Port   *int   `json:"port,omitempty"` // The database port. Pointer is used to handle optionality.
}

// Sync defines a Bucardo synchronization task, detailing what to replicate from where to where.
type Sync struct {
	Name                  string `json:"name"`                               // The unique name for this sync.
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
	Cron                  string `json:"cron,omitempty"`                     // A cron expression (e.g., "0 2 * * *") for scheduled runs.
}

// runCommand executes a shell command and prints its output.
// The optional logCmd allows for a custom command string for logging.
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

// runBucardoCommand is a helper to execute a `bucardo` command as the 'postgres' user.
func runBucardoCommand(args ...string) error {
	// Construct the command that will be executed inside the `su -c` shell.
	bucardoCmdWithArgs := bucardoCmd + " " + strings.Join(args, " ")

	// Execute `su - postgres -c "bucardo arg1 arg2 ..."` but pass arguments safely.
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
	// Wait for Bucardo to be ready, with a timeout.
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

// validateConfig performs a pre-check of the entire configuration to catch common errors
// before any commands are run. It returns a list of all validation errors found.
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

// setupPgpassFile creates or appends to a .pgpass file for the postgres user.
// This is the most robust way to handle passwords with special characters.
func setupPgpassFile(db Database, password string) error {
	// Format: hostname:port:database:username:password
	// Use '*' for port/database to match any.
	port := "*"
	if db.Port != nil {
		port = fmt.Sprintf("%d", *db.Port)
	}
	pgpassEntry := fmt.Sprintf("%s:%s:%s:%s:%s\n", db.Host, port, db.DBName, db.User, password)

	// The .pgpass file must be owned by the user running the command (postgres)
	// and have permissions 0600. We write it to the postgres user's home directory.
	f, err := os.OpenFile(pgpassPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("failed to open .pgpass file: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteString(pgpassEntry); err != nil {
		return fmt.Errorf("failed to write to .pgpass file: %w", err)
	}

	// Ensure the file is owned by the postgres user.
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

	// The output format is "Database: <name> ...". We check for this specific pattern.
	searchString := fmt.Sprintf("Database: %s", dbName)
	return strings.Contains(string(output), searchString)
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

		// Build the arguments for bucardo, omitting the password.
		// Bucardo will automatically use the .pgpass file.
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

	// The output format is "Sync "<name>" ...". We check for this specific pattern.
	// Using quotes ensures we don't accidentally match a substring in another sync's name.
	searchString := fmt.Sprintf("Sync \"%s\"", syncName)
	return strings.Contains(string(output), searchString)
}

// getSyncDbgroup finds the name of the dbgroup currently associated with a sync by parsing command output.
func getSyncDbgroup(syncName string) (string, error) {
	// This regex finds the "DB group" label and captures the quoted group name that follows.
	// e.g., from '... DB group "my_group_name" ...' it captures 'my_group_name'.
	re := regexp.MustCompile(`DB group "([^"]+)"`)

	cmd := exec.Command("su", "-", bucardoUser, "-c", fmt.Sprintf("bucardo list sync %s", syncName))
	// Use CombinedOutput to capture both stdout and stderr for better debugging.
	output, err := cmd.CombinedOutput()
	outputStr := string(output)

	if err != nil {
		return "", fmt.Errorf("failed to execute 'bucardo list sync %s': %w. Output: %s", syncName, err, outputStr)
	}

	matches := re.FindStringSubmatch(outputStr)
	if len(matches) > 1 {
		return matches[1], nil // The first capture group is the dbgroup name.
	}

	// If we are here, parsing failed. Log the output for debugging.
	logger.Debug("Bucardo output for 'list sync'", "sync", syncName, "output", outputStr)
	return "", fmt.Errorf("could not find 'DB group \"<name>\"' pattern in output for sync '%s'", syncName)
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

		args := []string{
			command, "sync", sync.Name,
			// Onetimecopy is a common setting to update.
			fmt.Sprintf("onetimecopy=%d", sync.Onetimecopy),
		}

		// --- Handle different sync types: bidirectional takes precedence ---

		if len(sync.Bidirectional) > 0 {
			// Handle bidirectional (multi-master) sync using a dbgroup.
			logger.Info("Configuring bidirectional sync", "dbs", sync.Bidirectional)

			// Create a dbgroup for the bidirectional sync.
			dbgroupName := fmt.Sprintf("bg_%s", sync.Name)
			// Always ensure the dbgroup is up-to-date.
			logger.Info("Re-creating dbgroup to ensure it matches configuration", "name", dbgroupName)
			runBucardoCommand("del", "dbgroup", dbgroupName, "--force")
			dbgroupMembers := make([]string, len(sync.Bidirectional))
			for i, dbID := range sync.Bidirectional {
				dbgroupMembers[i] = fmt.Sprintf("db%d:source", dbID)
			}
			runBucardoCommand(append([]string{"add", "dbgroup", dbgroupName}, dbgroupMembers...)...)

			args = append(args, fmt.Sprintf("dbs=%s", dbgroupName))

			// Tables can only be set on 'add'.
			if command == "add" && sync.Tables != "" {
				args = append(args, fmt.Sprintf("tables=%s", sync.Tables))
			} else if command == "update" && sync.Tables != "" {
				logger.Warn("The 'tables' property cannot be changed on an existing sync. This setting will be ignored.", "sync", sync.Name)
			}

		} else if len(sync.Sources) > 0 && len(sync.Targets) > 0 {
			// Handle standard source -> target sync
			if command == "add" {
				// For 'add', we can pass the list of dbs directly.
				var dbStrings []string
				for _, sourceID := range sync.Sources {
					dbStrings = append(dbStrings, fmt.Sprintf("db%d:source", sourceID))
				}
				for _, targetID := range sync.Targets {
					dbStrings = append(dbStrings, fmt.Sprintf("db%d:target", targetID))
				}
				dbsArg := strings.Join(dbStrings, ",")
				args = append(args, fmt.Sprintf("dbs=%s", dbsArg))
			} else { // command == "update"
				// The "swap and replace" method for updating a dbgroup without data loss.
				// 0. Find the name of the dbgroup currently used by the sync.
				oldDbgroupName, err := getSyncDbgroup(sync.Name)
				if err != nil {
					logger.Error("Failed to determine current dbgroup for sync", "name", sync.Name, "error", err)
					os.Exit(1)
				}

				// 1. Create a new, temporary dbgroup with the correct members.
				tempDbgroupName := fmt.Sprintf("sg_%s_%d", sync.Name, time.Now().Unix())
				logger.Info("Creating temporary dbgroup for sync update", "name", tempDbgroupName)
				var dbgroupArgs []string
				for _, sourceID := range sync.Sources {
					dbgroupArgs = append(dbgroupArgs, fmt.Sprintf("db%d:source", sourceID))
				}
				for _, targetID := range sync.Targets {
					dbgroupArgs = append(dbgroupArgs, fmt.Sprintf("db%d:target", targetID))
				}
				runBucardoCommand(append([]string{"add", "dbgroup", tempDbgroupName}, dbgroupArgs...)...)

				// 2. Atomically update the sync to use the new dbgroup.
				args = append(args, fmt.Sprintf("dbs=%s", tempDbgroupName))

				// Defer the cleanup of the old dbgroup until after the sync update command succeeds.
				defer func(groupToDelete string) {
					// This check is important. If the old group was the same as the new one (unlikely but possible), don't delete it.
					if groupToDelete != tempDbgroupName {
						logger.Info("Cleaning up old dbgroup", "name", groupToDelete)
						if err := runBucardoCommand("del", "dbgroup", groupToDelete, "--force"); err != nil {
							logger.Warn("Failed to clean up old dbgroup", "name", groupToDelete, "error", err)
						}
					}
				}(oldDbgroupName)
			}

			// For standard syncs, we need either a herd or a list of tables.
			// These can only be set on 'add'.
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
		}

		// Add optional parameters to the `add sync` command.
		if sync.ExitOnComplete != nil && *sync.ExitOnComplete {
			// stayalive=0 and kidsalive=0 ensure the sync does not persist after its initial run.
			args = append(args, "stayalive=0", "kidsalive=0")
		}
		if sync.Cron != "" {
			// autokick=0 prevents Bucardo's internal scheduler from running this sync.
			// Our external cron scheduler will be responsible for kicking it.
			args = append(args, "autokick=0")
		}
		if sync.StrictChecking != nil {
			args = append(args, fmt.Sprintf("strict_checking=%t", *sync.StrictChecking))
		}
		if sync.ConflictStrategy != "" {
			args = append(args, fmt.Sprintf("conflict_strategy=%s", sync.ConflictStrategy))
		}

		// Execute the command and apply customizations
		if err := runBucardoCommand(args...); err != nil {
			logger.Error("Failed to modify sync", "action", command, "name", sync.Name, "error", err)
			os.Exit(1)
		}
	}
}

// streamBucardoLog starts a `tail -F` command to stream the Bucardo log file to the container's stdout.
// It returns the command object so the caller can manage its lifecycle (e.g., kill it on shutdown).
func streamBucardoLog() *exec.Cmd {
	// Wait a moment for the log file to be created by Bucardo.
	time.Sleep(2 * time.Second)

	cmd := exec.Command("tail", "-F", bucardoLogPath)
	// Create a new process group for tail. This allows us to kill the entire process group,
	// ensuring no orphaned `tail` processes are left running.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	logger.Info("Streaming Bucardo log file", "path", bucardoLogPath)
	if err := cmd.Start(); err != nil {
		logger.Warn("Could not start streaming Bucardo log file", "error", err)
		return nil
	}

	// Return the command so its process can be managed.
	return cmd
}

// startBucardo starts the main Bucardo process.
func startBucardo() {
	logger.Info("Starting Bucardo...")
	// Before starting, ensure any stale Bucardo processes from an unclean shutdown are stopped.
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
		// The absence of the MCP PID file is the most reliable way to confirm a clean stop.
		if _, err := os.Stat("/var/run/bucardo/bucardo.mcp.pid"); os.IsNotExist(err) {
			logger.Info("Bucardo has stopped.")
			return
		}
		time.Sleep(1 * time.Second)
	}

	logger.Error("Bucardo did not stop gracefully within timeout. The process may be hung.", "timeout", shutdownTimeout)
}

// monitorBucardo handles the default long-running mode. It streams logs and waits for a
// termination signal (SIGINT, SIGTERM) to gracefully shut down Bucardo.
func monitorBucardo() {
	tailCmd := streamBucardoLog()
	if tailCmd != nil && tailCmd.Process != nil {
		defer func() {
			logger.Info("Stopping log streamer...")
			// Kill the process group to ensure tail and any children are stopped.
			syscall.Kill(-tailCmd.Process.Pid, syscall.SIGKILL)
		}()
	}

	// Set up a channel to listen for termination signals.
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

// monitorSyncs handles the "run-once" mode. It tails the Bucardo log, watches for completion
// messages for specific syncs, and manages a timeout. If all syncs are run-once, it shuts
// down the container upon completion. Otherwise, it transitions to long-running mode.
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

	// Wait a moment for the log file to be created by Bucardo.
	time.Sleep(5 * time.Second)

	cmd := exec.Command("tail", "-F", bucardoLogPath)

	// Get a pipe to the command's stdout to read the log lines.
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logger.Error("Could not create pipe for tail command", "error", err)
		os.Exit(1)
	}
	cmd.Stderr = os.Stderr

	// Create a context that we can cancel to stop the log scanning goroutine.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancellation happens on function exit
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		logger.Error("Could not start log tailing command", "error", err)
		os.Exit(1)
	}
	// Set up the timeout if specified
	if maxTimeout != nil && *maxTimeout > 0 {
		timeoutDuration := time.Duration(*maxTimeout) * time.Second
		logger.Info("Setting a timeout for run-once sync completion", "timeout", timeoutDuration)
		timeoutChannel = time.After(timeoutDuration)
	}

	// Goroutine to read from the stdout pipe and send lines to a channel.
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

	// Main monitoring loop.
	for {
		select {
		case line, ok := <-lineChan:
			if !ok {
				logger.Info("Log streaming finished unexpectedly.")
				// Kill the tail process group and wait for it to exit.
				syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				cmd.Wait()
				return
			}
			fmt.Println(line) // Stream the log line to the container's stdout.

			// Check for the completion message from the logs, e.g., "KID (sync0) Kid ... exiting at cleanup_kid.  Reason: Normal exit"
			if strings.Contains(line, "exiting at cleanup_kid") && strings.Contains(line, "Reason: Normal exit") {
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

			// If we were waiting for syncs and now the map is empty, we are done.
			if len(runOnceSyncs) == 0 {
				logger.Info("All monitored syncs have completed.")
				if allSyncsAreRunOnce {
					logger.Info("All configured syncs were run-once. Shutting down container.")
					cancel()                                        // Stop the scanner goroutine.
					syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // Kill the tail process group.
					stopBucardo()
					return // Success.
				} else {
					// Some syncs were run-once, but others are long-running. Switch to standard monitoring.
					logger.Info("Other syncs are still running. Switching to standard monitoring mode.")
					cancel()
					syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					monitorBucardo() // Hand off to the long-running monitor
					return
				}
			}
		case <-timeoutChannel:
			logger.Error("Timeout reached for run-once syncs", "timeout_seconds", *maxTimeout, "incomplete_syncs", getMapKeys(runOnceSyncs))
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // Kill the tail process group.
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
			// Log as a warning as Bucardo can still run with the default log level.
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

// CronJob represents a sync that is scheduled to run via a cron expression.
type CronJob struct {
	SyncName       string
	CronExpression string
	ExitOnComplete bool
	Timeout        *int
}

// runCronScheduler sets up and runs a cron scheduler for syncs with a 'cron' property.
func runCronScheduler(config *BucardoConfig, jobs []CronJob) {
	c := cron.New()
	var runOnceJob *CronJob // Special handling for the first job with exit_on_complete

	for i := range jobs {
		job := jobs[i] // Create a local copy for the closure

		// If a job is marked for exit, we only schedule it to run ONCE at its next designated time.
		if job.ExitOnComplete {
			if runOnceJob == nil {
				runOnceJob = &job
			} else {
				logger.Warn("Multiple cron syncs have 'exit_on_complete: true'. Only the first one will cause an exit.", "first_sync", runOnceJob.SyncName)
			}
			continue // Don't add to the recurring scheduler
		}

		// This is a recurring cron job.
		_, err := c.AddFunc(job.CronExpression, func() {
			logger.Info("Kicking sync as per schedule", "sync", job.SyncName, "schedule", job.CronExpression)
			// We don't specify a timeout for recurring jobs, letting Bucardo handle it.
			if err := runBucardoCommand("kick", job.SyncName, "0"); err != nil {
				logger.Error("Failed to kick sync", "sync", job.SyncName, "error", err)
			}
		})
		if err != nil {
			logger.Error("Failed to schedule cron job for sync. This sync will not run.", "sync", job.SyncName, "error", err)
		}
	}

	// If there's a run-once job, handle it separately.
	if runOnceJob != nil {
		schedule, err := cron.ParseStandard(runOnceJob.CronExpression)
		if err != nil {
			logger.Error("Invalid cron expression for sync", "sync", runOnceJob.SyncName, "error", err)
			os.Exit(1)
		}

		nextRun := schedule.Next(time.Now())
		logger.Info("Scheduled run-once job. Container will exit upon completion.", "sync", runOnceJob.SyncName, "next_run", nextRun.Format(time.RFC1123))

		// Wait until the next scheduled time, but listen for shutdown signals.
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		timer := time.NewTimer(time.Until(nextRun))

		select {
		case <-timer.C:
			// Time to run the job.
		case sig := <-sigs:
			logger.Info("Received signal while waiting for scheduled job. Shutting down.", "signal", sig)
			stopBucardo()
			os.Exit(0)
		}

		// Kick the sync.
		logger.Info("Kicking one-time sync", "sync", runOnceJob.SyncName)
		kickTimeout := "0" // Bucardo's default timeout
		if runOnceJob.Timeout != nil {
			kickTimeout = fmt.Sprintf("%d", *runOnceJob.Timeout)
		}
		if err := runBucardoCommand("kick", runOnceJob.SyncName, kickTimeout); err != nil {
			logger.Error("Failed to kick sync. The container will now exit.", "sync", runOnceJob.SyncName, "error", err)
			os.Exit(1)
		}

		// Now, monitor this single sync for completion.
		// We create a temporary config and sync map for monitorSyncs.
		syncsToWatch := map[string]bool{runOnceJob.SyncName: true}
		monitorSyncs(config, syncsToWatch, runOnceJob.Timeout)
		logger.Info("Run-once job complete. Exiting.")
		return // Exit the application.
	}

	// If we are here, there were no run-once jobs, so we run the scheduler indefinitely.
	if len(c.Entries()) > 0 {
		logger.Info("Starting cron scheduler for recurring sync(s).", "count", len(c.Entries()))
		c.Start()

		// Keep the application alive while the cron scheduler runs in the background.
		// Also stream logs.
		monitorBucardo()
		log.Println("[CRON] Shutting down cron scheduler.")
		<-c.Stop().Done()
	} else {
		logger.Info("No cron jobs were scheduled. The container will now exit as there is nothing to do.")
	}
}

// setupAllPgpass creates a single .pgpass file containing credentials for all databases.
func setupAllPgpass(config *BucardoConfig) {
	// Ensure we start with a clean slate.
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

func main() {
	// Use a structured logger for better log management.
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

	// Validate the configuration before proceeding.
	if validationErrors := validateConfig(config); len(validationErrors) > 0 {
		logger.Error("Invalid configuration found in bucardo.json")
		for _, e := range validationErrors {
			logger.Error(e.Error())
		}
		logger.Error("Please fix the configuration errors and restart the container.")
		os.Exit(1)
	}

	// Main startup sequence.
	startPostgres()
	setLogLevel(config)

	// Create the .pgpass file with all credentials. It will be used by subsequent bucardo commands.
	setupAllPgpass(config)
	// Defer the cleanup to ensure the password file is removed after all configuration is done.
	defer os.Remove(pgpassPath)

	addDatabasesToBucardo(config)
	addSyncsToBucardo(config)
	startBucardo()

	// --- Determine execution mode: Cron, Run-Once, or Standard Long-Running ---

	runOnceSyncs := make(map[string]bool)
	cronJobs := []CronJob{}
	var maxTimeout *int

	for _, sync := range config.Syncs {
		if sync.ExitOnComplete != nil && *sync.ExitOnComplete {
			runOnceSyncs[sync.Name] = true
			if sync.ExitOnCompleteTimeout != nil {
				if maxTimeout == nil || *sync.ExitOnCompleteTimeout > *maxTimeout {
					// Use the largest timeout specified across all run-once syncs.
					timeoutVal := *sync.ExitOnCompleteTimeout
					maxTimeout = &timeoutVal
				}
			}
		}

		if sync.Cron != "" {
			cronJobs = append(cronJobs, CronJob{
				SyncName:       sync.Name,
				CronExpression: sync.Cron,
				ExitOnComplete: sync.ExitOnComplete != nil && *sync.ExitOnComplete,
				Timeout:        sync.ExitOnCompleteTimeout,
			})
		}
	}

	// Priority 1: If there are cron jobs, the cron scheduler takes over.
	if len(cronJobs) > 0 {
		runCronScheduler(config, cronJobs)
		// Priority 2: If there are non-cron run-once syncs, monitor them for completion.
	} else if len(runOnceSyncs) > 0 {
		monitorSyncs(config, runOnceSyncs, maxTimeout)
		// Priority 3: Otherwise, enter the standard long-running mode.
	} else {
		monitorBucardo()
	}
}
