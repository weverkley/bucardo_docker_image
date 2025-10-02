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
	logger.Info("Running command", "component", "command_runner", "command", logCmd)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get stderr pipe: %w", err)
	}
	cmd.Stdout = os.Stdout

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %w", err)
	}

	// Filter out harmless "No such..." messages from stderr to reduce log noise.
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "No such dbgroup:") && !strings.HasPrefix(line, "No such sync:") {
			fmt.Fprintln(os.Stderr, line)
		}
	}

	return cmd.Wait()
}

// runBucardoCommand executes a `bucardo` command as the configured bucardoUser.
func runBucardoCommand(args ...string) error {
	bucardoCmdWithArgs := bucardoCmd + " " + strings.Join(args, " ")

	return runCommand(bucardoCmdWithArgs, "su", "-", bucardoUser, "-c", bucardoCmdWithArgs)
}

// startPostgres starts the PostgreSQL service and waits for Bucardo to be ready.
func startPostgres() {
	logger.Info("Starting PostgreSQL service", "component", "startup")
	if err := runCommand("", "service", "postgresql", "start"); err != nil {
		logger.Error("Failed to start postgresql service", "error", err)
		os.Exit(1)
	}

	logger.Info("Waiting for Bucardo to be ready...")
	const readinessTimeout = 2 * time.Minute
	deadline := time.Now().Add(readinessTimeout)

	for time.Now().Before(deadline) {
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
			// Source/target conflict strategies are invalid for multi-master syncs.
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
	// Format: hostname:port:database:username:password. Use '*' for port/database to match any.
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

	return runCommand("", "chown", fmt.Sprintf("%s:%s", bucardoUser, bucardoUser), pgpassPath)
}

// databaseExists checks if a Bucardo database with the given name already exists.
func databaseExists(dbName string) bool {
	// The most reliable way to check for existence is to list all dbs and parse the output.
	// `bucardo list dbs <name>` can have ambiguous exit codes.
	allDbs, err := listBucardoDbs()
	if err != nil {
		logger.Warn("Could not list Bucardo databases to check for existence", "error", err)
		return false
	}
	for _, bdb := range allDbs {
		if bdb == dbName {
			return true
		}
	}
	return false
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
		return []string{}, nil
	}

	var dbs []string
	for _, match := range matches {
		dbs = append(dbs, match[1])
	}
	return dbs, nil
}

// addDatabasesToBucardo iterates through the config and adds each database to Bucardo's configuration.
func addDatabasesToBucardo(config *BucardoConfig) {
	appLogger := logger.With("component", "db_reconciler")
	appLogger.Info("Starting database reconciliation")

	for _, db := range config.Databases {
		dbName := fmt.Sprintf("db%d", db.ID)
		dbLogger := appLogger.With("db_name", dbName, "db_id", db.ID, "db_host", db.Host)
		exists := databaseExists(dbName)
		command := "add"

		if exists {
			command = "update"
			dbLogger.Info("Database exists, preparing update")
		} else {
			dbLogger.Info("Database not found, preparing to add")
		}

		_, err := getDbPassword(db)
		if err != nil {
			logger.Error("Error getting password for db", "id", db.ID, "error", err)
			os.Exit(1)
		}

		args := []string{
			command, "dbs", dbName,
			fmt.Sprintf("dbname=%s", db.DBName),
			fmt.Sprintf("host=%s", db.Host),
			fmt.Sprintf("user=%s", db.User),
		}
		if db.Port != nil {
			args = append(args, fmt.Sprintf("port=%d", *db.Port))
		}
		if err := runBucardoCommand(args...); err != nil {
			dbLogger.Error("Failed to modify database", "action", command, "error", err)
			os.Exit(1)
		}
	}
}

// syncExists checks if a Bucardo sync with the given name already exists.
// It returns a boolean for existence and the command's output on success for parsing.
func syncExists(syncName string) (exists bool, stdout []byte) {
	cmd := exec.Command("su", "-", bucardoUser, "-c", fmt.Sprintf("bucardo list sync %s", syncName))
	// We must separate stdout and stderr. Warnings on stderr can break parsing,
	// but the command can still succeed (exit 0).
	var outb, errb strings.Builder
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	err := cmd.Run()

	// The sync exists only if the command succeeded AND produced output, as `list syncs <name>` can exit 0 even if not found.
	stdoutString := outb.String()
	return err == nil && stdoutString != "", []byte(stdoutString)
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
		return []string{}, nil
	}

	var syncs []string
	for _, match := range matches {
		syncs = append(syncs, match[1])
	}
	return syncs, nil
}

// getSyncRelgroup parses the output of `bucardo list sync` to find the relgroup name.
func getSyncRelgroup(syncDetailsOutput []byte) (string, error) {
	re := regexp.MustCompile(`Relgroup: (\S+)`)
	matches := re.FindStringSubmatch(string(syncDetailsOutput))
	if len(matches) < 2 {
		return "", fmt.Errorf("could not find relgroup in sync details")
	}
	return matches[1], nil
}

// getSyncTables fetches the list of tables for a given relgroup from Bucardo.
func getSyncTables(relgroupName string) ([]string, error) {
	if relgroupName == "" {
		return []string{}, nil
	}
	cmd := exec.Command("su", "-", bucardoUser, "-c", fmt.Sprintf("bucardo list relgroup %s --verbose", relgroupName))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to list relgroup %s: %w. Output: %s", relgroupName, err, string(output))
	}

	// The output lists tables one per line, indented. e.g., "  public.users"
	re := regexp.MustCompile(`\s+(\S+\.\S+)`)
	matches := re.FindAllStringSubmatch(string(output), -1)

	if len(matches) == 0 {
		// This can happen if the relgroup is empty or doesn't exist.
		return []string{}, nil
	}

	var tables []string
	for _, match := range matches {
		// The regex ensures we have at least one submatch.
		tableName := match[1]
		// The output may include "(sequence)" or a trailing comma, which must be stripped.
		tableName = strings.TrimRight(tableName, ",")
		tableName = strings.TrimSpace(strings.Split(tableName, "(")[0])
		if tableName != "" {
			tables = append(tables, tableName)
		}
	}

	// Sort for consistent string comparison.
	sort.Strings(tables)
	return tables, nil
}

// addSyncsToBucardo configures the replication tasks (syncs) in Bucardo based on the JSON config.
func addSyncsToBucardo(config *BucardoConfig) {
	appLogger := logger.With("component", "sync_reconciler")
	appLogger.Info("Starting sync reconciliation")

	for _, sync := range config.Syncs {
		syncLogger := appLogger.With("sync_name", sync.Name)
		exists, syncDetailsOutput := syncExists(sync.Name)

		if exists {
			// Logic for existing syncs: Check for table changes before deciding to update or recreate.
			if sync.Tables != "" {
				// We must get the relgroup name from the sync details, as it can be different from the sync name.
				relgroupName, err := getSyncRelgroup(syncDetailsOutput)
				if err != nil {
					// This can happen if the sync is inactive. As a fallback, assume the relgroup name matches the sync name.
					syncLogger.Debug("Could not parse relgroup from sync details, falling back to sync name.", "error", err)
					relgroupName = sync.Name
				}

				currentTables, err := getSyncTables(relgroupName)
				if err != nil {
					// If we can't get the tables, it's safer to assume no change and proceed with a safe update.
					syncLogger.Warn("Could not get tables for relgroup, cannot compare. Assuming no change.", "relgroup", relgroupName, "error", err)
				}

				configTablesRaw := strings.Split(sync.Tables, ",")
				configTables := make([]string, 0, len(configTablesRaw))
				for _, t := range configTablesRaw {
					configTables = append(configTables, strings.TrimSpace(t))
				}
				sort.Strings(configTables)

				if strings.Join(currentTables, ",") != strings.Join(configTables, ",") {
					syncLogger.Warn("Table list for sync has changed. This requires a destructive re-creation.", "current_tables", currentTables, "new_tables", configTables)
					syncLogger.Warn("WARNING: Any pending changes for this sync that have not been replicated will be lost.")
					syncLogger.Info("Re-creating sync to apply new table configuration.")
					// The sync must be deleted before the relgroup to avoid foreign key violations.
					runBucardoCommand("del", "sync", sync.Name)
					// Now it's safe to delete the orphaned relgroup to prevent Bucardo from creating a `_2` suffixed one.
					runBucardoCommand("del", "relgroup", relgroupName)
					// Fall through to the 'add' logic.
				} else {
					syncLogger.Info("Sync exists and tables are unchanged. Applying non-destructive update.")
					updateArgs := []string{"update", "sync", sync.Name}
					if sync.StrictChecking != nil {
						updateArgs = append(updateArgs, fmt.Sprintf("strict_checking=%t", *sync.StrictChecking))
					}
					if sync.ConflictStrategy != "" {
						updateArgs = append(updateArgs, fmt.Sprintf("conflict_strategy=%s", sync.ConflictStrategy))
					}
					runBucardoCommand(updateArgs...)
					continue
				}
			}
		}

		syncLogger.Info("Preparing to add sync.")
		command := "add"
		args := []string{command, "sync", sync.Name, fmt.Sprintf("onetimecopy=%d", sync.Onetimecopy)}

		if len(sync.Bidirectional) > 0 {
			syncLogger.Info("Configuring as bidirectional sync", "dbs", sync.Bidirectional)
			dbgroupName := fmt.Sprintf("bg_%s", sync.Name)
			dbgroupMembers := make([]string, len(sync.Bidirectional))
			for i, dbID := range sync.Bidirectional {
				dbgroupMembers[i] = fmt.Sprintf("db%d:source", dbID)
			}
			runBucardoCommand("del", "dbgroup", dbgroupName)
			runBucardoCommand(append([]string{"add", "dbgroup", dbgroupName}, dbgroupMembers...)...)
			args = append(args, fmt.Sprintf("dbs=%s", dbgroupName))
		} else {
			syncLogger.Info("Configuring as source-to-target sync")
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
			sort.Strings(memberNames)
			hash := sha1.Sum([]byte(strings.Join(memberNames, ",")))
			dbgroupName := fmt.Sprintf("sg_%s_%x", sync.Name, hash[:4])

			runBucardoCommand("del", "dbgroup", dbgroupName)
			runBucardoCommand(append([]string{"add", "dbgroup", dbgroupName}, dbStrings...)...)
			args = append(args, fmt.Sprintf("dbs=%s", dbgroupName))
		}

		if sync.Herd != "" {
			syncLogger.Info("Configuring with herd", "herd_name", sync.Herd)
			sourceDB := fmt.Sprintf("db%d", sync.Sources[0])
			runBucardoCommand("del", "herd", sync.Herd, "--force")
			runBucardoCommand("add", "herd", sync.Herd)
			runBucardoCommand("add", "all", "tables", fmt.Sprintf("--herd=%s", sync.Herd), fmt.Sprintf("db=%s", sourceDB))
			args = append(args, fmt.Sprintf("herd=%s", sync.Herd))
		} else if sync.Tables != "" {
			syncLogger.Info("Configuring with table list")
			args = append(args, fmt.Sprintf("tables=%s", sync.Tables))
		}

		if sync.ExitOnComplete != nil && *sync.ExitOnComplete {
			args = append(args, "stayalive=0", "kidsalive=0")
		}
		if sync.StrictChecking != nil {
			args = append(args, fmt.Sprintf("strict_checking=%t", *sync.StrictChecking))
		}
		if sync.ConflictStrategy != "" {
			args = append(args, fmt.Sprintf("conflict_strategy=%s", sync.ConflictStrategy))
		}

		if err := runBucardoCommand(args...); err != nil {
			syncLogger.Error("Failed to modify sync", "action", command, "error", err)
			os.Exit(1)
		}
	}
}

// streamBucardoLog starts a `tail -F` command to stream the Bucardo log file,
// returning the command object so its lifecycle can be managed.
func streamBucardoLog() *exec.Cmd {
	time.Sleep(2 * time.Second)

	cmd := exec.Command("tail", "-F", bucardoLogPath)
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
	logger.Info("Starting main Bucardo service", "component", "bucardo_service")
	logger.Info("Checking for and stopping any stale Bucardo processes...")
	stopBucardo()

	if err := runBucardoCommand("start"); err != nil {
		logger.Error("Failed to start bucardo", "error", err)
		os.Exit(1)
	}
}

// stopBucardo gracefully stops the Bucardo service and waits for confirmation.
func stopBucardo() {
	logger.Info("Stopping main Bucardo service", "component", "bucardo_service")
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
			logger.Info("Stopping log streamer", "component", "log_streamer")
			syscall.Kill(-tailCmd.Process.Pid, syscall.SIGKILL)
		}()
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-sigs:
			logger.Info("Received signal, shutting down gracefully", "component", "shutdown", "signal", sig)
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
		appLogger := logger.With("component", "run_once_monitor")
		appLogger.Info("Monitoring sync(s) for completion", "count", len(runOnceSyncs), "syncs", getMapKeys(runOnceSyncs))
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		logger.Error("Could not start log tailing command for run-once monitor", "error", err)
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
			if !ok { // Channel closed
				logger.Info("Log streaming finished unexpectedly.")
				syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				cmd.Wait()
				return
			}
			fmt.Println(line) // Stream the log line to the container's stdout.

			if strings.Contains(line, "Reason: Normal exit") {
				for syncName := range runOnceSyncs {
					syncLogger := logger.With("component", "run_once_monitor", "sync_name", syncName)
					if strings.Contains(line, fmt.Sprintf("KID (%s)", syncName)) {
						syncLogger.Info("Completion message for sync detected")
						if err := runBucardoCommand("stop", syncName); err != nil {
							syncLogger.Warn("Failed to stop sync after completion", "error", err)
						}
						delete(runOnceSyncs, syncName)
						logger.Info("Run-once sync(s) remaining", "count", len(runOnceSyncs))
					}
				}
			}

			if len(runOnceSyncs) == 0 {
				logger.Info("All monitored syncs have completed.")
				if allSyncsAreRunOnce {
					logger.Info("All configured syncs were run-once. Shutting down container.")
					cancel()
					syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					stopBucardo()
					return // Success.
				} else { // Otherwise, hand off to the standard long-running monitor.
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
			os.Exit(1)
		}
	}
}

// setLogLevel sets the global Bucardo logging level if specified in the config.
func setLogLevel(config *BucardoConfig) {
	if config.LogLevel != "" {
		logger.Info("Setting Bucardo global log level", "component", "config", "level", config.LogLevel)
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
	appLogger := logger.With("component", "cleanup")
	appLogger.Info("Checking for orphaned databases to remove")

	// Get all database names declared in the config file (e.g., "db1", "db2").
	configDbs := make(map[string]bool)
	for _, db := range config.Databases {
		dbName := fmt.Sprintf("db%d", db.ID)
		configDbs[dbName] = true
	}

	// 2. Get all database names currently existing in Bucardo.
	bucardoDbs, err := listBucardoDbs()
	if err != nil {
		appLogger.Error("Could not list existing Bucardo databases for cleanup", "error", err)
		return // Skip cleanup if we can't get the list.
	}

	// Compare and delete any database from Bucardo that is not in the config.
	for _, bucardoDbName := range bucardoDbs {
		if !configDbs[bucardoDbName] {
			appLogger.Info("Removing orphaned database not found in configuration", "db_name", bucardoDbName)
			runBucardoCommand("del", "dbs", bucardoDbName)
		}
	}
}

// removeOrphanedSyncs compares the syncs in the config with those in Bucardo
// and removes any syncs from Bucardo that are not declared in the config.
func removeOrphanedSyncs(config *BucardoConfig) {
	appLogger := logger.With("component", "cleanup")
	appLogger.Info("Checking for orphaned syncs to remove")

	// Get all sync names declared in the config file.
	configSyncs := make(map[string]bool)
	for _, sync := range config.Syncs {
		configSyncs[sync.Name] = true
	}

	// 2. Get all sync names currently existing in Bucardo.
	bucardoSyncs, err := listBucardoSyncs()
	if err != nil {
		appLogger.Error("Could not list existing Bucardo syncs for cleanup", "error", err)
		return // Skip cleanup if we can't get the list.
	}

	// Compare and delete any sync from Bucardo that is not in the config.
	for _, bucardoSyncName := range bucardoSyncs {
		if !configSyncs[bucardoSyncName] {
			appLogger.Info("Removing orphaned sync not found in configuration", "sync_name", bucardoSyncName)

			// To perform a clean removal, we must also delete the associated relgroup.
			// First, get the sync details to find the relgroup name.
			exists, syncDetailsOutput := syncExists(bucardoSyncName)
			if !exists {
				continue // Sync disappeared, nothing to do.
			}

			relgroupName, err := getSyncRelgroup(syncDetailsOutput)
			if err != nil {
				// If we can't find the relgroup, it might not have one. Fallback to assuming the name is the same.
				relgroupName = bucardoSyncName
			}

			runBucardoCommand("del", "sync", bucardoSyncName)
			runBucardoCommand("del", "relgroup", relgroupName)
		}
	}
}

func main() {
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

	setupAllPgpass(config)
	defer os.Remove(pgpassPath)

	removeOrphanedDbs(config)
	removeOrphanedSyncs(config)
	addDatabasesToBucardo(config)
	addSyncsToBucardo(config)
	startBucardo()

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
