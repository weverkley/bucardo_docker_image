package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const bucardoLogPath = "/var/log/bucardo/log.bucardo"

const bucardoConfigPath = "/media/bucardo/bucardo.json"

// BucardoConfig represents the structure of bucardo.json
type BucardoConfig struct {
	Databases []Database `json:"databases"`
	Syncs     []Sync     `json:"syncs"`
	LogLevel  string     `json:"log_level,omitempty"`
}

// Database defines a database connection for Bucardo
type Database struct {
	ID     int    `json:"id"`
	DBName string `json:"dbname"`
	Host   string `json:"host"`
	User   string `json:"user"`
	Pass   string `json:"pass"`
	Port   *int   `json:"port,omitempty"` // Use a pointer to handle optional port
}

// Sync defines a Bucardo synchronization task
type Sync struct {
	Sources               []int  `json:"sources"`
	Targets               []int  `json:"targets"`
	Herd                  string `json:"herd,omitempty"`
	Tables                string `json:"tables,omitempty"`
	Onetimecopy           int    `json:"onetimecopy"`
	StrictChecking        *bool  `json:"strict_checking,omitempty"`
	ExitOnComplete        *bool  `json:"exit_on_complete,omitempty"`
	ExitOnCompleteTimeout *int   `json:"exit_on_complete_timeout,omitempty"`
	ConflictStrategy      string `json:"conflict_strategy,omitempty"`
}

// runCommand executes a shell command and prints its output.
// The optional logCmd allows for a custom command string for logging.
func runCommand(logCmd, name string, arg ...string) error {
	cmd := exec.Command(name, arg...)
	if logCmd == "" {
		logCmd = cmd.String()
	}
	log.Printf("Running command: %s", logCmd)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// runBucardoCommand executes a bucardo command as the 'postgres' user.
func runBucardoCommand(args ...string) error {
	bucardoCmdStr := fmt.Sprintf("bucardo %s", strings.Join(args, " "))
	fullArgs := []string{"-", "postgres", "-c", bucardoCmdStr}
	return runCommand(bucardoCmdStr, "su", fullArgs...)
}

// startPostgres starts the PostgreSQL service and waits for Bucardo to be ready.
func startPostgres() {
	log.Println("[CONTAINER] Starting PostgreSQL...")
	if err := runCommand("", "service", "postgresql", "start"); err != nil {
		log.Fatalf("Failed to start postgresql service: %v", err)
	}

	log.Println("[CONTAINER] Waiting for Bucardo to be ready...")
	// Wait for Bucardo to be ready, with a timeout.
	const readinessTimeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), readinessTimeout)
	defer cancel()

	for ctx.Err() == nil {
		// We check the output of `bucardo status` to see if it's ready.
		cmd := exec.Command("su", "-", "postgres", "-c", "bucardo status")
		if err := cmd.Run(); err == nil {
			log.Println("[CONTAINER] Bucardo is ready.")
			return
		}
		time.Sleep(5 * time.Second)
	}
	log.Fatalf("[ERROR] Bucardo did not become ready within %v.", readinessTimeout)
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

// getDbPassword resolves the database password, fetching from environment variables if needed.
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

// addDatabasesToBucardo configures the databases in Bucardo.
func addDatabasesToBucardo(config *BucardoConfig) {
	log.Println("[CONTAINER] Adding databases to Bucardo...")
	for _, db := range config.Databases {
		log.Printf("[CONTAINER] Adding db %s (id: %d)", db.DBName, db.ID)

		dbName := fmt.Sprintf("db%d", db.ID)
		runBucardoCommand("del", "db", dbName, "--force")

		password, err := getDbPassword(db)
		if err != nil {
			log.Fatalf("Error getting password for db id %d: %v", db.ID, err)
		}

		args := []string{
			"add", "db", dbName,
			"--force",
			fmt.Sprintf("dbname=%s", db.DBName),
			fmt.Sprintf("user=%s", db.User),
			fmt.Sprintf("pass=%s", password),
			fmt.Sprintf("host=%s", db.Host),
		}

		if db.Port != nil {
			args = append(args, fmt.Sprintf("port=%d", *db.Port))
		}

		if err := runBucardoCommand(args...); err != nil {
			log.Fatalf("Failed to add database %s: %v", dbName, err)
		}
	}
}

// applySyncCustomizations applies advanced sync settings using 'bucardo update'.
func applySyncCustomizations(sync Sync, syncName string) {
	// Set the conflict_strategy if specified.
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
			log.Printf("[WARNING] Invalid conflict_strategy '%s' for sync '%s'. Skipping.", sync.ConflictStrategy, syncName)
			return
		}

		log.Printf("[CONTAINER] Setting conflict_strategy=%s for sync '%s'", sync.ConflictStrategy, syncName)
		if err := runBucardoCommand("update", "sync", syncName, fmt.Sprintf("conflict_strategy=%s", sync.ConflictStrategy)); err != nil {
			log.Printf("[WARNING] Failed to set conflict_strategy for sync '%s': %v", syncName, err)
		}

	}
}

// addSyncsToBucardo configures the syncs in Bucardo.
func addSyncsToBucardo(config *BucardoConfig) {
	log.Println("[CONTAINER] Adding syncs to Bucardo...")
	for i, sync := range config.Syncs {
		syncName := fmt.Sprintf("sync%d", i)
		log.Printf("[CONTAINER] Adding %s to Bucardo...", syncName)

		runBucardoCommand("del", "sync", syncName, "--force")

		var dbStrings []string
		for _, sourceID := range sync.Sources {
			dbStrings = append(dbStrings, fmt.Sprintf("db%d:source", sourceID))
		}
		for _, targetID := range sync.Targets {
			dbStrings = append(dbStrings, fmt.Sprintf("db%d:target", targetID))
		}
		dbsArg := strings.Join(dbStrings, ",")

		// Base arguments for adding a sync
		args := []string{
			"add", "sync", syncName,
			fmt.Sprintf("dbs=%s", dbsArg),
			fmt.Sprintf("onetimecopy=%d", sync.Onetimecopy),
		}

		if sync.Herd != "" {
			log.Printf("[CONTAINER] Using herd: %s", sync.Herd)
			if len(sync.Sources) == 0 {
				log.Fatalf("Sync %s uses a herd but has no sources defined.", syncName)
			}
			sourceDB := fmt.Sprintf("db%d", sync.Sources[0])

			runBucardoCommand("del", "herd", sync.Herd, "--force")
			runBucardoCommand("add", "herd", sync.Herd)
			runBucardoCommand("add", "all", "tables", fmt.Sprintf("--herd=%s", sync.Herd), fmt.Sprintf("db=%s", sourceDB))

			args = append(args, fmt.Sprintf("herd=%s", sync.Herd))
		} else if sync.Tables != "" {
			log.Println("[CONTAINER] Using table list")
			trimmedTables := strings.ToLower(strings.TrimSpace(sync.Tables))
			if trimmedTables == "all" || trimmedTables == "*" {
				log.Fatalf("Error in sync '%s': The 'tables' field cannot be 'all' or '*'. To sync all tables from a source, please use the 'herd' option instead. See the README for more details.", syncName)
			}
			args = append(args, fmt.Sprintf("tables=%s", sync.Tables))
		} else {
			log.Printf("[WARNING] Sync %s has no 'herd' or 'tables' defined. Skipping.", syncName)
			continue
		}

		// Add optional parameters
		if sync.ExitOnComplete != nil && *sync.ExitOnComplete {
			args = append(args, "stayalive=0", "kidsalive=0")
		}
		if sync.StrictChecking != nil {
			args = append(args, fmt.Sprintf("strict_checking=%t", *sync.StrictChecking))
		}

		// Execute the command and apply customizations
		if err := runBucardoCommand(args...); err != nil {
			log.Fatalf("Failed to add sync %s: %v", syncName, err)
		}
		applySyncCustomizations(sync, syncName)
	}
}

// streamBucardoLog tails the Bucardo log file and streams it to stdout.
// It returns the command object so the caller can manage its lifecycle.
func streamBucardoLog() *exec.Cmd {
	// Wait a moment for the log file to be created by Bucardo.
	time.Sleep(2 * time.Second)

	// Use `tail -f` to stream the log file.
	cmd := exec.Command("tail", "-F", bucardoLogPath)
	// Create a new process group for tail. This allows us to kill it and its children.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	log.Printf("[CONTAINER] Streaming Bucardo log file: %s", bucardoLogPath)
	if err := cmd.Start(); err != nil {
		log.Printf("[WARNING] Could not start streaming Bucardo log file: %v", err)
		return nil
	}

	// Return the command so the caller can wait for it or kill it.
	return cmd
}

// startBucardo starts the main Bucardo process.
func startBucardo() {
	log.Println("[CONTAINER] Starting Bucardo...")

	// Before starting, ensure no stale Bucardo processes are running.
	// This can happen if the container was stopped uncleanly.
	// We ignore the error because it will fail if Bucardo is not running, which is the desired state.
	log.Println("[CONTAINER] Checking for and stopping any stale Bucardo processes...")
	runBucardoCommand("stop")
	// Give it a moment to stop
	time.Sleep(2 * time.Second)

	if err := runBucardoCommand("start"); err != nil {
		log.Fatalf("Failed to start bucardo: %v", err)
	}
}

// stopBucardo stops the Bucardo service and waits for it to shut down.
func stopBucardo() {
	log.Println("[CONTAINER] Stopping Bucardo...")
	// The output of this command will be streamed by runBucardoCommand.
	if err := runBucardoCommand("stop"); err != nil {
		// Log as a warning because we are shutting down anyway.
		log.Printf("[WARNING] 'bucardo stop' command failed: %v", err)
	}

	log.Println("[CONTAINER] Waiting for Bucardo to stop completely...")
	for {
		// The most reliable way to check if Bucardo has stopped is to see if its
		// PID file has been removed. `bucardo status` can return a 0 exit code
		// even when the daemon is not running.
		if _, err := os.Stat("/var/run/bucardo/bucardo.mcp.pid"); os.IsNotExist(err) {
			log.Println("[CONTAINER] Bucardo has stopped.")
			return
		}
		time.Sleep(1 * time.Second)
	}
}

// monitorBucardo handles long-running mode, streaming logs and waiting for a termination signal.
func monitorBucardo() {
	tailCmd := streamBucardoLog()
	if tailCmd != nil && tailCmd.Process != nil {
		defer func() {
			log.Println("[CONTAINER] Stopping log streamer...")
			// Kill the process group to ensure tail and any children are stopped.
			syscall.Kill(-tailCmd.Process.Pid, syscall.SIGKILL)
		}()
	}

	// Set up a channel to listen for termination signals.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// log.Println("[CONTAINER] Checking Bucardo status...")
			// if err := runBucardoCommand("status"); err != nil {
			// 	log.Printf("[WARNING] 'bucardo status' command failed: %v", err)
			// }

		case sig := <-sigs:
			log.Printf("Received signal %s, shutting down.", sig)
			stopBucardo()
			return
		}
	}
}

// monitorSyncs tails the Bucardo log, waits for completion messages for "run-once" syncs,
// and stops them individually. If all syncs are "run-once", it exits the container after they finish.
func monitorSyncs(config *BucardoConfig, runOnceSyncs map[string]bool, maxTimeout *int) {
	if config.LogLevel != "VERBOSE" && config.LogLevel != "DEBUG" {
		log.Printf("[WARNING] 'exit_on_complete' is true, but 'log_level' is not 'VERBOSE' or 'DEBUG'.")
		log.Printf("[WARNING] The completion message may not be logged, and the container might time out or run indefinitely.")
	}

	if len(runOnceSyncs) > 0 {
		log.Printf("[CONTAINER] Monitoring %d sync(s) for completion: %v", len(runOnceSyncs), getMapKeys(runOnceSyncs))
	}

	allSyncsAreRunOnce := len(config.Syncs) == len(runOnceSyncs)

	var timeoutChannel <-chan time.Time

	// Wait a moment for the log file to be created by Bucardo.
	time.Sleep(5 * time.Second)

	cmd := exec.Command("tail", "-F", bucardoLogPath)

	// Get a pipe to the command's stdout
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("[ERROR] Could not create pipe for tail command: %v", err)
	}
	cmd.Stderr = os.Stderr

	// Create a context that we can cancel to stop the tail command
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancellation happens on function exit
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		log.Fatalf("[ERROR] Could not start tail command: %v", err)
	}
	// Set up the timeout if specified
	if maxTimeout != nil && *maxTimeout > 0 {
		timeoutDuration := time.Duration(*maxTimeout) * time.Second
		log.Printf("[CONTAINER] Setting a timeout of %v for run-once sync completion.", timeoutDuration)
		timeoutChannel = time.After(timeoutDuration)
	}

	// Channel to receive log lines
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

	// Read from the pipe line by line
	for {
		select {
		case line, ok := <-lineChan:
			if !ok {
				log.Println("[CONTAINER] Log streaming finished unexpectedly.")
				// Kill the tail process group
				syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				cmd.Wait()
				return
			}
			fmt.Println(line) // Print the log line to the container's stdout

			// Check for the completion message from the logs, e.g., "KID (sync0) Kid ... exiting at cleanup_kid.  Reason: Normal exit"
			if strings.Contains(line, "exiting at cleanup_kid") && strings.Contains(line, "Reason: Normal exit") {
				for syncName := range runOnceSyncs {
					if strings.Contains(line, fmt.Sprintf("KID (%s)", syncName)) {
						log.Printf("[CONTAINER] Completion message for sync '%s' detected.", syncName)
						if err := runBucardoCommand("stop", syncName); err != nil {
							log.Printf("[WARNING] Failed to stop sync %s: %v", syncName, err)
						}
						// Remove from the map of syncs we are waiting for
						delete(runOnceSyncs, syncName)
						log.Printf("[CONTAINER] %d run-once sync(s) remaining.", len(runOnceSyncs))
					}
				}
			}

			// If we were waiting for syncs and now the map is empty, we are done.
			if len(runOnceSyncs) == 0 {
				log.Println("[CONTAINER] All monitored syncs have completed.")
				if allSyncsAreRunOnce {
					log.Println("[CONTAINER] All configured syncs were run-once. Shutting down container.")
					cancel()                                        // Stop the scanner goroutine
					syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // Kill the tail process group
					stopBucardo()
					return // Success
				} else {
					// Not all syncs were run-once, so we switch to standard monitoring.
					log.Println("[CONTAINER] Other syncs are still running. Switching to standard monitoring mode.")
					cancel()
					syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					monitorBucardo() // Hand off to the long-running monitor
					return
				}
			}
		case <-timeoutChannel:
			log.Printf("[ERROR] Timeout reached. The following syncs did not complete: %v", getMapKeys(runOnceSyncs))
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // Kill the tail process group
			stopBucardo()
			os.Exit(1) // Exit with a non-zero status code to indicate failure
		}
	}
}

// setLogLevel sets the Bucardo logging level if specified in the config.
func setLogLevel(config *BucardoConfig) {
	if config.LogLevel != "" {
		log.Printf("[CONTAINER] Setting Bucardo log level to: %s", config.LogLevel)
		if err := runBucardoCommand("set", fmt.Sprintf("log_level=%s", config.LogLevel)); err != nil {
			// Log as a warning as Bucardo can still run with the default log level.
			log.Printf("[WARNING] Failed to set log_level: %v", err)
		}
	}
}

// getMapKeys is a helper to get keys from a map for logging.
func getMapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if _, err := os.Stat(bucardoConfigPath); os.IsNotExist(err) {
		log.Fatalf("[ERROR] Configuration file not found at %s. Please mount it as a volume.", bucardoConfigPath)
	}

	config, err := loadConfig()
	if err != nil {
		log.Fatalf("[ERROR] Failed to load configuration: %v", err)
	}

	startPostgres()
	setLogLevel(config)
	addDatabasesToBucardo(config)
	addSyncsToBucardo(config)
	startBucardo()

	// Check if any sync has exit_on_complete set to true
	runOnceSyncs := make(map[string]bool)
	var maxTimeout *int
	for i, sync := range config.Syncs {
		if sync.ExitOnComplete != nil && *sync.ExitOnComplete {
			syncName := fmt.Sprintf("sync%d", i)
			runOnceSyncs[syncName] = true
			if sync.ExitOnCompleteTimeout != nil {
				if maxTimeout == nil || *sync.ExitOnCompleteTimeout > *maxTimeout {
					// Use the largest timeout specified across all run-once syncs
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
