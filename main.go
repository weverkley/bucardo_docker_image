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

	"github.com/robfig/cron/v3"
)

// bucardoLogPath is the default location for the Bucardo log file inside the container.
const bucardoLogPath = "/var/log/bucardo/log.bucardo"

// bucardoConfigPath is the expected location of the user-provided configuration file.
const bucardoConfigPath = "/media/bucardo/bucardo.json"

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
	Sources               []int  `json:"sources"`                            // A list of database IDs to use as sources.
	Targets               []int  `json:"targets"`                            // A list of database IDs to use as targets.
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
	log.Printf("Running command: %s", logCmd)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// runBucardoCommand is a helper to execute a `bucardo` command as the 'postgres' user.
func runBucardoCommand(args ...string) error {
	// Prepend "bucardo" to the arguments to form the full command.
	fullBucardoCmd := append([]string{"bucardo"}, args...)

	// Construct the command string for logging purposes only.
	logCmdStr := strings.Join(fullBucardoCmd, " ")

	// Execute `su - postgres -c "bucardo arg1 arg2 ..."` but pass arguments safely.
	return runCommand(logCmdStr, "su", "-", "postgres", "-c", strings.Join(fullBucardoCmd, " "))
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
	pgpassPath := "/var/lib/postgresql/.pgpass"
	f, err := os.OpenFile(pgpassPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("failed to open .pgpass file: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteString(pgpassEntry); err != nil {
		return fmt.Errorf("failed to write to .pgpass file: %w", err)
	}

	// Ensure the file is owned by the postgres user.
	return runCommand("", "chown", "postgres:postgres", pgpassPath)
}

// addDatabasesToBucardo iterates through the config and adds each database to Bucardo's configuration.
func addDatabasesToBucardo(config *BucardoConfig) {
	log.Println("[CONTAINER] Adding databases to Bucardo...")
	// Clear any existing .pgpass file to start fresh.
	os.Remove("/var/lib/postgresql/.pgpass")

	for _, db := range config.Databases {
		log.Printf("[CONTAINER] Adding db %s (id: %d)", db.DBName, db.ID)

		dbName := fmt.Sprintf("db%d", db.ID)
		runBucardoCommand("del", "db", dbName, "--force")

		password, err := getDbPassword(db)
		if err != nil {
			log.Fatalf("Error getting password for db id %d: %v", db.ID, err)
		}

		// Add the password to the .pgpass file to avoid command-line parsing issues.
		if err := setupPgpassFile(db, password); err != nil {
			log.Fatalf("Failed to set up .pgpass for db %s: %v", dbName, err)
		}

		// Build the arguments for bucardo, omitting the password.
		// Bucardo will automatically use the .pgpass file.
		args := []string{
			"add", "db", dbName, "--force",
			fmt.Sprintf("dbname=%s", db.DBName),
			fmt.Sprintf("host=%s", db.Host),
			fmt.Sprintf("user=%s", db.User),
		}
		if db.Port != nil {
			args = append(args, fmt.Sprintf("port=%d", *db.Port))
		}
		if err := runBucardoCommand(args...); err != nil {
			log.Fatalf("Failed to add database %s: %v", dbName, err)
		}
	}
}

// applySyncCustomizations applies advanced sync settings that are not available during
// the `bucardo add sync` command in this version of Bucardo. It uses `bucardo update sync`.
func applySyncCustomizations(sync Sync, syncName string) {
	// Set the conflict_strategy if specified in the config.
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

// addSyncsToBucardo configures the replication tasks (syncs) in Bucardo based on the JSON config.
func addSyncsToBucardo(config *BucardoConfig) {
	log.Println("[CONTAINER] Adding syncs to Bucardo...")
	for i, sync := range config.Syncs {
		syncName := fmt.Sprintf("sync%d", i)
		log.Printf("[CONTAINER] Adding %s to Bucardo...", syncName)

		runBucardoCommand("del", "sync", syncName, "--force")

		// Prepare the database connection string for the sync command.
		var dbStrings []string
		for _, sourceID := range sync.Sources {
			dbStrings = append(dbStrings, fmt.Sprintf("db%d:source", sourceID))
		}
		for _, targetID := range sync.Targets {
			dbStrings = append(dbStrings, fmt.Sprintf("db%d:target", targetID))
		}
		dbsArg := strings.Join(dbStrings, ",")

		// Prepare the base arguments for the `bucardo add sync` command.
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

			// For a herd, we must create the herd and add all tables from the source before creating the sync.
			runBucardoCommand("del", "herd", sync.Herd, "--force")
			runBucardoCommand("add", "herd", sync.Herd)
			runBucardoCommand("add", "all", "tables", fmt.Sprintf("--herd=%s", sync.Herd), fmt.Sprintf("db=%s", sourceDB))

			args = append(args, fmt.Sprintf("herd=%s", sync.Herd))
		} else if sync.Tables != "" {
			log.Println("[CONTAINER] Using table list")
			// Prevent users from accidentally trying to sync all tables via the 'tables' field.
			trimmedTables := strings.ToLower(strings.TrimSpace(sync.Tables))
			if trimmedTables == "all" || trimmedTables == "*" {
				log.Fatalf("Error in sync '%s': The 'tables' field cannot be 'all' or '*'. To sync all tables from a source, please use the 'herd' option instead. See the README for more details.", syncName)
			}
			args = append(args, fmt.Sprintf("tables=%s", sync.Tables))
		} else {
			log.Printf("[WARNING] Sync %s has no 'herd' or 'tables' defined. Skipping.", syncName)
			continue
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

		// Execute the command and apply customizations
		if err := runBucardoCommand(args...); err != nil {
			log.Fatalf("Failed to add sync %s: %v", syncName, err)
		}
		applySyncCustomizations(sync, syncName)
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

	log.Printf("[CONTAINER] Streaming Bucardo log file: %s", bucardoLogPath)
	if err := cmd.Start(); err != nil {
		log.Printf("[WARNING] Could not start streaming Bucardo log file: %v", err)
		return nil
	}

	// Return the command so its process can be managed.
	return cmd
}

// startBucardo starts the main Bucardo process.
func startBucardo() {
	log.Println("[CONTAINER] Starting Bucardo...")

	// Before starting, attempt to stop any stale Bucardo processes that might have been
	// left over from an unclean shutdown. We ignore errors here.
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
	if err := runBucardoCommand("stop"); err != nil {
		// Log as a warning because we are shutting down anyway.
		log.Printf("[WARNING] 'bucardo stop' command failed: %v", err)
	}

	log.Println("[CONTAINER] Waiting for Bucardo to stop completely...")
	// Loop until the main Bucardo PID file is removed, which confirms a clean shutdown.
	for {
		// Checking for the absence of the MCP (Master Control Process) PID file is the
		// most reliable way to confirm Bucardo has stopped.
		if _, err := os.Stat("/var/run/bucardo/bucardo.mcp.pid"); os.IsNotExist(err) {
			log.Println("[CONTAINER] Bucardo has stopped.")
			return
		}
		time.Sleep(1 * time.Second)
	}
}

// monitorBucardo handles the default long-running mode. It streams logs and waits for a
// termination signal (SIGINT, SIGTERM) to gracefully shut down Bucardo.
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

	for {
		select {
		case sig := <-sigs:
			log.Printf("[CONTAINER] Received signal %s, shutting down.", sig)
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

	// Get a pipe to the command's stdout to read the log lines.
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("[ERROR] Could not create pipe for tail command: %v", err)
	}
	cmd.Stderr = os.Stderr

	// Create a context that we can cancel to stop the log scanning goroutine.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancellation happens on function exit
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		log.Fatalf("[ERROR] Could not start log tailing command: %v", err)
	}
	// Set up the timeout if specified
	if maxTimeout != nil && *maxTimeout > 0 {
		timeoutDuration := time.Duration(*maxTimeout) * time.Second
		log.Printf("[CONTAINER] Setting a timeout of %v for run-once sync completion.", timeoutDuration)
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
				log.Println("[CONTAINER] Log streaming finished unexpectedly.")
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
						log.Printf("[CONTAINER] Completion message for sync '%s' detected.", syncName)
						if err := runBucardoCommand("stop", syncName); err != nil {
							log.Printf("[WARNING] Failed to stop sync %s: %v", syncName, err)
						}
						// Remove from the map of syncs we are waiting for.
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
					cancel()                                        // Stop the scanner goroutine.
					syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // Kill the tail process group.
					stopBucardo()
					return // Success.
				} else {
					// Some syncs were run-once, but others are long-running. Switch to standard monitoring.
					log.Println("[CONTAINER] Other syncs are still running. Switching to standard monitoring mode.")
					cancel()
					syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					monitorBucardo() // Hand off to the long-running monitor
					return
				}
			}
		case <-timeoutChannel:
			log.Printf("[ERROR] Timeout of %d seconds reached. The following syncs did not complete: %v", *maxTimeout, getMapKeys(runOnceSyncs))
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // Kill the tail process group.
			stopBucardo()
			os.Exit(1) // Exit with a non-zero status code to indicate failure.
		}
	}
}

// setLogLevel sets the global Bucardo logging level if specified in the config.
func setLogLevel(config *BucardoConfig) {
	if config.LogLevel != "" {
		log.Printf("[CONTAINER] Setting Bucardo log level to: %s", config.LogLevel)
		if err := runBucardoCommand("set", fmt.Sprintf("log_level=%s", config.LogLevel)); err != nil {
			// Log as a warning as Bucardo can still run with the default log level.
			log.Printf("[WARNING] Failed to set log_level: %v", err)
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
func runCronScheduler(jobs []CronJob) {
	c := cron.New()
	var runOnceJob *CronJob // Special handling for the first job with exit_on_complete

	for i := range jobs {
		job := jobs[i] // Create a local copy for the closure

		// If a job is marked for exit, we only schedule it to run ONCE at its next designated time.
		if job.ExitOnComplete {
			if runOnceJob == nil {
				runOnceJob = &job
			} else {
				log.Printf("[WARNING] Multiple cron syncs have 'exit_on_complete: true'. Only the first one ('%s') will cause an exit. The others will run on their schedule but will not trigger an exit.", runOnceJob.SyncName)
			}
			continue // Don't add to the recurring scheduler
		}

		// This is a recurring cron job.
		_, err := c.AddFunc(job.CronExpression, func() {
			log.Printf("[CRON] Kicking sync '%s' as per schedule '%s'", job.SyncName, job.CronExpression)
			// We don't specify a timeout for recurring jobs, letting Bucardo handle it.
			if err := runBucardoCommand("kick", job.SyncName, "0"); err != nil {
				log.Printf("[ERROR] Failed to kick sync '%s': %v", job.SyncName, err)
			}
		})
		if err != nil {
			log.Printf("[ERROR] Failed to schedule cron job for sync '%s': %v. This sync will not run.", job.SyncName, err)
		}
	}

	// If there's a run-once job, handle it separately.
	if runOnceJob != nil {
		schedule, err := cron.ParseStandard(runOnceJob.CronExpression)
		if err != nil {
			log.Fatalf("[ERROR] Invalid cron expression for sync '%s': %v", runOnceJob.SyncName, err)
		}

		nextRun := schedule.Next(time.Now())
		log.Printf("[CRON] Sync '%s' is a run-once job. It will run next at %s and then the container will exit upon completion.", runOnceJob.SyncName, nextRun.Format(time.RFC1123))

		// Wait until the next scheduled time.
		time.Sleep(time.Until(nextRun))

		// Kick the sync.
		log.Printf("[CRON] Kicking one-time sync '%s'", runOnceJob.SyncName)
		kickTimeout := "0" // Bucardo's default timeout
		if runOnceJob.Timeout != nil {
			kickTimeout = fmt.Sprintf("%d", *runOnceJob.Timeout)
		}
		if err := runBucardoCommand("kick", runOnceJob.SyncName, kickTimeout); err != nil {
			log.Printf("[ERROR] Failed to kick sync '%s': %v. The container will now exit.", runOnceJob.SyncName, err)
			os.Exit(1)
		}

		// Now, monitor this single sync for completion.
		// We create a temporary config and sync map for monitorSyncs.
		dummyConfig := &BucardoConfig{LogLevel: "VERBOSE"} // Assume VERBOSE for monitoring
		syncsToWatch := map[string]bool{runOnceJob.SyncName: true}
		monitorSyncs(dummyConfig, syncsToWatch, runOnceJob.Timeout)
		log.Println("[CRON] Run-once job complete. Exiting.")
		return // Exit the application.
	}

	// If we are here, there were no run-once jobs, so we run the scheduler indefinitely.
	if len(c.Entries()) > 0 {
		log.Printf("[CRON] Starting cron scheduler for %d recurring sync(s).", len(c.Entries()))
		c.Start()

		// Keep the application alive while the cron scheduler runs in the background.
		// Also stream logs.
		monitorBucardo()
		log.Println("[CRON] Shutting down cron scheduler.")
		<-c.Stop().Done()
	} else {
		log.Println("[CONTAINER] No cron jobs were scheduled. The container will now exit as there is nothing to do.")
	}
}

func main() {
	// Use a more granular timestamp for logs.
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if _, err := os.Stat(bucardoConfigPath); os.IsNotExist(err) {
		log.Fatalf("[ERROR] Configuration file not found at %s. Please mount it as a volume.", bucardoConfigPath)
	}

	config, err := loadConfig()
	if err != nil {
		log.Fatalf("[ERROR] Failed to load configuration: %v", err)
	}

	// Main startup sequence.
	startPostgres()
	setLogLevel(config)

	// The .pgpass file needs to exist with the right permissions before Bucardo uses it.
	// We touch it here and set permissions, even if it's empty initially.
	runCommand("", "touch", "/var/lib/postgresql/.pgpass")
	runCommand("", "chmod", "0600", "/var/lib/postgresql/.pgpass")
	addDatabasesToBucardo(config)
	addSyncsToBucardo(config)
	startBucardo()

	// --- Determine execution mode: Cron, Run-Once, or Standard Long-Running ---

	runOnceSyncs := make(map[string]bool)
	cronJobs := []CronJob{}
	var maxTimeout *int

	for i, sync := range config.Syncs {
		if sync.ExitOnComplete != nil && *sync.ExitOnComplete {
			syncName := fmt.Sprintf("sync%d", i)
			runOnceSyncs[syncName] = true
			if sync.ExitOnCompleteTimeout != nil {
				if maxTimeout == nil || *sync.ExitOnCompleteTimeout > *maxTimeout {
					// Use the largest timeout specified across all run-once syncs.
					timeoutVal := *sync.ExitOnCompleteTimeout
					maxTimeout = &timeoutVal
				}
			}
		}

		if sync.Cron != "" {
			syncName := fmt.Sprintf("sync%d", i)
			cronJobs = append(cronJobs, CronJob{
				SyncName:       syncName,
				CronExpression: sync.Cron,
				ExitOnComplete: sync.ExitOnComplete != nil && *sync.ExitOnComplete,
				Timeout:        sync.ExitOnCompleteTimeout,
			})
		}
	}

	// Priority 1: If there are cron jobs, the cron scheduler takes over.
	if len(cronJobs) > 0 {
		runCronScheduler(cronJobs)
		// Priority 2: If there are non-cron run-once syncs, monitor them for completion.
	} else if len(runOnceSyncs) > 0 {
		monitorSyncs(config, runOnceSyncs, maxTimeout)
		// Priority 3: Otherwise, enter the standard long-running mode.
	} else {
		monitorBucardo()
	}
}
