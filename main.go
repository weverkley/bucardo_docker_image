package main

import (
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

const bucardoConfigPath = "/media/bucardo/bucardo.json"

// BucardoConfig represents the structure of bucardo.json
type BucardoConfig struct {
	Databases []Database `json:"databases"`
	Syncs     []Sync     `json:"syncs"`
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
	Sources     []int  `json:"sources"`
	Targets     []int  `json:"targets"`
	Herd        string `json:"herd,omitempty"`
	Tables      string `json:"tables,omitempty"`
	Onetimecopy int    `json:"onetimecopy"`
}

// runCommand executes a shell command and prints its output.
func runCommand(name string, arg ...string) error {
	cmd := exec.Command(name, arg...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Printf("Running command: %s", cmd.String())
	return cmd.Run()
}

// runBucardoCommand executes a bucardo command as the 'postgres' user.
func runBucardoCommand(args ...string) error {
	fullArgs := []string{"-", "postgres", "-c", fmt.Sprintf("bucardo %s", strings.Join(args, " "))}
	return runCommand("su", fullArgs...)
}

// startPostgres starts the PostgreSQL service and waits for Bucardo to be ready.
func startPostgres() {
	log.Println("[CONTAINER] Starting PostgreSQL...")
	if err := runCommand("service", "postgresql", "start"); err != nil {
		log.Fatalf("Failed to start postgresql service: %v", err)
	}

	log.Println("[CONTAINER] Waiting for Bucardo to be ready...")
	for {
		// We check the output of `bucardo status` to see if it's ready.
		// We don't need to check the error here, as it will fail until postgres is fully up.
		cmd := exec.Command("su", "-", "postgres", "-c", "bucardo status")
		if err := cmd.Run(); err == nil {
			log.Println("[CONTAINER] Bucardo is ready.")
			return
		}
		time.Sleep(5 * time.Second)
	}
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

// addSyncsToBucardo configures the syncs in Bucardo.
func addSyncsToBucardo(config *BucardoConfig) {
	log.Println("[CONTAINER] Adding syncs to Bucardo...")
	for i, sync := range config.Syncs {
		syncName := fmt.Sprintf("sync%d", i)
		log.Printf("[CONTAINER] Adding %s to Bucardo...", syncName)

		runBucardoCommand("del", "sync", syncName)

		var dbStrings []string
		for _, sourceID := range sync.Sources {
			dbStrings = append(dbStrings, fmt.Sprintf("db%d:source", sourceID))
		}
		for _, targetID := range sync.Targets {
			dbStrings = append(dbStrings, fmt.Sprintf("db%d:target", targetID))
		}
		dbsArg := strings.Join(dbStrings, ",")

		if sync.Herd != "" {
			log.Printf("[CONTAINER] Using herd: %s", sync.Herd)
			if len(sync.Sources) == 0 {
				log.Fatalf("Sync %s uses a herd but has no sources defined.", syncName)
			}
			sourceDB := fmt.Sprintf("db%d", sync.Sources[0])

			runBucardoCommand("del", "herd", sync.Herd)
			runBucardoCommand("add", "herd", sync.Herd)
			runBucardoCommand("add", "all", "tables", fmt.Sprintf("--herd=%s", sync.Herd), fmt.Sprintf("db=%s", sourceDB))

			if err := runBucardoCommand(
				"add", "sync", syncName,
				fmt.Sprintf("herd=%s", sync.Herd),
				fmt.Sprintf("dbs=%s", dbsArg),
				fmt.Sprintf("onetimecopy=%d", sync.Onetimecopy),
			); err != nil {
				log.Fatalf("Failed to add herd-based sync %s: %v", syncName, err)
			}
		} else if sync.Tables != "" {
			log.Println("[CONTAINER] Using table list")
			if err := runBucardoCommand(
				"add", "sync", syncName,
				fmt.Sprintf("dbs=%s", dbsArg),
				fmt.Sprintf("tables=%s", sync.Tables),
				fmt.Sprintf("onetimecopy=%d", sync.Onetimecopy),
			); err != nil {
				log.Fatalf("Failed to add table-based sync %s: %v", syncName, err)
			}
		} else {
			log.Printf("[WARNING] Sync %s has no 'herd' or 'tables' defined. Skipping.", syncName)
		}
	}
}

// startBucardo starts the main Bucardo process.
func startBucardo() {
	log.Println("[CONTAINER] Starting Bucardo...")
	if err := runBucardoCommand("start", "--verbose"); err != nil {
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
		// We check the output of `bucardo status` to see if it has stopped.
		// A non-zero exit code from `bucardo status` indicates it's not running.
		cmd := exec.Command("su", "-", "postgres", "-c", "bucardo status")
		if err := cmd.Run(); err != nil {
			log.Println("[CONTAINER] Bucardo has stopped.")
			return
		}
		time.Sleep(2 * time.Second)
	}
}

// monitorBucardo continuously prints the Bucardo status.
func monitorBucardo() {
	log.Println("[CONTAINER] Now, some status for you.")
	// Set up a channel to listen for termination signals.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// It's okay if this command fails, we just log it.
			runBucardoCommand("status")
		case sig := <-sigs:
			log.Printf("Received signal %s, shutting down.", sig)
			stopBucardo()
			return
		}
	}
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
	addDatabasesToBucardo(config)
	addSyncsToBucardo(config)
	startBucardo()
	monitorBucardo()
}
