# Bucardo

This repository provides a Docker image for [Bucardo](https://bucardo.org/), a powerful asynchronous multi-master replication system for PostgreSQL. The image is based on Ubuntu and is automatically built and published to Docker Hub.

**Docker Hub:** [weverkley/bucardo](https://hub.docker.com/r/weverkley/bucardo)

## Table of Contents
* [Features](#features)
* [Usage](#usage)
  * [Configuration (`bucardo.json`)](#configuration-bucardojson)
  * Running the Container
    * Using `docker run`
    * Using `docker-compose`
* Customizing the Build
* Configuration Details
  * The `databases` object
  * The `syncs` object
* Acknowledgements
* [Copyright and License](#copyright-and-license)

---

## Features
- **Declarative Configuration**: Define all your databases and synchronization tasks in a single `bucardo.json` file.
- **Environment Variable Support**: Keep your database passwords secure by loading them from environment variables.
- **Flexible Sync Definitions**: Sync all tables from a source (`herd`) or specify a list of tables (`tables`).
- **Detailed Logging**: Control Bucardo's log verbosity to get real-time insight into replication.
- **Run-Once Mode**: An option to have the container exit automatically after a successful sync, perfect for batch jobs.
- **Timeout for Run-Once**: Set a timeout for run-once mode to prevent jobs from running indefinitely.
- **Automated Setup**: The container automatically configures Bucardo on startup based on your JSON file.

## Usage

### Configuration (`bucardo.json`)

First, create a `bucardo.json` configuration file. The container will mount and read this file on startup.

  ```jsonc
  {
    "databases":[
      {
        "id": 3,
        "dbname": "example_db",
        "host": "host0.example.com",
        "user": "example_user",
        "pass": "secret",
        "port": 5432
      },{
        "id": 1,
        "dbname": "example_db",
        "host": "host1.example.com",
        "user": "example_user",
        "pass": "secret",
        "port": 5432
      },{
        "id": 2,
        "dbname": "example_db",
        "host": "host3.example.com",
        "user": "example_user",
        "pass": "secret",
        "port": 5432
      }],
    "syncs" : [
      {
        "sources": [3],
        "targets": [1,2],
        "herd": "all_from_source",
        "onetimecopy": 2,
        "conflict_strategy": "bucardo_source",
        "exit_on_complete": true,
        "exit_on_complete_timeout": 300
      },{
        "sources": [1,2],
        "targets": [3],
        "tables": "product, order",
        "onetimecopy": 0,
        "exit_on_complete": false
      }
    ]
  }
  ```

  * Inside databases, describe all databases you desire to sync as a source and/or as a target;

  * The *ID* attribute must be a unique integer per database, and has nothing to do your database but the way the container will identify it;

  * Once your databases are described, you must describe your *syncs*;

  * Each sync must have one or more *sources*, and one or more *targets*; and these have to be described following JSON standard Array notation;

  * Each entity inside the *sources* and *targets* arrays represents an *ID* referring to the databases described beforehand;

  * You must define what to sync. You can either use `herd` or `tables`:
    - `herd`: Provide a string with a name for the herd. All tables from the first source database will be added to this herd and synchronized.
    - `tables`: A string containing a comma-separated list of tables to be synchronized (e.g., `"public.users, public.orders"`).

  * [Onetimecopy](https://bucardo.org/wiki/Onetimecopy) is used for full table copy:
    - 0 No full copy is done
    - 1 A full table copy is always performed
    - 2 A full copy is done in case the destination table is empty
  
  * `strict_checking` (optional): A boolean (`true` or `false`). If set to `false`, Bucardo will not perform strict schema validation, allowing for differences in column order between source and target tables. Defaults to `true` if not specified.
  
  * `conflict_strategy` (optional): A string that defines how Bucardo should handle conflicts (e.g., when a row is updated on both the source and target). If not specified, Bucardo uses its default (`bucardo_random`). Supported values are:
    - `bucardo_source`: The source database always wins. This is the recommended setting for "upsert" behavior, as it ensures the target reflects the source.
    - `bucardo_target`: The target database always wins.
    - `bucardo_skip`: The conflicting row is not replicated.
    - `bucardo_latest`: The row with the most recent change wins (requires a timestamp column).
    - `bucardo_abort`: The entire synchronization is aborted on a conflict.
    - `bucardo_random`: A random database is chosen to be the winner (the default).
  
  * `exit_on_complete` (optional): A boolean (`true` or `false`). When set to `true`, the container will perform a single synchronization and then automatically shut down and exit. This is ideal for "run-once" or batch-style replication tasks.
    - **Note:** This feature requires `log_level` to be set to `VERBOSE` or `DEBUG` to detect sync completion.

  * `exit_on_complete_timeout` (optional): An integer representing seconds. Use this with `exit_on_complete: true`. If the sync does not complete within this time, the container will exit with an error. This prevents failed syncs from running forever.

  * `log_level` (optional, global): A string to control Bucardo's logging verbosity. Set to `"VERBOSE"` or `"DEBUG"` to get detailed, real-time logs about sync activity. This is highly recommended for monitoring and troubleshooting.


### Running the Container

#### Using `docker run`

  ```bash
  docker run --name my_own_bucardo_container \
    -v "$(pwd)/bucardo.json:/media/bucardo/bucardo.json" \
    -d --rm weverkley/bucardo
  ```

5. Check bucardo's status:

  ```bash
  docker logs my_own_bucardo_container -f
  ```

## How to use it (env-based passwords)

Same as before. The only difference is:

* In the JSON database definition, type "env" for password instead of the database user password;

* When you create a container, inform the password as a environment variable named *BUCARDO_DB<ID>*, where *ID* is the *ID* you defined earlier in the *bucardo.json*:

  ```bash
  docker run --name my_own_bucardo_container \
      -v <bucardo.json dir>:/media/bucardo \
      -e BUCARDO_DB3="secret" \
      -d weverkley/bucardo
  ```

## Acknowlegments

This image uses the following software components:

* Ubuntu 22.04;
* PostgreSQL 14;
* Bucardo;
* JQ.

## Copyright and License

This project is copyright 2025 Wever Kley [wever-kley@live.com](mailto:wever-kley@live.com).<br />
Licensed under Apache 2.0 License.<br />
Check the license file for details.
