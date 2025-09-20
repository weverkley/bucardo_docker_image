# Bucardo Docker

[!Docker Image CI](https://github.com/wever-kley/bucardo_docker_image/actions/workflows/docker-image.yml)

This repository provides a powerful, configuration-driven Docker image for Bucardo, an asynchronous multi-master replication system for PostgreSQL.
The image is designed for modern, declarative, and automated workflows. It uses a single `bucardo.json` file as the source of truth, automatically reconciling the Bucardo state on every startup to match your configuration.

**Docker Hub:** weverkley/bucardo

## Key Features

- **Declarative Configuration**: Define all databases and syncs in a single `bucardo.json` file.
- **Automated Reconciliation**: On startup, the container ensures Bucardo's state matches your config, removing any orphaned databases or syncs.
- **Multiple Execution Modes**:
  - **Long-Running**: The default mode for continuous replication.
  - **Run-Once**: The container performs a single sync and then exits, ideal for batch jobs.
  - **Cron-Scheduled**: Trigger syncs based on a cron schedule, with support for both recurring and run-once cron jobs.
- **Flexible Sync Types**:
  - **Source-to-Target**: Classic one-way replication.
  - **Bidirectional (Multi-Master)**: Easily configure two-way or multi-way replication.
- **Secure Password Management**: Load database passwords from environment variables to avoid hardcoding them in your configuration.
- **Structured JSON Logging**: All container and Bucardo logs are emitted as structured JSON for easy parsing and monitoring.
- **Robust Startup & Shutdown**: Graceful shutdown procedures ensure no data is lost, and the startup process cleans up any stale processes from previous runs.

## Quick Start with Docker Compose

1. Create a `bucardo.json` file to define your replication topology. See the Configuration Reference for all options.

   **`bucardo.json`**

   ```json
   {
     "log_level": "VERBOSE",
     "databases": [
       {
         "id": 1,
         "dbname": "sourcedb",
         "host": "source-postgres",
         "user": "postgres",
         "pass": "env"
       },
       {
         "id": 2,
         "dbname": "targetdb",
         "host": "target-postgres",
         "user": "postgres",
         "pass": "env"
       }
     ],
     "syncs": [
       {
         "name": "users_sync",
         "sources": [1],
         "targets": [2],
         "tables": "public.users",
         "onetimecopy": 2,
         "conflict_strategy": "bucardo_source"
       }
     ]
   }
   ```

2. Create a `docker-compose.yml` file.

   **`docker-compose.yml`**

   ```yaml
   services:
     bucardo:
       image: weverkley/bucardo:latest
       container_name: bucardo_app
       volumes:
         - ./bucardo.json:/media/bucardo/bucardo.json:ro
       environment:
         # Passwords for databases defined in bucardo.json with "pass": "env"
         - BUCARDO_DB1=your_source_db_password
         - BUCARDO_DB2=your_target_db_password
       # Add depends_on if your databases are also in Docker Compose
       # depends_on:
       #   - source-postgres
       #   - target-postgres
   ```

3. Run the container.

   ```bash
   docker-compose up
   ```

## Configuration Reference (`bucardo.json`)

### Top-Level Properties

| Property    | Type     | Description                                                                                             |
| ----------- | -------- | ------------------------------------------------------------------------------------------------------- |
| `databases` | `array`  | **Required.** An array of Database Objects.                                                             |
| `syncs`     | `array`  | **Required.** An array of Sync Objects.                                                                 |
| `log_level` | `string` | _Optional._ Sets Bucardo's global log level. Recommended: `"VERBOSE"` or `"DEBUG"` for troubleshooting. |

### Database Object

| Property | Type     | Description                                                                                                                                |
| -------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `id`     | `int`    | **Required.** A unique integer to identify this database within the config. Used to reference it in syncs.                                 |
| `dbname` | `string` | **Required.** The name of the database.                                                                                                    |
| `host`   | `string` | **Required.** The database hostname or IP address.                                                                                         |
| `user`   | `string` | **Required.** The username for the connection.                                                                                             |
| `pass`   | `string` | **Required.** The password for the user, or the string `"env"` to load the password from an environment variable. See Password Management. |
| `port`   | `int`    | _Optional._ The database port. Defaults to `5432`.                                                                                         |

### Sync Object

| Property                   | Type     | Description                                                                                                                                            |
| -------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `name`                     | `string` | **Required.** A unique name for the sync. -                                                                                                            |
| `sources`                  | `array`  | An array of database IDs to use as sources. Used for one-way replication. -                                                                            |
| `targets`                  | `array`  | An array of database IDs to use as targets. Used for one-way replication. -                                                                            |
| `bidirectional`            | `array`  | An array of two or more database IDs for multi-master replication. When used, `sources` and `targets` are ignored. -                                   |
| `herd`                     | `string` | The name of a "herd" (a group of tables). All tables from the first source database will be added to this herd and replicated. Use this OR `tables`. - |
| `tables`                   | `string` | A comma-separated list of specific tables to sync (e.g., `"public.users, public.orders"`). Use this OR `herd`. -                                       |
| `onetimecopy`              | `int`    | Controls full-table-copy behavior. `0`=off, `1`=always, `2`=if target table is empty. See Bucardo docs. -                                              |
| `strict_checking`          | `bool`   | _Optional._ If `false`, allows schema differences like column order. Defaults to `true`. -                                                             |
| `conflict_strategy`        | `string` | _Optional._ Defines how to resolve data conflicts. Common values: `bucardo_source` (source wins), `bucardo_latest` (most recent change wins). -        |
| `exit_on_complete`         | `bool`   | _Optional._ If `true`, the container performs a single sync and then exits. Ideal for batch jobs. Requires `log_level` of `VERBOSE` or `DEBUG`. -      |
| `exit_on_complete_timeout` | `int`    | _Optional._ Timeout in seconds for a run-once sync. If the sync doesn't complete in time, the container exits with an error. -                         |
| `cron`                     | `string` | _Optional._ A cron expression (e.g., `"0 2 * * *"`) to trigger the sync. This disables Bucardo's default timer and uses an internal scheduler. -       |

## Password Management

For better security, you can load database passwords from environment variables instead of

writing them in `bucardo.json`.

1. In your `database` object, set `"pass": "env"`.
2. In your `docker-compose.yml` or `docker run` command, set an environment variable named `BUCARDO_DB<ID>`, where `<ID>` is the `id` of the database.

```yaml
# docker-compose.yml
services:
  bucardo:
    image: weverkley/bucardo:latest
    volumes:
      - ./bucardo.json:/media/bucardo/bucardo.json:ro
    environment:
      - BUCARDO_DB1=your_db1_password
      - BUCARDO_DB2=your_db2_password
```

## Copyright and License

This project is copyright 2025 Wever Kley. Licensed under the Apache 2.0 License.
