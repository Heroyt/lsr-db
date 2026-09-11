# LSR DB

`lsr/db` provides database connections and fluent queries on top of Dibi, with cached reads, DTO mapping, a static `DB` facade, named connections and Nette DI integration.

## Requirements

- PHP `>= 8.4`.
- Dibi `^5`, Nette DI `^3.2.4` (native lazy logger services), Nette Schema `^1.2.5`, PSR Log `^1.0 || ^2.0 || ^3.0`, and `lsr/logging`, `lsr/caching` and `lsr/serializer` `^0.3` (installed by Composer).
- The PHP database extension for the configured driver, such as `ext-mysqli`, `ext-sqlite3`, or `ext-pdo` with `ext-pdo_sqlite`/`ext-pdo_mysql`. This package does not declare a particular driver extension; its dependencies also impose their own extension requirements, including `ext-redis` through `lsr/caching`.
- Application-provided `Lsr\Caching\Cache` and `Lsr\Serializer\Mapper` services.
- Define `LOG_DIR` before using a default database logger. Without an explicit logger, connections lazily create `Lsr\Logging\Logger(LOG_DIR, 'db')`; use an application-owned writable directory. An injected logger does not require this constant. File-based SQLite configurations also need an existing writable parent directory; the native SQLite fallback path uses `TMP_DIR . 'db.db'`.

## Installation

```sh
composer require lsr/db
```

## Nette DI integration

This is an integration into an existing application container, not a standalone bootstrap. First register the cache and mapper services, then register [`Lsr\Db\DI\DbExtension`](src/DI/DbExtension.php). Default loggers resolve `LOG_DIR` at runtime, not during container compilation:

```neon
extensions:
    db: Lsr\Db\DI\DbExtension

db:
    connections:
        main:
            driver: pdo
            dsn: "sqlite::memory:"
        reporting:
            driver: pdo
            dsn: "sqlite::memory:"
```

This example creates two independent in-memory SQLite databases; select a persistent database configuration for application data. A `main` connection is required. The extension registers `db.connection` and `db.connection.reporting`; only the main connection is autowired by type.

Resolving the main service initializes the static facade and registers the named connections. Compilation alone does not initialize `DB`. Given the compiled application container:

```php
use Lsr\Db\DB;

$connection = $container->getService('db.connection');
$value = $connection->query('SELECT %i AS value', 42)->fetchSingle();

$reporting = DB::getConnection('reporting');
```

The schema in [`DbExtension`](src/DI/DbExtension.php) lists the supported configuration keys. [`DbExtensionTest`](tests/TestCases/DbExtensionTest.php) contains a complete container-construction example with cache and mapper dependencies.

## Manual initialization and queries

Without the DI extension, use [`DB::createConnection($cache, $mapper, $config, $name = null, $logger = null)`](src/DB.php), then `DB::init($connection)` to register the main connection. `DB::initNamed($name, $connection)` adds other connections. `DB::getMain($cache, $mapper, $config = [], $logger = null)` builds a connection from the package's environment-variable configuration when no explicit configuration is passed; it still needs `DB::init()` before static query calls. The environment keys include `DB_driver`, `DB_host`, `DB_port`, `DB_user`, `DB_password`, `DB_NAME`, `DB_dsn` and `DB_pdoDriver`; their exact defaults and remaining options are defined in [`DB::getMain()`](src/DB.php).

[`Lsr\Db\Connection`](src/Connection.php) supports Dibi placeholder queries, inserts, updates, deletes and fluent reads. Its `select($table, ...$fields)` takes the table first, unlike Dibi's field-first builder. `from($table)->select(...)` is also available.

[`Lsr\Db\Dibi\Fluent`](src/Dibi/Fluent.php) adds the cached fetch methods in [`FetchFunctions`](src/Dibi/FetchFunctions.php), including `fetchDto()` and `fetchAllDto()`. These use the configured mapper for DTO conversion. Read helpers use caching by default; pass their `cache: false` argument when an uncached read is required.

## Database logging

**Unreleased:** logger injection and the configuration below are working-tree changes, not features of an existing published version. Check installed source before using them.

`Connection::__construct($cache, $mapper, $config, $name = null, $logger = null)`, `DB::createConnection()` and `DB::getMain()` accept an optional `Psr\Log\LoggerInterface` as their final `$logger` argument. Existing positional and named arguments remain valid. For standalone connections, pass the logger separately from the driver configuration:

```php
$connection = DB::createConnection(
    $cache,
    $mapper,
    ['driver' => 'pdo', 'dsn' => 'sqlite::memory:'],
    logger: $logger,
);
```

In Nette DI, select application-owned logger services using native `@service` references:

```neon
db:
    logger: @sharedDatabaseLogger
    connections:
        main:
            driver: pdo
            dsn: "sqlite::memory:"
        reporting:
            driver: pdo
            dsn: "sqlite::memory:"
            logger: @reportingLogger
```

Both referenced services must implement `Psr\Log\LoggerInterface`. The optional root `logger` is shared by connections without their own override. A connection's non-null `logger` takes precedence over the root; `null` means inherit the root, or use the legacy dedicated default if the root is also null. Selecting the same service for several connections shares the actual logger object.

The extension exposes `db.logger` for the root selection and `db.logger.<name>` for each effective connection logger, including `db.logger.main`. All these services are non-autowired by type, so they do not compete with the application's `@logger`. The prefix follows the extension's registered name. A main-connection override changes `db.logger.main`, not the common `db.logger` or other connections.

Without overrides, the main logger is `db.logger`, and named connections keep separate lazy default logger instances. Each uses runtime `LOG_DIR` and the `db` file name (`db-YYYY-MM-DD.log`). The extension never implicitly adopts a global `@logger`; use `logger: @logger` to opt in. Logger selection is not part of driver options or the connection's cache namespace.

[`DibiEventLogger`](src/Logging/DibiEventLogger.php) translates failed Dibi events into the same records as the legacy `Logger::logDb()` helper: one `error` record with the exception message and an optional nonzero code prefix, followed by one `debug` record (`SQL: ...`) only when the exception contains nonempty SQL. Contexts remain empty; successful events are not logged. Logger exceptions propagate synchronously, and a failed error write prevents the following debug write. The public logging-package helper remains available and unchanged.

## Transactions and long-running processes

- `Connection::transaction()` passes the connection to its callback. Return `true` to commit; a false result rolls back, and thrown exceptions trigger rollback and are rethrown. A callback that returns nothing does not commit.
- `DB::withConnection($nameOrConnection, $callback)` restores the prior active connection in a `finally` block. Static connection selection is process-global: the callback must finish synchronously and must not span a Fiber suspension or concurrent unit of work. Prefer injected connection instances in concurrent applications.
- `autoReconnect` is opt-in. It health-checks outside package-managed transactions and can reconnect before a query, but never replays a failed application statement. Raw Dibi access, raw SQL transaction control and session state such as temporary tables or locks are outside that guarantee. See [`Connection::ensureConnected()`](src/Connection.php).

## Development

CI runs the complete suite on PHP 8.4 and 8.5 with SQLite and an isolated MySQL 8.4 service. Install the extensions listed in [.github/workflows/ci.yml](.github/workflows/ci.yml), including `sqlite3`, `pdo_sqlite`, `mysqli` and `pdo_mysql`. Keep `proc_open` enabled for the interrupted-query tests. For the same checks locally:

```sh
composer install --prefer-dist --no-interaction --no-progress
composer cs
vendor/bin/phpstan analyse --no-progress
LSR_DB_TEST_PORT=13376 vendor/bin/phpunit --no-coverage
```

Provide a disposable MySQL server at `127.0.0.1:13376` with database `reconnect_test` and user `root` with an empty password; set `LSR_DB_TEST_PORT` to your port if different. Tests create/drop tables and deliberately kill database sessions, so never use an application database. Without this environment variable the MySQL tests are skipped locally; CI always supplies it. Redis is required as a PHP extension by dependencies, but no Redis server is needed.

The checkout must be writable: tests use `tests/tmp/` for temporary SQLite files and `tests/logs/` for logs. `composer test` enables Xdebug coverage mode; coverage reports need a compatible coverage driver. See [`phpunit.xml`](phpunit.xml), [`phpstan.neon`](phpstan.neon) and [`tests/bootstrap.php`](tests/bootstrap.php).

Run `composer cs` to check PHP coding style without changing files and `composer cs:fix` to apply fixes (`composer cbf` is an alias). PHP CS Fixer uses [`.php-cs-fixer.php`](.php-cs-fixer.php).

## AI coding assistance

See [LSR Skills](https://github.com/Heroyt/lsr-skills) for AI agent skills for working with the LSR framework.

## License

Licensed under the [MIT License](LICENSE).
