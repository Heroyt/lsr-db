<?php
declare(strict_types=1);

namespace Lsr\Db;

use DateTimeInterface;
use Dibi\Connection as DibiConnection;
use Dibi\DriverException;
use Dibi\Drivers\MySqliDriver;
use Dibi\Drivers\PdoDriver;
use Dibi\Drivers\SqliteDriver;
use Dibi\Exception;
use Dibi\Event;
use Dibi\Result;
use JetBrains\PhpStorm\Language;
use Lsr\Caching\Cache;
use Lsr\Db\Dibi\Fluent;
use Lsr\Db\Lifecycle\DatabaseLifecycleEvent;
use Lsr\Db\Lifecycle\DatabaseLifecycleHookInterface;
use Lsr\Logging\Logger;
use Lsr\Serializer\Mapper;
use LogicException;
use mysqli;
use mysqli_sql_exception;
use PDO;
use PDOException;
use ReflectionProperty;
use Throwable;
use WeakMap;

/**
 * @phpstan-type Config array{
 *     lazy?: bool,
 *     driver: non-empty-string,
 *     host?: non-empty-string,
 *     port?: int,
 *     user?: string,
 *     username?: string|null,
 *     password?: string,
 *     database?: non-empty-string,
 *     collate?: non-empty-string,
 *     pdoDriver?: string,
 *     dsn?: string,
 *     options?: array<array-key, mixed>,
 *     prefix?: string,
 *     strictSelectForUpdate?: bool,
 *     autoReconnect?: bool,
 * }
 */
final class Connection
{
    public const bool DEFAULT_STRICT_SELECT_FOR_UPDATE = false;

    /** @var Config */
    private array $config;

    private readonly ?string $cacheNamespace;

    /** @var list<string> */
    private array $transactionSavepoints = [];
    /** @var WeakMap<object, DatabaseLifecycleHookInterface>|null */
    private static ?WeakMap $lifecycleHooks = null;
    /** @var WeakMap<object, bool>|null */
    private static ?WeakMap $lifecycleListeners = null;
    /** @var WeakMap<object, bool>|null */
    private static ?WeakMap $lifecycleRawSql = null;


    public DibiConnection $connection {
        get {
            if (!isset($this->connection)) {
                $startedAt = $this->lifecycleHook() !== null ? hrtime(true) : null;
                try {
                    $this->connection = new DibiConnection($this->config, $this->name);
                } catch (Throwable $exception) {
                    if ($startedAt !== null) {
                        $this->recordLifecycle(
                            new DatabaseLifecycleEvent(
                                DatabaseLifecycleEvent::CONNECT,
                                DatabaseLifecycleEvent::ERROR,
                                (hrtime(true) - $startedAt) / 1_000_000_000,
                                $this->databaseSystem(),
                                $this->name,
                                errorType: $exception::class,
                            )
                        );
                    }
                    throw $exception;
                }
                if (!empty($this->config['prefix'])) {
                    $this->connection->getSubstitutes()->__set('', $this->config['prefix']);
                }
                $this->connection->onEvent[] = [$this->logger, 'logDb'];
                $this->registerLifecycleListener();
                if ($startedAt !== null && empty($this->config['lazy'])) {
                    $this->recordLifecycle(
                        new DatabaseLifecycleEvent(
                            DatabaseLifecycleEvent::CONNECT,
                            DatabaseLifecycleEvent::SUCCESS,
                            (hrtime(true) - $startedAt) / 1_000_000_000,
                            $this->databaseSystem(),
                            $this->name,
                        )
                    );
                }
            }
            return $this->connection;
        }
    }

    private Logger $logger {
        get {
            if (!isset($this->logger)) {
                $this->logger = new Logger(LOG_DIR, 'db');
            }
            return $this->logger;
        }
    }

    /**
     * @param  Config  $config
     */
    public function __construct(
        private readonly Cache   $cache,
        private readonly Mapper  $mapper,
        array $config,
        private readonly ?string $name = null,
    ) {
        /** @var Config $config */
        $this->config = $this->normalizeConfig($config);
        $this->cacheNamespace = $name === 'main'
            ? null
            : hash('sha256', serialize([$name, $this->config]));
        $sqliteFile = $this->getSqliteFilePath();
        if (isset($sqliteFile) && !file_exists($sqliteFile)) {
            touch($sqliteFile);
        }
    }

    public function setLifecycleHook(DatabaseLifecycleHookInterface $hook, bool $includeRawSql = false): static {
        self::$lifecycleHooks ??= new WeakMap();
        self::$lifecycleHooks[$this] = $hook;
        self::$lifecycleRawSql ??= new WeakMap();
        self::$lifecycleRawSql[$this] = $includeRawSql;
        if ((new ReflectionProperty($this, 'connection'))->isInitialized($this)) {
            $this->registerLifecycleListener();
        }
        return $this;
    }

    /**
     * @param Config $config
     * @return Config
     */
    private function normalizeConfig(array $config): array
    {
        $driver = strtolower($config['driver']);
        if (str_starts_with($driver, 'pdo_') || str_starts_with($driver, 'pdo-')) {
            $config['pdoDriver'] ??= substr($driver, 4);
            $config['driver'] = 'pdo';
            $driver = 'pdo';
        }

        if ($driver !== 'pdo' || !empty($config['dsn'])) {
            return $config;
        }

        $pdoDriver = strtolower((string)($config['pdoDriver'] ?? 'mysql'));
        $config['dsn'] = match ($pdoDriver) {
            'sqlite' => 'sqlite:' . ($config['database'] ?? ':memory:'),
            'pgsql', 'postgres', 'postgresql' => $this->buildPdoKvDsn(
                'pgsql',
                [
                    'host' => $config['host'] ?? null,
                    'port' => $config['port'] ?? null,
                    'dbname' => $config['database'] ?? null,
                ]
            ),
            'sqlsrv' => $this->buildPdoSqlsrvDsn($config),
            'mysql', 'mariadb' => $this->buildPdoKvDsn(
                'mysql',
                [
                    'host' => $config['host'] ?? null,
                    'port' => $config['port'] ?? null,
                    'dbname' => $config['database'] ?? null,
                    'charset' => $config['collate'] ?? null,
                ]
            ),
            default => $this->buildPdoKvDsn(
                $pdoDriver,
                [
                    'host' => $config['host'] ?? null,
                    'port' => $config['port'] ?? null,
                    'dbname' => $config['database'] ?? null,
                ]
            ),
        };

        return $config;
    }

    /**
     * @param array<string, float|int|string|null> $parts
     */
    private function buildPdoKvDsn(string $driver, array $parts): string
    {
        $segments = [];
        foreach ($parts as $key => $value) {
            if ($value === null || $value === '') {
                continue;
            }
            $segments[] = $key . '=' . $value;
        }
        return $driver . ':' . implode(';', $segments);
    }

    /**
     * @param Config $config
     */
    private function buildPdoSqlsrvDsn(array $config): string
    {
        $segments = [];
        if (!empty($config['host'])) {
            $server = $config['host'];
            if (!empty($config['port'])) {
                $server .= ',' . (string)$config['port'];
            }
            $segments[] = 'Server=' . $server;
        }
        if (!empty($config['database'])) {
            $segments[] = 'Database=' . $config['database'];
        }
        return 'sqlsrv:' . implode(';', $segments);
    }

    /**
     * @return non-empty-string|null
     */
    private function getSqliteFilePath(): ?string
    {
        if ($this->config['driver'] === 'sqlite') {
            return $this->config['database'] ?? TMP_DIR . 'db.db';
        }

        if ($this->config['driver'] !== 'pdo') {
            return null;
        }

        $dsn = $this->config['dsn'] ?? null;
        if (!is_string($dsn) || !str_starts_with($dsn, 'sqlite:')) {
            return null;
        }

        $path = substr($dsn, 7);
        if ($path === '' || $path === ':memory:') {
            return null;
        }
        return $path;
    }

    /**
     * @param  non-empty-string  $name
     * @param  mixed[]  $arguments
     * @return mixed
     */
    public function __call(string $name, array $arguments) : mixed {
        if (
            in_array(
                strtolower($name),
                ['nativequery', 'fetch', 'fetchall', 'fetchsingle', 'fetchpairs', 'loadfile'],
                true,
            )
        ) {
            $this->ensureConnected();
        }
        return $this->connection->$name(...$arguments);
    }

    /**
     * Checks an idle MySQL connection before submitting an application statement.
     *
     * Opt in with autoReconnect=true. Adds one round trip outside managed
     * transactions. Only the health check may be retried by reconnecting; an
     * application statement is never replayed, even if it loses its connection.
     *
     * Use begin()/commit()/rollback() for transactions. Raw Dibi/native access,
     * raw SQL transaction control, and session state (temporary tables, locks,
     * variables) are outside this guarantee and should not use autoReconnect.
     *
     * @internal Used by the package's query execution paths, not result getters.
     * @throws DriverException
     */
    public function ensureConnected(): void {
        if (empty($this->config['autoReconnect']) || $this->transactionSavepoints !== []) {
            return;
        }
        $connection = $this->connection;
        if (!$connection->isConnected()) {
            return;
        }
        $driver = $connection->getDriver();
        try {
            if ($driver instanceof MySqliDriver) {
                $resource = $driver->getResource();
                assert($resource instanceof mysqli);
                // COM_STATISTICS does not change insert IDs or affected rows.
                // Unlike mysqli::ping(), stat() is not deprecated in PHP 8.4+.
                if (@$resource->stat() === false) {
                    throw new DriverException($resource->error, $resource->errno);
                }
            } elseif ($driver instanceof PdoDriver) {
                $resource = $driver->getResource();
                assert($resource instanceof PDO);
                if ($resource->getAttribute(PDO::ATTR_DRIVER_NAME) !== 'mysql' || $resource->inTransaction()) {
                    return;
                }
                // Bypass Dibi so its cached affected-row count is not overwritten.
                $result = $resource->query('SELECT 1');
                if ($result === false) {
                    $error = $resource->errorInfo();
                    $message = $error[2] ?? null;
                    $code = $error[1] ?? null;
                    throw new DriverException(
                        is_string($message) ? $message : 'MySQL connection health check failed.',
                        is_int($code) || is_string($code) ? $code : 0,
                    );
                }
                $result->closeCursor();
            } else {
                return;
            }
        } catch (DriverException | mysqli_sql_exception | PDOException $exception) {
            if (!$this->isLostConnection($exception)) {
                throw $exception;
            }
            // Keep the Dibi object: existing fluent builders, substitutions and
            // event listeners must continue to refer to this connection.
            $connection->disconnect();
            $connection->connect();
        }
    }

    private function isLostConnection(Throwable $exception): bool {
        if ($exception instanceof PDOException) {
            $code = $exception->errorInfo[1] ?? $exception->getCode();
        } elseif ($exception instanceof DriverException || $exception instanceof mysqli_sql_exception) {
            $code = $exception->getCode();
        } else {
            return false;
        }
        return in_array($code, [2006, 2013, 2055, '2006', '2013', '2055'], true);
    }

    /**
     * Execute a callback inside a transaction.
     *
     * The callback must return a boolean indicating if the transaction is successful or not.
     * If an exception is thrown inside the callback the transaction is automatically rolled back.
     *
     * @param  callable(Connection $connection):bool  $callback
     * @throws DriverException|Throwable
     */
    public function transaction(callable $callback) : void {
        $this->begin();
        try {
            if ($callback($this)) {
                $this->commit();
                return;
            }
        } catch (Throwable $e) {
            try {
                $this->rollback();
            } catch (Throwable) {
                // A dead connection can also fail rollback. Preserve the cause.
            }
            throw $e;
        }
        $this->rollback();
    }

    /**
     * @param  string|null  $savepoint
     * @return void
     * @throws DriverException
     */
    public function begin(?string $savepoint = null) : void {
        if ($this->transactionSavepoints === []) {
            $this->ensureConnected();
            $this->connection->begin($savepoint);
            $this->transactionSavepoints[] = $savepoint ?? '__transaction__';
            return;
        }

        $savepoint ??= 'lsr_nested_' . count($this->transactionSavepoints);
        $this->connection->query('SAVEPOINT %n', $savepoint);
        $this->transactionSavepoints[] = $savepoint;
    }

    /**
     * @param  string|null  $savepoint
     * @return void
     * @throws DriverException
     */
    public function rollback(?string $savepoint = null) : void {
        $currentSavepoint = end($this->transactionSavepoints);
        try {
            if (count($this->transactionSavepoints) <= 1) {
                $this->connection->rollback($savepoint);
                array_pop($this->transactionSavepoints);
                return;
            }
            $this->connection->query('ROLLBACK TO SAVEPOINT %n', $savepoint ?? $currentSavepoint);
            $this->connection->query('RELEASE SAVEPOINT %n', $savepoint ?? $currentSavepoint);
            $index = array_search($savepoint ?? $currentSavepoint, $this->transactionSavepoints, true);
            if ($index !== false) {
                array_splice($this->transactionSavepoints, $index);
            }
        } catch (Throwable $exception) {
            if ($this->isLostConnection($exception)) {
                if (!empty($this->config['autoReconnect']) && count($this->transactionSavepoints) <= 1) {
                    // PDO can retain inTransaction() after a failed rollback.
                    // Discard only after unwinding the outermost transaction.
                    $this->connection->disconnect();
                }
                array_pop($this->transactionSavepoints);
            }
            throw $exception;
        }
    }

    /**
     * @param  string|null  $savepoint
     * @return void
     * @throws DriverException
     */
    public function commit(?string $savepoint = null) : void {
        $currentSavepoint = end($this->transactionSavepoints);
        if (count($this->transactionSavepoints) <= 1) {
            $this->connection->commit($savepoint);
        } else {
            $this->connection->query('RELEASE SAVEPOINT %n', $savepoint ?? $currentSavepoint);
        }
        // A failed commit must not make subsequent work look non-transactional.
        array_pop($this->transactionSavepoints);
    }

    /**
     * @throws Exception
     */
    public function query(#[Language('GenericSQL')] string $query, mixed ...$args): Result {
        $this->ensureConnected();
        foreach ($args as $key => $arg) {
            if ($arg instanceof Fluent || $arg instanceof \Dibi\Fluent) {
                $args[$key] = $arg->__toString();
            }
        }
        return $this->connection->query($query, ...$args);
    }

    /**
     * Start query select
     *
     * @param  string[]|string|null  $table
     * @param  mixed  ...$args
     *
     * @return Fluent
     *
     * @since 1.0
     */
    public function select(array | string | null $table = null, ...$args) : Fluent {
        if (empty($args)) {
            $args = ['*'];
        }
        $query = $this->connection->select(...$args);
        if (is_string($table)) {
            $query->from($table);
        }
        else if (is_array($table)) {
            $query->from(...$table);
        }
        return $this->getFluent($query);
    }

    /**
     * Start query from a table before selecting fields.
     *
     * @param  string[]|string  $table
     * @param  mixed  ...$args
     *
     * @return Fluent
     */
    public function from(array | string $table, mixed ...$args) : Fluent {
        $query = $this->connection->select();
        if (is_string($table)) {
            $query->from($table, ...$args);
        }
        else {
            $query->from(...$table);
        }
        return $this->getFluent($query)->requireSelect();
    }

    public function getSelectForUpdateModifier() : ?string {
        return match ($this->getDriverFamily()) {
            'mysql', 'mysqli', 'mariadb', 'pgsql', 'postgres', 'postgresql', 'postgre', 'oci', 'oracle' => 'FOR UPDATE',
            default => null,
        };
    }

    public function isStrictSelectForUpdate() : bool {
        return $this->config['strictSelectForUpdate'] ?? self::DEFAULT_STRICT_SELECT_FOR_UPDATE;
    }

    public function assertSelectForUpdateSupported() : void {
        if ($this->getSelectForUpdateModifier() !== null) {
            return;
        }

        throw new LogicException(
            sprintf(
                'SELECT FOR UPDATE is not supported by the configured "%s" database driver.',
                $this->getDriverFamily()
            )
        );
    }

    private function getDriverFamily() : string {
        $driver = strtolower((string) $this->config['driver']);
        if ($driver !== 'pdo') {
            return $driver;
        }

        if (!empty($this->config['pdoDriver'])) {
            return strtolower((string) $this->config['pdoDriver']);
        }

        $dsn = strtolower((string) ($this->config['dsn'] ?? ''));
        if (preg_match('/^([a-z0-9_]+):/', $dsn, $matches) === 1) {
            return $matches[1];
        }

        return 'pdo';
    }

    /**
     * @param  \Dibi\Fluent  $query
     * @return Fluent
     */
    public function getFluent(\Dibi\Fluent $query) : Fluent {
        return new Fluent(
            $query,
            $this,
            $this->cache,
            $this->mapper,
            cacheNamespace: $this->cacheNamespace
        );
    }

    /**
     * Get query update
     *
     * @param  string  $table
     * @param  array<string, mixed>  $args
     * @param  array<int, mixed>|null  $where
     *
     * @return ($where is null ? Fluent : int)
     *
     * @throws Exception
     * @since 1.0
     */
    public function update(string $table, array $args, ?array $where = null) : Fluent | int {
        $q = $this->connection->update($table, $args);
        if (isset($where)) {
            $this->ensureConnected();
            /** @var int $rows */
            $rows = $q->where(...$where)->execute(\Dibi\Fluent::AffectedRows);
            return $rows;
        }
        return $this->getFluent($q);
    }

    /**
     * Insert values
     *
     * @param  string  $table
     * @param  array<string, mixed>  ...$args
     *
     * @return int
     * @throws Exception
     *
     * @since 1.0
     */
    public function insert(string $table, array ...$args) : int {
        $this->ensureConnected();
        if (count($args) > 1) {
            $result = $this->connection->command()
                ->insert()
                ->into('%n', $table, '(%n)', array_keys($args[0]))
                ->values(
                    '%l' . str_repeat(', %l', count($args) - 1),
                    ...$args
                )
                ->execute(\Dibi\Fluent::AffectedRows);
            assert(is_int($result));
            return $result;
        }
        $result = $this->connection->insert($table, ...$args)->execute(\Dibi\Fluent::AffectedRows);
        assert(is_int($result));
        return $result;
    }


    /**
     * Get query insert
     *
     * @param  string  $table
     * @param  iterable<string, mixed>  $args
     *
     * @return Fluent
     *
     * @since 1.0
     */
    public function insertGet(string $table, iterable $args) : Fluent {
        return $this->getFluent($this->connection->insert($table, $args));
    }

    /**
     * Insert value with IGNORE flag enabled
     *
     * @param  string  $table
     * @param  iterable<string, mixed>  $args
     *
     * @return int
     * @throws Exception
     */
    public function insertIgnore(string $table, iterable $args) : int {
        $this->ensureConnected();
        $query = $this->connection->insert($table, $args);
        if ($this->connection->getDriver() instanceof SqliteDriver) {
            $query->setFlag('OR IGNORE');
        }
        else {
            $query->setFlag('IGNORE');
        }
        $result = $query->execute(\Dibi\Fluent::AffectedRows);
        assert(is_int($result));
        return $result;
    }

    /**
     * Resets autoincrement value to the first available number
     *
     * @param  string  $table
     *
     * @return Result
     * @throws Exception
     */
    public function resetAutoIncrement(string $table) : Result {
        $this->ensureConnected();
        if ($this->config['driver'] === 'sqlite') {
            return $this->connection->query('delete from sqlite_sequence where name=%s;', $table);
        }
        return $this->connection->query('ALTER TABLE %n AUTO_INCREMENT = 1', $table);
    }

    /**
     * Get query insert
     *
     * @param  string  $table
     *
     * @return Fluent
     *
     * @since 1.0
     */
    public function deleteGet(string $table) : Fluent {
        return $this->getFluent($this->connection->delete($table));
    }

    /**
     * Insert values
     *
     * @param  string  $table
     * @param  array<int, mixed>  $where
     *
     * @return int
     * @throws Exception
     * @since 1.0
     */
    public function delete(string $table, array $where = []) : int {
        $this->ensureConnected();
        $query = $this->connection->delete($table);
        if (!empty($where)) {
            $query->where(...$where);
        }
        $result = $query->execute(\Dibi\Fluent::AffectedRows);
        assert(is_int($result));
        return $result;
    }

    /**
     * @param  string  $table
     * @param  array<string, mixed>|array<int, array<string, mixed>>  $values
     *
     * @return int
     * @throws Exception
     */
    public function replace(string $table, array $values) : int {
        $this->ensureConnected();
        $multiple = array_any($values, static fn($val) => is_array($val));

        $args = [];
        $valueArgs = [];
        $queryKeys = [];
        $rows = [];
        $row = [];
        foreach ($values as $key => $data) {
            if (is_array($data)) {
                $row = [];
                foreach ($data as $key2 => $val) {
                    $queryKeys[$key2] = '%n';
                    $args[$key2] = $key2;
                    $row[$key2] = $this->getEscapeType($val);
                    $valueArgs[] = $val;
                }
                $rows[] = '('.implode(', ', $row).')';
                continue;
            }
            $queryKeys[$key] = '%n';
            $args[$key] = $key;
            $row[$key] = $this->getEscapeType($data);
            $valueArgs[] = $data;
        }
        if (!$multiple) {
            $rows[] = '('.implode(', ', $row).')';
        }
        $args = array_merge($args, $valueArgs);

        // Split for debugging
        $sql = "REPLACE INTO %n (".implode(', ', $queryKeys).") VALUES ".implode(', ', $rows).";";
        return $this->connection->query($sql, $table, ...array_values($args))->count();
    }

    private function getEscapeType(mixed $value) : string {
        return match (true) {
            is_int($value)                      => '%i',
            is_float($value)                    => '%f',
            $value instanceof DateTimeInterface => '%dt',
            default                             => '%s',
        };
    }

    private function registerLifecycleListener(): void {
        if ($this->lifecycleHook() === null || (self::$lifecycleListeners[$this] ?? false)) {
            return;
        }
        $this->connection->onEvent[] = fn(Event $event) => $this->recordDibiEvent($event);
        self::$lifecycleListeners ??= new WeakMap();
        self::$lifecycleListeners[$this] = true;
    }

    private function recordDibiEvent(Event $event): void {
        $errorType = $event->result instanceof DriverException ? $event->result::class : null;
        $this->recordLifecycle(
            new DatabaseLifecycleEvent(
                $this->databaseOperation($event->type),
                $errorType === null ? DatabaseLifecycleEvent::SUCCESS : DatabaseLifecycleEvent::ERROR,
                max(0.0, $event->time),
                $this->databaseSystem(),
                $this->name,
                $event->count,
                $errorType,
                $this->includeRawSql() && $event->sql !== '' ? $event->sql : null,
            )
        );
    }

    private function recordLifecycle(DatabaseLifecycleEvent $event): void {
        $hook = $this->lifecycleHook();
        if ($hook === null) {
            return;
        }
        try {
            $hook->record($event);
        } catch (Throwable) {
            // Lifecycle hooks must never affect database operations.
        }
    }

    private function lifecycleHook(): ?DatabaseLifecycleHookInterface {
        return self::$lifecycleHooks[$this] ?? null;
    }

    private function includeRawSql(): bool {
        return self::$lifecycleRawSql[$this] ?? false;
    }

    private function databaseOperation(int $type): string {
        return match ($type) {
            Event::CONNECT => DatabaseLifecycleEvent::CONNECT,
            Event::SELECT => DatabaseLifecycleEvent::SELECT,
            Event::INSERT => DatabaseLifecycleEvent::INSERT,
            Event::UPDATE => DatabaseLifecycleEvent::UPDATE,
            Event::DELETE => DatabaseLifecycleEvent::DELETE,
            Event::BEGIN => DatabaseLifecycleEvent::BEGIN,
            Event::COMMIT => DatabaseLifecycleEvent::COMMIT,
            Event::ROLLBACK => DatabaseLifecycleEvent::ROLLBACK,
            default => DatabaseLifecycleEvent::QUERY,
        };
    }

    private function databaseSystem(): string {
        $driver = strtolower((string) $this->config['driver']);
        if ($driver === 'pdo') {
            $driver = strtolower((string) ($this->config['pdoDriver'] ?? ''));
            if ($driver === '') {
                $driver = strtolower(strstr((string) ($this->config['dsn'] ?? ''), ':', true) ?: 'pdo');
            }
        }
        return match ($driver) {
            'mysql', 'mysqli' => 'mysql',
            'pgsql', 'postgre', 'postgres', 'postgresql' => 'postgresql',
            'sqlite', 'sqlite3' => 'sqlite',
            'sqlsrv', 'mssql' => 'mssql',
            'oci', 'oracle' => 'oracle',
            default => 'other_sql',
        };
    }

    public function getInsertId() : int {
        return $this->connection->getInsertId();
    }

    public function getAffectedRows() : int {
        return $this->connection->getAffectedRows();
    }

    public function close() : void {
        if (
            (new ReflectionProperty($this, 'connection'))->isInitialized($this)
            && $this->connection->isConnected()
        ) {
            $this->connection->disconnect();
        }
        $this->transactionSavepoints = [];
    }

}
