<?php
declare(strict_types=1);

namespace Lsr\Db;

use DateTimeInterface;
use Dibi\Connection as DibiConnection;
use Dibi\DriverException;
use Dibi\Exception;
use Dibi\Result;
use JetBrains\PhpStorm\Language;
use Lsr\Caching\Cache;
use Lsr\Db\Dibi\Fluent;
use Lsr\Logging\Logger;
use Lsr\Serializer\Mapper;
use Throwable;

/**
 * @phpstan-type Config array{
 *     lazy?: bool,
 *     driver: non-empty-string,
 *     host?: non-empty-string,
 *     port?: int,
 *     user?: string,
 *     password?: string,
 *     database?: non-empty-string,
 *     collate?: non-empty-string,
 *     prefix?: string,
 * }
 */
final class Connection
{

    public DibiConnection $connection {
        get {
            if (!isset($this->connection)) {
                $this->connection = new DibiConnection($this->config, $this->name);
                if (!empty($this->config['prefix'])) {
                    $this->connection->getSubstitutes()->{''} = $this->config['prefix'];
                }
                $this->connection->onEvent[] = [$this->logger, 'logDb'];
            }
            return $this->connection;
        }
    }

    /** @phpstan-ignore property.unused */
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
        private readonly array   $config,
        /** @phpstan-ignore property.onlyWritten */
        private readonly ?string $name = null,
    ) {
        if ($this->config['driver'] === 'sqlite') {
            /** @phpstan-ignore constant.notFound, binaryOp.invalid */
            $dbFile = $this->config['database'] ?? TMP_DIR.'db.db';
            if (!file_exists($dbFile)) {
                touch($dbFile);
            }
        }
    }

    /**
     * @param  non-empty-string  $name
     * @param  mixed[]  $arguments
     * @return mixed
     */
    public function __call(string $name, array $arguments) : mixed {
        return $this->connection->$name(...$arguments);
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
            $success = $callback($this);

        } catch (Throwable $e) {
            $this->rollback();
            throw $e;
        }
        if ($success) {
            $this->commit();
            return;
        }
        $this->rollback();
    }

    /**
     * @param  string|null  $savepoint
     * @return void
     * @throws DriverException
     */
    public function begin(?string $savepoint = null) : void {
        $this->connection->begin($savepoint);
    }

    /**
     * @param  string|null  $savepoint
     * @return void
     * @throws DriverException
     */
    public function rollback(?string $savepoint = null) : void {
        $this->connection->rollback($savepoint);
    }

    /**
     * @param  string|null  $savepoint
     * @return void
     * @throws DriverException
     */
    public function commit(?string $savepoint = null) : void {
        $this->connection->commit($savepoint);
    }

    /**
     * @throws Exception
     */
    public function query(#[Language('GenericSQL')] string $query, mixed ...$args): Result {
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
     * @param  \Dibi\Fluent  $query
     * @return Fluent
     */
    public function getFluent(\Dibi\Fluent $query) : Fluent {
        return new Fluent($query, $this, $this->cache, $this->mapper);
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
        if (count($args) > 1) {
            $result = $this->connection->command()
                                    ->insert()
                                    ->into('%n', $table, '(%n)', array_keys($args[0]))
                                    ->values(
                                           '%l'.str_repeat(', %l', count($args) - 1),
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
        $result = $this->connection->insert($table, $args)->setFlag('IGNORE')->execute(\Dibi\Fluent::AffectedRows);
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
     * @param  array<string, mixed>|array<array<string, mixed>>  $values
     *
     * @return int
     * @throws Exception
     */
    public function replace(string $table, array $values) : int {
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

    public function getInsertId() : int {
        return $this->connection->getInsertId();
    }

    public function getAffectedRows() : int {
        return $this->connection->getAffectedRows();
    }

    public function close() : void {
        if ($this->connection->isConnected()) {
            $this->connection->disconnect();
        }
    }

}