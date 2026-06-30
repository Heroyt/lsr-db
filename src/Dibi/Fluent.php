<?php

namespace Lsr\Db\Dibi;

use Closure;
use Dibi\Fluent as DibiFluent;
use Dibi\Result;
use Dibi\Row;
use LogicException;
use Lsr\Caching\Cache;
use Lsr\Db\Connection;
use Lsr\Serializer\Mapper;
use ReflectionProperty;

/**
 * SQL builder via fluent interfaces.
 *
 * @method Fluent distinct()
 * @method Fluent where(...$cond)
 * @method Fluent groupBy(...$field)
 * @method Fluent having(...$cond)
 * @method Fluent orderBy(...$field)
 * @method Fluent limit(int $limit)
 * @method Fluent offset(int $offset)
 * @method Fluent join(...$table)
 * @method Fluent leftJoin(...$table)
 * @method Fluent innerJoin(...$table)
 * @method Fluent rightJoin(...$table)
 * @method Fluent outerJoin(...$table)
 * @method Fluent as(...$field)
 * @method Fluent on(...$cond)
 * @method Fluent and (...$cond)
 * @method Fluent or (...$cond)
 * @method Fluent using(...$cond)
 * @method Fluent into(...$cond)
 * @method Fluent values(...$cond)
 * @method Fluent set(...$args)
 * @method Fluent asc()
 * @method Fluent desc()
 */
final class Fluent
{
    use FetchFunctions;

    private const int ITERATOR_CHUNK_SIZE = 5;
    private const string DEFAULT_CACHE_EXPIRE = '1 hours';

    private ?string $queryHash = null;

    /** @var non-empty-string[] */
    private array $cacheTags = [];
    /** @var list<list<mixed>> */
    private array $resultSetups = [];
    private string $table;
    private bool $forUpdate = false;
    private bool $requiresSelect = false;
    public private(set) string $method;

    /**
     * @param  non-empty-string|int  $cacheExpire
     */
    public function __construct(
        private(set) DibiFluent $fluent,
        private readonly Connection $connection,
        private readonly Cache $cache,
        private readonly Mapper $mapper,
        private(set) string|int $cacheExpire = self::DEFAULT_CACHE_EXPIRE,
    ) {}

    /**
     * @param  string  $name
     * @param  array<string|int,mixed>  $arguments
     *
     * @return mixed
     * @noinspection PhpMissingParamTypeInspection
     */
    public static function __callStatic($name, $arguments) : mixed {
        return DibiFluent::$name(...$arguments);
    }

    public function select(mixed ...$field) : Fluent {
        $field = $this->transformArgs($field);
        $this->method = 'select';
        $this->requiresSelect = false;
        $this->fluent->select(...$field);
        $this->queryHash = null;
        return $this;
    }

    public function delete(mixed ...$cond) : Fluent {
        $cond = $this->transformArgs($cond);
        $this->method = 'delete';
        $this->forUpdate = false;
        $this->fluent->delete(...$cond);
        $this->queryHash = null;
        return $this;
    }

    public function update(mixed ...$cond) : Fluent {
        $cond = $this->transformArgs($cond);
        $this->method = 'update';
        $this->forUpdate = false;
        $this->fluent->update(...$cond);
        $this->queryHash = null;
        return $this;
    }

    public function insert(mixed ...$cond) : Fluent {
        $cond = $this->transformArgs($cond);
        $this->method = 'insert';
        $this->forUpdate = false;
        $this->fluent->insert(...$cond);
        $this->queryHash = null;
        return $this;
    }

    public function __clone() : void {
        $this->fluent = clone $this->fluent;
    }

    /**
     * @param  array<mixed>  $args
     * @return array<mixed>
     */
    private function transformArgs(array $args) : array {
        foreach ($args as $key => $arg) {
            if ($arg instanceof self) {
                $args[$key] = $arg->fluent;
            }
        }
        return $args;
    }

    private function getQueryHash() : string {
        if (!isset($this->queryHash)) {
            $this->queryHash = md5($this->__toString());
        }
        return $this->queryHash;
    }

    public function __toString() {
        return $this->getSql();
    }

    /**
     * @return $this
     */
    public function requireSelect() : Fluent {
        $this->requiresSelect = true;
        $this->queryHash = null;
        return $this;
    }

    /**
     * Locks selected rows for update until the current transaction is completed.
     *
     * This modifier is available only for drivers that support the trailing
     * "FOR UPDATE" locking clause. Unsupported drivers fail before executing
     * invalid SQL.
     *
     * @return $this
     */
    public function forUpdate(bool $enabled = true) : Fluent {
        if ($enabled) {
            $command = $this->fluent->getCommand();
            if ($command !== 'SELECT') {
                throw new LogicException('SELECT FOR UPDATE can only be used with SELECT queries.');
            }
            $this->connection->assertSelectForUpdateSupported();
        }

        $this->forUpdate = $enabled;
        $this->queryHash = null;
        return $this;
    }

    /**
     * Adds Result setup.
     *
     * @return $this
     */
    public function setupResult(string $method, mixed ...$args) : Fluent {
        $this->fluent->setupResult($method, ...$args);
        $setup = [$method];
        foreach ($args as $arg) {
            $setup[] = $arg;
        }
        $this->resultSetups[] = $setup;
        return $this;
    }

    /**
     * Generates and executes SQL query.
     *
     * @return ($return is DibiFluent::Identifier|DibiFluent::AffectedRows ? int : Result|null)
     */
    public function execute(?string $return = null) : Result|int|null {
        $this->assertSelectClauseReady();
        if (!$this->forUpdate) {
            return $this->fluent->execute($return);
        }

        $result = $this->executeSelect();
        return match ($return) {
            DibiFluent::Identifier => $this->connection->getInsertId(),
            DibiFluent::AffectedRows => $this->connection->getAffectedRows(),
            default => $result,
        };
    }

    /**
     * @param  list<mixed>  $exportArgs
     */
    private function executeSelect(array $exportArgs = []) : Result {
        $this->assertSelectClauseReady();
        $result = $this->connection->query($this->getSql($exportArgs));
        foreach ($this->resultSetups as $setup) {
            $method = array_shift($setup);
            assert(is_string($method));
            $result->$method(...$setup);
        }
        return $result;
    }

    /**
     * @return Row|array<mixed>|null
     */
    public function fetchRow() : Row | array | null {
        $this->assertSelectClauseReady();
        if (!$this->forUpdate) {
            return $this->normalizeFetchedRow($this->fluent->fetch());
        }

        return $this->normalizeFetchedRow(
            $this->executeSelect($this->shouldAddSingleRowLimit() ? ['%lmt', 1] : [])->fetch()
        );
    }

    public function fetchSingleValue() : mixed {
        $this->assertSelectClauseReady();
        if (!$this->forUpdate) {
            return $this->fluent->fetchSingle();
        }

        return $this->executeSelect($this->shouldAddSingleRowLimit() ? ['%lmt', 1] : [])->fetchSingle();
    }

    /**
     * @return Row[]
     */
    public function fetchAllRows(?int $offset = null, ?int $limit = null) : array {
        $this->assertSelectClauseReady();
        if (!$this->forUpdate) {
            return $this->fluent->fetchAll($offset, $limit);
        }

        return $this->executeSelect(['%ofs %lmt', $offset, $limit])->fetchAll();
    }

    /**
     * @return array<mixed>
     */
    public function fetchAssocRows(string $assoc) : array {
        $this->assertSelectClauseReady();
        if (!$this->forUpdate) {
            return $this->fluent->fetchAssoc($assoc);
        }

        return $this->executeSelect()->fetchAssoc($assoc);
    }

    /**
     * @return array<string, mixed>|array<int,mixed>
     */
    public function fetchPairRows(?string $key = null, ?string $value = null) : array {
        $this->assertSelectClauseReady();
        if (!$this->forUpdate) {
            return $this->fluent->fetchPairs($key, $value);
        }

        return $this->executeSelect()->fetchPairs($key, $value);
    }

    /**
     * @return Row|array<mixed>|null
     */
    private function normalizeFetchedRow(mixed $row) : Row | array | null {
        if ($row instanceof Row || is_array($row) || $row === null) {
            return $row;
        }

        throw new LogicException(sprintf('Unexpected row type "%s".', get_debug_type($row)));
    }

    private function shouldAddSingleRowLimit() : bool {
        if ($this->fluent->getCommand() !== 'SELECT') {
            return false;
        }

        return !$this->hasClause('LIMIT');
    }

    private function hasClause(string $name) : bool {
        $property = new ReflectionProperty(DibiFluent::class, 'clauses');
        $property->setAccessible(true);
        /** @var array<string, mixed> $clauses */
        $clauses = $property->getValue($this->fluent);
        return !empty($clauses[$name]);
    }

    /**
     * @param  list<mixed>  $exportArgs
     */
    private function getSql(array $exportArgs = []) : string {
        $this->assertSelectClauseReady();
        if (!$this->forUpdate && $exportArgs === []) {
            return $this->fluent->__toString();
        }

        /**
         * @param  string|null  $clause
         * @param  list<mixed>  $args
         * @return list<mixed>
         */
        $export = Closure::bind(
            fn(?string $clause = null, array $args = []) : array => $this->_export($clause, array_values($args)),
            $this->fluent,
            DibiFluent::class
        );
        /** @var list<mixed> $query */
        $query = $export(null, $exportArgs);
        $sql = $this->fluent->getConnection()->translate($query);

        if (!$this->forUpdate) {
            return $sql;
        }

        $modifier = $this->connection->getSelectForUpdateModifier();
        if ($modifier === null) {
            throw new LogicException('SELECT FOR UPDATE is not supported by the configured database driver.');
        }
        return $sql.' '.$modifier;
    }

    private function assertSelectClauseReady() : void {
        if (!$this->requiresSelect) {
            return;
        }

        throw new LogicException('A query started with from() requires a later select() call.');
    }

    /**
     * @return non-empty-string[]
     */
    private function getCacheTags() : array {
        $tags = $this->cacheTags;
        $tags[] = 'sql';
        if (isset($this->table)) {
            $tags[] = 'sql/'.$this->table;
        }
        return $tags;
    }

    /**
     * @param  non-empty-string|int  $expire
     * @return $this
     */
    public function cacheExpire(string|int $expire) : Fluent {
        $this->cacheExpire = $expire;
        return $this;
    }

    /**
     * @param  non-empty-string  ...$tags
     * @return $this
     */
    public function cacheTags(string ...$tags) : Fluent {
        $this->cacheTags = array_merge($this->cacheTags, $tags);
        return $this;
    }

    public function from(string $table, mixed ...$args) : Fluent {
        foreach ($args as $key => $arg) {
            if ($arg instanceof self) {
                $args[$key] = $arg->fluent;
            }
        }
        $this->table = $table;
        $this->fluent->from($table, ...$args);
        $this->queryHash = null;
        return $this;
    }

    /**
     * @param  string  $name
     *
     * @return mixed
     */
    public function __get($name) : mixed {
        return $this->fluent->$name;
    }

    /**
     * @param  string  $name
     * @param  mixed  $value
     *
     * @return void
     */
    public function __set($name, $value) : void {
        $this->fluent->$name = $value;
    }

    /**
     * @param  string  $name
     *
     * @return bool
     */
    public function __isset($name) {
        return isset($this->fluent->$name);
    }

    /**
     * @param  string  $name
     * @param  array<string|int,mixed>  $arguments
     *
     * @return $this|mixed
     */
    public function __call($name, $arguments) {
        $arguments = $this->transformArgs($arguments);
        $return = $this->fluent->$name(...$arguments);
        if ($return === $this->fluent) {
            $this->queryHash = null;
            return $this;
        }
        return $return;
    }

}
