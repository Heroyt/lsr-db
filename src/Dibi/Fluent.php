<?php

namespace Lsr\Db\Dibi;

use Dibi\Fluent as DibiFluent;
use Dibi\Result;
use Lsr\Caching\Cache;
use Lsr\Db\Connection;
use Lsr\Serializer\Mapper;

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
 * @method Result|int|null execute(string $return = null)
 */
final class Fluent
{
    use FetchFunctions;

    private const int ITERATOR_CHUNK_SIZE = 5;
    private const string DEFAULT_CACHE_EXPIRE = '1 hours';

    private ?string $queryHash = null;

    /** @var non-empty-string[] */
    private array $cacheTags = [];
    private string $table;
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
        $this->fluent->select(...$field);
        $this->queryHash = null;
        return $this;
    }

    public function delete(mixed ...$cond) : Fluent {
        $cond = $this->transformArgs($cond);
        $this->method = 'delete';
        $this->fluent->delete(...$cond);
        $this->queryHash = null;
        return $this;
    }

    public function update(mixed ...$cond) : Fluent {
        $cond = $this->transformArgs($cond);
        $this->method = 'update';
        $this->fluent->update(...$cond);
        $this->queryHash = null;
        return $this;
    }

    public function insert(mixed ...$cond) : Fluent {
        $cond = $this->transformArgs($cond);
        $this->method = 'insert';
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
            $this->queryHash = md5($this->fluent->__toString());
        }
        return $this->queryHash;
    }

    public function __toString() {
        return $this->fluent->__toString();
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