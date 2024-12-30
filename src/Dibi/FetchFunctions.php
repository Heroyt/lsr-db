<?php
declare(strict_types=1);

namespace Lsr\Db\Dibi;

use Dibi\Exception;
use Dibi\Row;
use Iterator;
use Lsr\Caching\Cache;
use Throwable;

trait FetchFunctions
{
    /**
     * @template T of object
     * @param  class-string<T>  $class
     * @param  bool  $cache
     * @return T|null
     * @throws Exception
     */
    public function fetchDto(string $class, bool $cache = true) : ?object {
        if (!$cache) {
            /** @phpstan-ignore return.type */
            return $this->fluent
                ->execute()
                ?->setRowClass($class)
                ?->setRowFactory($this->getRowFactory($class))
                ?->fetch();
        }
        try {
            /** @phpstan-ignore return.type */
            return $this->cache->load(
                'sql/'.$this->getQueryHash().'/fetch/'.$class,
                fn() => $this->fluent->execute()
                                     ?->setRowClass($class)
                                     ?->setRowFactory($this->getRowFactory($class))
                                     ?->fetch(),
                [
                    Cache::Expire => $this->cacheExpire,
                    Cache::Tags   => $this->getCacheTags(),
                ]
            );
        } catch (Throwable) {
            /** @phpstan-ignore return.type */
            return $this->fluent
                ->execute()
                ?->setRowClass($class)
                ?->setRowFactory($this->getRowFactory($class))
                ?->fetch();
        }
    }

    /**
     * Generates, executes SQL query and fetches the single row.
     *
     * @return Row|null|array<string,mixed>
     */
    public function fetch(bool $cache = true) : Row | array | null {
        if (!$cache) {
            /** @phpstan-ignore return.type */
            return $this->fluent->fetch();
        }
        try {
            /** @phpstan-ignore return.type */
            return $this->cache->load(
                'sql/'.$this->getQueryHash().'/fetch',
                fn() => $this->fluent->fetch(),
                [
                    Cache::Expire => $this->cacheExpire,
                    Cache::Tags   => $this->getCacheTags(),
                ]
            );
        } catch (Throwable) {
            /** @phpstan-ignore return.type */
            return $this->fluent->fetch();
        }
    }


    /**
     * Like fetch(), but returns only first field.
     *
     * @return mixed  value on success, null if no next record
     */
    public function fetchSingle(bool $cache = true) : mixed {
        if (!$cache) {
            return $this->fluent->fetchSingle();
        }
        try {
            return $this->cache->load(
                'sql/'.$this->getQueryHash().'/fetchSingle',
                fn() => $this->fluent->fetchSingle(),
                [
                    Cache::Expire => $this->cacheExpire,
                    Cache::Tags   => $this->getCacheTags(),
                ]
            );
        } catch (Throwable) {
            return $this->fluent->fetchSingle();
        }
    }

    /**
     * Fetches all records from table.
     *
     * @template T of object
     * @param  class-string<T>  $class
     * @param  int|null  $offset
     * @param  int|null  $limit
     * @param  bool  $cache
     * @return T[]
     * @throws Exception
     */
    public function fetchAllDto(string $class, ?int $offset = null, ?int $limit = null, bool $cache = true) : array {
        if (!$cache) {
            /** @phpstan-ignore return.type */
            return $this->fluent->execute()
                                ?->setRowClass($class)
                                ?->setRowFactory($this->getRowFactory($class))
                                ?->fetchAll();
        }
        try {
            /** @phpstan-ignore return.type */
            return $this->cache->load(
                'sql/'.$this->getQueryHash().'/fetchAll/'.$offset.'/'.$limit.'/'.$class,
                fn() => $this->fluent->execute()
                                     ?->setRowClass($class)
                                     ?->setRowFactory($this->getRowFactory($class))
                                     ?->fetchAll(),
                [
                    Cache::Expire => $this->cacheExpire,
                    Cache::Tags   => $this->getCacheTags(),
                ]
            );
        } catch (Throwable) {
            /** @phpstan-ignore return.type */
            return $this->fluent->execute()
                                ?->setRowClass($class)
                                ?->setRowFactory($this->getRowFactory($class))
                                ?->fetchAll();
        }
    }

    /**
     * Fetches all records from table.
     *
     * @return Row[]
     */
    public function fetchAll(?int $offset = null, ?int $limit = null, bool $cache = true) : array {
        if (!$cache) {
            /** @phpstan-ignore return.type */
            return $this->fluent->fetchAll($offset, $limit);
        }
        try {
            /** @phpstan-ignore return.type */
            return $this->cache->load(
                'sql/'.$this->getQueryHash().'/fetchAll/'.$offset.'/'.$limit,
                fn() => $this->fluent->fetchAll($offset, $limit),
                [
                    Cache::Expire => $this->cacheExpire,
                    Cache::Tags   => $this->getCacheTags(),
                ]
            );
        } catch (Throwable) {
            /** @phpstan-ignore return.type */
            return $this->fluent->fetchAll($offset, $limit);
        }
    }

    /**
     * @template T of object
     * @param  class-string<T>  $class
     * @param  bool  $cache
     * @return Iterator<T>
     * @throws Exception
     */
    public function fetchIteratorDto(string $class, bool $cache = true) : Iterator {
        if (!$cache) {
            $query = $this->fluent->execute()
                                  ?->setRowClass($class)
                                  ?->setRowFactory($this->getRowFactory($class));
            while ($row = $query?->fetch()) {
                /** @var T $row */
                yield $row;
            }
            return;
        }

        $chunkIndex = 0;
        while (true) {
            $chunk = $this->cache->load(
                'sql/'.$this->getQueryHash().'/iterator/'.$chunkIndex.'/'.$class,
                fn() => $this->fetchAll($chunkIndex * $this::ITERATOR_CHUNK_SIZE, $this::ITERATOR_CHUNK_SIZE, false),
                [
                    Cache::Expire => $this->cacheExpire,
                    Cache::Tags   => $this->getCacheTags(),
                ]
            );
            $chunkIndex++;

            foreach ($chunk as $row) {
                yield $this->mapper->map($row, $class);
            }

            if (count($chunk) < $this::ITERATOR_CHUNK_SIZE) {
                break;
            }
        }
    }

    /**
     * @param  bool  $cache
     * @return Iterator<Row>
     * @throws Exception
     */
    public function fetchIterator(bool $cache = true) : Iterator {
        if (!$cache) {
            $query = $this->fluent->execute();
            while ($row = $query?->fetch()) {
                assert($row instanceof Row);
                yield $row;
            }
            return;
        }

        $chunkIndex = 0;
        while (true) {
            $chunk = $this->cache->load(
                'sql/'.$this->getQueryHash().'/iterator/'.$chunkIndex,
                fn() => $this->fetchAll($chunkIndex * $this::ITERATOR_CHUNK_SIZE, $this::ITERATOR_CHUNK_SIZE, false),
                [
                    Cache::Expire => $this->cacheExpire,
                    Cache::Tags   => $this->getCacheTags(),
                ]
            );
            $chunkIndex++;

            foreach ($chunk as $row) {
                yield $row;
            }

            if (count($chunk) < $this::ITERATOR_CHUNK_SIZE) {
                break;
            }
        }
    }

    /**
     * Fetches all records from table and returns associative tree.
     *
     * @template T of object
     *
     * @param  class-string<T>  $class
     * @param  string  $assoc  associative descriptor
     *
     * @return array<string, T>|array<int, T>
     * @throws Exception
     */
    public function fetchAssocDto(string $class, string $assoc, bool $cache = true) : array {
        if (!$cache) {
            /** @phpstan-ignore return.type */
            return $this->fluent->execute()
                                ?->setRowClass($class)
                                ?->setRowFactory($this->getRowFactory($class))
                                ?->fetchAssoc($assoc) ?? [];
        }
        try {
            /** @phpstan-ignore return.type */
            return $this->cache->load(
                'sql/'.$this->getQueryHash().'/fetchAssoc/'.$assoc,
                fn() => $this->fluent->execute()
                                     ?->setRowClass($class)
                                     ?->setRowFactory($this->getRowFactory($class))
                                     ?->fetchAssoc($assoc) ?? [],
                [
                    Cache::Expire => $this->cacheExpire,
                    Cache::Tags   => $this->getCacheTags(),
                ]
            );
        } catch (Throwable) {
            /** @phpstan-ignore return.type */
            return $this->fluent->execute()
                                ?->setRowClass($class)
                                ?->setRowFactory($this->getRowFactory($class))
                                ?->fetchAssoc($assoc) ?? [];
        }
    }

    /**
     * Fetches all records from table and returns associative tree.
     *
     * @param  string  $assoc  associative descriptor
     *
     * @return array<string, Row>|array<int, Row>
     */
    public function fetchAssoc(string $assoc, bool $cache = true) : array {
        if (!$cache) {
            /** @phpstan-ignore return.type */
            return $this->fluent->fetchAssoc($assoc);
        }
        try {
            /** @phpstan-ignore return.type */
            return $this->cache->load(
                'sql/'.$this->getQueryHash().'/fetchAssoc/'.$assoc,
                fn() => $this->fluent->fetchAssoc($assoc),
                [
                    Cache::Expire => $this->cacheExpire,
                    Cache::Tags   => $this->getCacheTags(),
                ]
            );
        } catch (Throwable) {
            /** @phpstan-ignore return.type */
            return $this->fluent->fetchAssoc($assoc);
        }
    }

    /**
     * Fetches all records from table like $key => $value pairs.
     *
     * @return array<string, mixed>|array<int,mixed>
     */
    public function fetchPairs(?string $key = null, ?string $value = null, bool $cache = true) : array {
        if (!$cache) {
            /** @phpstan-ignore return.type */
            return $this->fluent->fetchPairs($key, $value);
        }
        try {
            /** @phpstan-ignore return.type */
            return $this->cache->load(
                'sql/'.$this->getQueryHash().'/fetchPairs/'.$key.'/'.$value,
                fn() => $this->fluent->fetchPairs($key, $value),
                [
                    Cache::Expire => $this->cacheExpire,
                    Cache::Tags   => $this->getCacheTags(),
                ]
            );
        } catch (Throwable) {
            /** @phpstan-ignore return.type */
            return $this->fluent->fetchPairs($key, $value);
        }
    }

    /**
     * Fetches the row count
     *
     * @param  bool  $cache
     *
     * @return int
     */
    public function count(bool $cache = true) : int {
        if (!$cache) {
            return $this->fluent->count();
        }
        try {
            return $this->cache->load(
                'sql/'.$this->getQueryHash().'/count',
                fn() : int => $this->fluent->count(),
                [
                    Cache::Expire => $this->cacheExpire,
                    Cache::Tags   => $this->getCacheTags(),
                ]
            );
        } catch (Throwable) {
            return $this->fluent->count();
        }
    }

    /**
     * Wraps the current query in the SQL EXISTS() function.
     *
     * @param  bool  $cache
     * @return bool
     * @throws Exception
     */
    public function exists(bool $cache = true) : bool {
        if (!$cache) {
            return !empty($this->connection->query('SELECT EXISTS(%sql)', $this)->fetchSingle());
        }
        try {
            return $this->cache->load(
                'sql/'.$this->getQueryHash().'/exists',
                fn() : bool => !empty($this->connection->query('SELECT EXISTS(%sql)', $this)->fetchSingle()),
                [
                    Cache::Expire => $this->cacheExpire,
                    Cache::Tags   => $this->getCacheTags(),
                ]
            );
        } catch (Throwable) {
            return !empty($this->connection->query('SELECT EXISTS(%sql)', $this)->fetchSingle());
        }
    }

    /**
     * @template T of object
     * @param  class-string<T>  $type
     * @return callable(mixed $data):T
     */
    private function getRowFactory(string $type) : callable {
        return fn(mixed $data) : object => $this->mapper->map($data, $type);
    }
}