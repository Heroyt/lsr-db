<?php
/**
 * @file      DB.php
 * @brief     Database connection handling
 * @author    Tomáš Vojík <vojik@wboy.cz>
 * @date      2021-09-22
 * @version   1.0
 * @since     1.0
 */


namespace Lsr\Db;

use Dibi\Helpers;
use Dibi\Result;
use Lsr\Caching\Cache;
use Lsr\Db\Dibi\Fluent;
use Lsr\Serializer\Mapper;
use RuntimeException;

/**
 * @class   DB
 * @brief   Class responsible for managing Database connection and storing common queries
 * @details Database abstraction layer for managing database connection. It uses a Dibi library to connect to the
 *   database and expands on it, adding some common queries as single methods.
 *
 * @package Core
 *
 * @author  Tomáš Vojík <vojik@wboy.cz>
 * @version 1.0
 * @since   1.0
 * @phpstan-type Config array{
 *          driver?: string,
 *          host?: string,
 *          port?: numeric,
 *          user?: string,
 *          password?: string,
 *          database?: string,
 *          collate?: string,
 *          prefix?: string,
 *          lazy?: string|bool,
 *  }
 *
 * @method static void transaction(callable $callback)
 * @method static void begin(string|null $savepoint=null)
 * @method static void commit(string|null $savepoint=null)
 * @method static void rollback(string|null $savepoint=null)
 * @method static Result query(string $query, mixed ...$args)
 * @method static Fluent select(string|string[]|null $table = null, mixed ...$args = null)
 * @method static Fluent|int update(string $table, array $args, array|null $where = null)
 * @method static int insert(string $table, array ...$args)
 * @method static Fluent insertGet(string $table, array ...$args)
 * @method static int insertIgnore(string $table, array ...$args)
 * @method static int delete(string $table, array $where = [])
 * @method static Fluent deleteGet(string $table)
 * @method static int replace(string $table, array $values)
 * @method static int getInsertId()
 * @method static int getAffectedRows()
 * @method static Result resetAutoIncrement(string $table)
 */
class DB
{

    /**
     * @var Connection $db Dibi Database connection
     */
    protected static Connection $db;

    public static function init(Connection $db) : void {
        self::$db = $db;
    }

    /**
     * @param  Cache  $cache
     * @param  Mapper  $mapper
     * @param  Config  $config
     * @return Connection
     */
    public static function getMain(
        Cache  $cache,
        Mapper $mapper,
        array  $config = [],
    ) : Connection {
        Helpers::alias($config, 'user', 'username');
        Helpers::alias($config, 'password', 'pass');

        /** @var Config $config */

        // Default config from ENV
        if (empty($config)) {
            $driver = getenv('DB_driver');
            if ($driver === false) {
                $driver = 'mysqli';
            }
            $host = getenv('DB_host');
            if ($host === false) {
                $host = 'localhost';
            }
            $port = getenv('DB_port');
            if ($port === false) {
                $port = 3306;
            }
            $user = getenv('DB_user');
            if ($user === false) {
                $user = 'root';
            }
            $password = getenv('DB_password');
            if ($password === false) {
                $password = '';
            }
            $database = getenv('DB_NAME');
            if ($database === false) {
                $database = '';
            }
            $collate = getenv('DB_collate');
            if ($collate === false) {
                $collate = 'utf8mb4';
            }
            $prefix = getenv('DB_prefix');
            if ($prefix === false) {
                $prefix = '';
            }
            $lazy = getenv('DB_lazy');
            if ($lazy === false) {
                $lazy = '';
            }
            $config = [
                'driver'   => $driver,
                'host'     => $host,
                'port'     => (int) $port,
                'user'     => $user,
                'password' => $password,
                'database' => $database,
                'collate'  => $collate,
                'prefix'   => $prefix,
                'lazy'     => $lazy,
            ];
        }

        // Build valid options
        $options = [
            'lazy'   => !empty($config['lazy']),
            'driver' => $config['driver'] ?? 'mysqli',
        ];
        assert(!empty($options['driver']));
        if (!empty($config['host'])) {
            $options['host'] = $config['host'];
        }
        if (!empty($config['port'])) {
            $options['port'] = (int) $config['port'];
        }
        if (!empty($config['user'])) {
            $options['username'] = $config['user'];
        }
        if (!empty($config['password'])) {
            $options['password'] = $config['password'];
        }
        if (!empty($config['database'])) {
            $options['database'] = $config['database'];
        }
        if (!empty($config['collate'])) {
            $options['charset'] = $config['collate'];
        }
        if (!empty($config['prefix'])) {
            $options['prefix'] = $config['prefix'];
        }

        // Instantiate connection
        return new Connection(
            $cache,
            $mapper,
            $options,
            'main'
        );
    }

    /**
     * @param  string  $name
     * @param  mixed[]  $arguments
     * @return mixed
     */
    public static function __callStatic(string $name, array $arguments) : mixed {
        if (!isset(self::$db)) {
            throw new RuntimeException('Database is not connected');
        }
        return self::$db->{$name}(...array_values($arguments));
    }


    /**
     * Get connection class
     *
     * @return Connection
     *
     * @since 1.0
     */
    public static function getConnection() : Connection {
        if (!isset(self::$db)) {
            throw new RuntimeException('Database is not initialized');
        }
        return self::$db;
    }

    public static function close() : void {
        if (isset(self::$db)) {
            self::$db->close();
        }
    }
}
