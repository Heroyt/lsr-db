<?php

declare(strict_types=1);
/** @noinspection PhpDocMissingThrowsInspection */
/** @noinspection PhpUndefinedFieldInspection */
/** @noinspection SqlResolve */
/** @noinspection PhpUnhandledExceptionInspection */

namespace TestCases;

use DateTime;
use Dibi\Result;
use Dibi\Row;
use Exception;
use InvalidArgumentException;
use LogicException;
use Lsr\Caching\Cache;
use Lsr\Db\Connection;
use Lsr\Db\DB;
use Lsr\Db\Dibi\Fluent;
use Lsr\Serializer\Mapper;
use Lsr\Serializer\Normalizer\DateTimeNormalizer;
use Lsr\Serializer\Normalizer\DibiRowNormalizer;
use Nette\Caching\Storages\DevNullStorage;
use Nette\Caching\Storages\MemoryStorage;
use PDO;
use PHPUnit\Framework\Attributes\Depends;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\PropertyInfo\Extractor\ReflectionExtractor;
use Symfony\Component\Serializer\Normalizer\ArrayDenormalizer;
use Symfony\Component\Serializer\Normalizer\BackedEnumNormalizer;
use Symfony\Component\Serializer\Normalizer\JsonSerializableNormalizer;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;
use Symfony\Component\Serializer\Serializer;
use Throwable;

/**
 * Database abstraction test suite
 *
 * @author Tomáš Vojík
 */
class DBTest extends TestCase
{
    private Cache $cache;
    private Mapper $mapper;

    public function tearDown(): void {
        $this->dropTable();
        DB::close();
        DB::resetConnections();
        $files = glob(TMP_DIR . '*.db');
        if (is_array($files)) {
            foreach ($files as $file) {
                unlink($file);
            }
        }
        parent::tearDown();
    }

    public function dropTable(): void {
        try {
            DB::getConnection()->query("DROP TABLE table2");
            DB::getConnection()->query("DROP TABLE table1");
        } catch (Throwable) {

        }
    }

    public function test_uninitialized_select(): void {
        $this->expectException(RuntimeException::class);
        DB::select('table1', '*');
    }

    public function test_uninitialized_insert(): void {
        $this->expectException(RuntimeException::class);
        DB::insert('table1', []);
    }

    public function test_uninitialized_insert_ignore(): void {
        $this->expectException(RuntimeException::class);
        DB::insertIgnore('table1', []);
    }

    public function test_uninitialized_insert_get(): void {
        $this->expectException(RuntimeException::class);
        DB::insertGet('table1', []);
    }

    public function test_uninitialized_update(): void {
        $this->expectException(RuntimeException::class);
        DB::update('table1', []);
    }

    public function test_uninitialized_delete(): void {
        $this->expectException(RuntimeException::class);
        DB::delete('table1');
    }

    public function test_uninitialized_delete_get(): void {
        $this->expectException(RuntimeException::class);
        DB::deleteGet('table1');
    }

    public function test_uninitialized_replace(): void {
        $this->expectException(RuntimeException::class);
        DB::replace('table1', []);
    }

    public function test_uninitialized_get_insert_id(): void {
        $this->expectException(RuntimeException::class);
        DB::getInsertId();
    }

    public function test_uninitialized_reset_autoincrement(): void {
        $this->expectException(RuntimeException::class);
        DB::resetAutoIncrement('table');
    }

    public function test_uninitialized_get_affected_rows(): void {
        $this->expectException(RuntimeException::class);
        DB::getAffectedRows();
    }

    #[Depends('testInitSqlite')]
    public function test_insert(): void {
        $this->initSqlite();
        $count = DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        self::assertEquals(1, $count);
    }

    #[Depends('testInitSqlite')]
    public function test_insert_transactional(): void {
        $this->initSqlite();
        DB::transaction(
            static function (Connection $connection) {
                $connection->insert(
                    'table1',
                    [
                        'name' => 'transaction1',
                        'age'  => null,
                    ],
                );
                $connection->insert(
                    'table1',
                    [
                        'name' => 'transaction2',
                        'age'  => null,
                    ],
                );
                return true;
            },
        );
        $rows = DB::select('table1')
            ->where('name IN %in', ['transaction1', 'transaction2'])
            ->fetchAll();
        $this->assertCount(2, $rows);

        DB::transaction(
            static function (Connection $connection) {
                $connection->insert(
                    'table1',
                    [
                        'name' => 'transaction3',
                        'age'  => null,
                    ],
                );
                return false;
            },
        );
        $rows = DB::select('table1')
            ->where('name IN %in', ['transaction3'])
            ->fetchAll();
        $this->assertCount(0, $rows);

        try {
            DB::transaction(
                static function (Connection $connection): void {
                    $connection->insert(
                        'table1',
                        [
                            'name' => 'transaction4',
                            'age'  => null,
                        ],
                    );
                    throw new Exception('Thrown exception');
                },
            );
        } catch (Exception $e) {
            $this->assertEquals('Thrown exception', $e->getMessage());
        }
        $rows = DB::select('table1')
            ->where('name IN %in', ['transaction4'])
            ->fetchAll();
        $this->assertCount(0, $rows);
    }

    /**
     * @param array<string, mixed> $config
     */
    public function initSqlite(array $config = []): void {
        $fileName = uniqid('', true) . '.db';
        DB::init(
            DB::getMain(
                $this->cache,
                $this->mapper,
                array_merge(
                    [
                        'database' => ROOT . "tests/tmp/{$fileName}",
                        'driver'   => "sqlite",
                        'prefix'   => "",
                    ],
                    $config,
                ),
            ),
        );
        $this->initSqliteTable();
    }

    /**
     * @param array<string, mixed> $config
     */
    public function initPdoSqlite(array $config = []): void {
        if ( ! extension_loaded('pdo_sqlite')) {
            self::markTestSkipped('PDO SQLite extension is not available.');
        }

        $fileName = uniqid('', true) . '.db';
        DB::init(
            DB::getMain(
                $this->cache,
                $this->mapper,
                array_merge(
                    [
                        'database' => ROOT . "tests/tmp/{$fileName}",
                        'driver' => 'pdo',
                        'pdoDriver' => 'sqlite',
                        'prefix' => '',
                    ],
                    $config,
                ),
            ),
        );
        $this->initSqliteTable();
    }

    public function initSqliteTable(): void {
        DB::getConnection()->query(
            "
			CREATE TABLE table1 ( 
			    id integer PRIMARY KEY autoincrement NOT NULL , 
			    name char(60) NOT NULL, 
			    age int 
			);
		",
        );
        DB::getConnection()->query(
            "
			CREATE TABLE table2 ( 
			    id integer PRIMARY KEY autoincrement NOT NULL ,
			    table_1_id integer,
			    name varchar(60) NOT NULL 
			);
		",
        );
    }

    private function createSqliteConnection(?string $name = null, ?Cache $cache = null): Connection {
        $fileName = uniqid('', true) . '.db';
        $connection = DB::createConnection(
            $cache ?? $this->cache,
            $this->mapper,
            [
                'database' => ROOT . "tests/tmp/{$fileName}",
                'driver'   => "sqlite",
                'prefix'   => "",
            ],
            $name,
        );
        $connection->query(
            "
			CREATE TABLE table1 (
			    id integer PRIMARY KEY autoincrement NOT NULL ,
			    name char(60) NOT NULL,
			    age int
			);
		",
        );

        return $connection;
    }

    #[Depends('testInitMysql')]
    public function test_insert_multiple(): void {
        $this->initMysql();
        $count = DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
            [
                'name' => 'test2',
                'age'  => 10,
            ],
            [
                'name' => 'test3',
                'age'  => 99,
            ],
        );
        self::assertEquals(3, DB::select('table1', 'count(*)')->fetchSingle());
        self::assertEquals(3, $count);
    }

    public function initMysql(): void {
        $port = getenv('LSR_DB_TEST_PORT');
        if ($port === false || $port === '') {
            self::markTestSkipped('Set LSR_DB_TEST_PORT to an isolated MySQL integration server.');
        }
        DB::init(
            DB::getMain(
                $this->cache,
                $this->mapper,
                [
                    'driver'   => 'mysqli',
                    'host'     => '127.0.0.1',
                    'port'     => (int) $port,
                    'user'     => 'root',
                    'password' => '',
                    'database' => 'reconnect_test',
                    'collate'  => 'utf8mb4',
                ],
            ),
        );
        $this->initMysqlTable();
    }

    public function initMysqlTable(): void {
        DB::getConnection()->query(
            "
			CREATE TABLE IF NOT EXISTS table1 ( 
			    id int(11) UNSIGNED NOT NULL AUTO_INCREMENT, 
			    name varchar(60) NOT NULL, 
			    age int(10) UNSIGNED,
			    date datetime DEFAULT NULL,
			    PRIMARY KEY (`id`)
			);
		",
        );
        DB::getConnection()->query(
            "
			CREATE TABLE IF NOT EXISTS table2 ( 
			    id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
			    table_1_id int(11) UNSIGNED DEFAULT NULL,
			    name varchar(60) NOT NULL, 
			    PRIMARY KEY (`id`),
			    CONSTRAINT table_1_fk FOREIGN KEY (`table_1_id`) REFERENCES table1 (id) ON DELETE SET NULL 
			);
		",
        );
    }

    #[Depends('testInitMysql')]
    public function test_insert_ignore(): void {
        $this->initMysql();
        $count = DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        $id = DB::getInsertId();
        self::assertEquals(1, $count);
        $count = DB::insertIgnore(
            'table1',
            [
                'id'   => $id,
                'name' => 'test2',
            ],
        );
        self::assertEquals(0, $count);
    }

    #[Depends('testInitMysql')]
    public function test_get_affected_rows(): void {
        $this->initMysql();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        self::assertEquals(1, DB::getAffectedRows());
    }

    public function test_init_sqlite(): void {
        // Init SQLite
        $this->initSqlite();
        self::assertTrue(DB::getConnection()->isConnected());
        DB::close();
        self::assertFalse(DB::getConnection()->isConnected());
    }

    public function test_init_pdo_sqlite(): void {
        $this->initPdoSqlite();
        self::assertTrue(DB::getConnection()->isConnected());
        DB::close();
        self::assertFalse(DB::getConnection()->isConnected());
    }

    public function test_create_connection_uses_explicit_test_config(): void {
        $connection = $this->createSqliteConnection('test');
        $connection->insert('table1', ['name' => 'test', 'age' => null]);

        self::assertSame(1, $connection->select('table1', 'count(*)')->fetchSingle());
    }

    public function test_named_connection_can_become_active_connection(): void {
        $connection = $this->createSqliteConnection();

        DB::initNamed('test', $connection);
        DB::useConnection('test');

        self::assertSame($connection, DB::getConnection());
        self::assertSame($connection, DB::getConnection('test'));
    }

    public function test_main_named_connection_stays_active_and_registered(): void {
        $connection = $this->createSqliteConnection('main');

        DB::initNamed('main', $connection);

        self::assertSame($connection, DB::getConnection());
        self::assertSame($connection, DB::getConnection('main'));
    }

    public function test_empty_connection_name_is_rejected(): void {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Database connection name cannot be empty');

        DB::initNamed('', $this->createSqliteConnection('test'));
    }

    public function test_unknown_named_connection_is_rejected(): void {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Database connection "missing" is not initialized');

        DB::getConnection('missing');
    }

    public function test_with_connection_restores_previous_static_connection(): void {
        $this->initSqlite();
        DB::insert('table1', ['name' => 'main', 'age' => null]);
        $mainConnection = DB::getConnection();
        $testConnection = $this->createSqliteConnection('test');
        DB::initNamed('test', $testConnection);

        $count = DB::withConnection(
            'test',
            static function (): int {
                DB::insert('table1', ['name' => 'test', 'age' => null]);
                return (int) DB::select('table1', 'count(*)')->fetchSingle();
            },
        );

        self::assertSame(1, $count);
        self::assertSame($mainConnection, DB::getConnection());
        self::assertSame(1, DB::select('table1', 'count(*)')->fetchSingle());
        self::assertSame(0, DB::select('table1', 'count(*)')->where('name = %s', 'test')->fetchSingle());
    }

    public function test_with_connection_restores_previous_connection_after_exception(): void {
        $this->initSqlite();
        $mainConnection = DB::getConnection();
        DB::initNamed('test', $this->createSqliteConnection('test'));

        try {
            DB::withConnection(
                'test',
                static function (): never {
                    throw new RuntimeException('Test failure');
                },
            );
            self::fail('Expected scoped callback to throw');
        } catch (RuntimeException $exception) {
            self::assertSame('Test failure', $exception->getMessage());
        }

        self::assertSame($mainConnection, DB::getConnection());
    }

    public function test_with_connection_restores_uninitialized_state(): void {
        DB::resetConnections();
        $testConnection = $this->createSqliteConnection('test');

        self::assertSame(
            $testConnection,
            DB::withConnection($testConnection, static fn (): Connection => DB::getConnection()),
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Database is not initialized');
        DB::getConnection();
    }

    public function test_with_connection_supports_nested_scopes(): void {
        $this->initSqlite();
        $mainConnection = DB::getConnection();
        $firstConnection = $this->createSqliteConnection('first');
        $secondConnection = $this->createSqliteConnection('second');
        DB::initNamed('first', $firstConnection);
        DB::initNamed('second', $secondConnection);

        DB::withConnection(
            'first',
            static function () use ($firstConnection, $secondConnection): void {
                self::assertSame($firstConnection, DB::getConnection());
                DB::withConnection(
                    'second',
                    static fn () => self::assertSame($secondConnection, DB::getConnection()),
                );
                self::assertSame($firstConnection, DB::getConnection());
            },
        );

        self::assertSame($mainConnection, DB::getConnection());
    }

    public function test_reset_connections_clears_active_and_named_connections(): void {
        $connection = $this->createSqliteConnection('test');
        DB::initNamed('test', $connection);
        DB::useConnection('test');

        DB::resetConnections();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Database connection "test" is not initialized');
        DB::getConnection('test');
    }

    public function test_named_connections_with_same_name_use_separate_query_caches(): void {
        $cache = new Cache(new MemoryStorage());
        $firstConnection = $this->createSqliteConnection('test', $cache);
        $secondConnection = $this->createSqliteConnection('test', $cache);
        $firstConnection->insert('table1', ['name' => 'first', 'age' => null]);
        $secondConnection->insert('table1', ['name' => 'second', 'age' => null]);

        self::assertSame('first', $firstConnection->select('table1', 'name')->fetchSingle());
        self::assertSame('second', $secondConnection->select('table1', 'name')->fetchSingle());
        self::assertSame('first', $firstConnection->select('table1', 'name')->fetchSingle());
    }

    public function test_for_update_ignores_unsupported_sqlite_driver_by_default(): void {
        $this->initSqlite();

        $sql = (string) DB::select('table1')->forUpdate();

        self::assertSame('SELECT * FROM [table1]', $sql);
    }

    public function test_for_update_ignores_unsupported_pdo_sqlite_driver_by_default(): void {
        $this->initPdoSqlite();

        $sql = (string) DB::select('table1')->forUpdate();

        self::assertSame('SELECT * FROM [table1]', $sql);
    }

    public function test_for_update_rejects_unsupported_sqlite_driver_in_strict_mode(): void {
        $this->initSqlite(['strictSelectForUpdate' => true]);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('SELECT FOR UPDATE is not supported');

        DB::select('table1')->forUpdate();
    }

    public function test_for_update_rejects_unsupported_pdo_sqlite_driver_in_strict_mode(): void {
        $this->initPdoSqlite(['strictSelectForUpdate' => true]);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('SELECT FOR UPDATE is not supported');

        DB::select('table1')->forUpdate();
    }

    public function test_pdo_config_builds_mysql_dsn(): void {
        $connection = DB::getMain(
            $this->cache,
            $this->mapper,
            [
                'driver' => 'pdo',
                'pdoDriver' => 'mysql',
                'host' => 'localhost',
                'port' => 3306,
                'user' => 'root',
                'password' => '',
                'database' => 'test',
                'collate' => 'utf8mb4',
                'lazy' => true,
            ],
        );

        self::assertSame('pdo', $connection->connection->getConfig('driver'));
        self::assertSame('mysql:host=localhost;port=3306;dbname=test;charset=utf8mb4', $connection->connection->getConfig('dsn'));
        self::assertSame('root', $connection->connection->getConfig('username'));
        self::assertSame('localhost', $connection->connection->getConfig('host'));
        self::assertSame(3306, $connection->connection->getConfig('port'));
        self::assertTrue($connection->connection->getConfig('lazy'));
    }

    public function test_pdo_config_uses_explicit_dsn(): void {
        $connection = DB::getMain(
            $this->cache,
            $this->mapper,
            [
                'driver' => 'pdo',
                'dsn' => 'sqlite::memory:',
                'options' => [PDO::ATTR_TIMEOUT => 2],
                'lazy' => true,
            ],
        );

        self::assertSame('pdo', $connection->connection->getConfig('driver'));
        self::assertSame('sqlite::memory:', $connection->connection->getConfig('dsn'));
        self::assertSame([PDO::ATTR_TIMEOUT => 2], $connection->connection->getConfig('options'));
        self::assertTrue($connection->connection->getConfig('lazy'));
    }

    public function test_direct_connection_builds_pdo_dsn(): void {
        $connection = new Connection(
            $this->cache,
            $this->mapper,
            [
                'driver' => 'pdo',
                'pdoDriver' => 'mysql',
                'host' => 'localhost',
                'port' => 3306,
                'user' => 'root',
                'password' => '',
                'database' => 'test',
                'collate' => 'utf8mb4',
                'lazy' => true,
            ],
        );

        self::assertSame('pdo', $connection->connection->getConfig('driver'));
        self::assertSame('mysql:host=localhost;port=3306;dbname=test;charset=utf8mb4', $connection->connection->getConfig('dsn'));
    }

    public function test_direct_connection_builds_aliased_pdo_dsn(): void {
        $connection = new Connection(
            $this->cache,
            $this->mapper,
            [
                'driver' => 'pdo_mysql',
                'host' => 'localhost',
                'port' => 3306,
                'user' => 'root',
                'password' => '',
                'database' => 'test',
                'collate' => 'utf8mb4',
                'lazy' => true,
            ],
        );

        self::assertSame('pdo', $connection->connection->getConfig('driver'));
        self::assertSame('mysql:host=localhost;port=3306;dbname=test;charset=utf8mb4', $connection->connection->getConfig('dsn'));
    }

    public function test_init_mysql(): void {
        // Init MySQL
        $this->initMysql();
        self::assertTrue(DB::getConnection()->isConnected());
        DB::close();
        self::assertFalse(DB::getConnection()->isConnected());
    }

    #[Depends('testInitMysql')]
    public function test_for_update_mysql_sql_generation(): void {
        $this->initMysql();

        $sql = (string) DB::select('table1')
            ->where('id = %i', 1)
            ->limit(1)
            ->forUpdate();

        self::assertSame('SELECT * FROM `table1` WHERE id = 1 LIMIT 1 FOR UPDATE', $sql);
    }

    #[Depends('testInitMysql')]
    public function test_reset_auto_increment(): void {
        $this->initMysql();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        DB::insert(
            'table1',
            [
                'name' => 'test2',
                'age'  => 10,
            ],
        );
        DB::delete('table1');
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        self::assertEquals(3, DB::getInsertId());
        DB::delete('table1');
        DB::resetAutoIncrement('table1');
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        self::assertEquals(1, DB::getInsertId());
    }

    #[Depends('testInitSqlite')]
    public function test_reset_auto_increment_sqlite(): void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        DB::insert(
            'table1',
            [
                'name' => 'test2',
                'age'  => 10,
            ],
        );
        DB::delete('table1');
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        self::assertEquals(3, DB::getInsertId());
        DB::delete('table1');
        DB::resetAutoIncrement('table1');
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        self::assertEquals(1, DB::getInsertId());
    }

    #[Depends('testInitSqlite')]
    public function test_insert_get(): void {
        $this->initSqlite();
        $query = DB::insertGet(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        /** @noinspection UnnecessaryAssertionInspection */
        self::assertInstanceOf(Fluent::class, $query);
        /** @var Result $count */
        $count = $query->execute();
        self::assertEquals(1, $count->count());
    }

    #[Depends('testInitMysql')]
    public function test_update(): void {
        $this->initMysql();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        $id = DB::getInsertId();
        $count = DB::update(
            'table1',
            [
                'name' => 'hello!',
            ],
            [
                'id = %i',
                $id,
            ],
        );
        self::assertIsInt($count);
        self::assertEquals(1, $count);
        /** @var Row|null $row */
        $row = DB::select('table1', '*')->where('id = %i', $id)->fetch();
        self::assertNotNull($row);
        self::assertEquals('hello!', $row->name);
        self::assertEquals(null, $row->age);

        $query = DB::update(
            'table1',
            [
                'name' => 'hello!',
            ],
        );
        self::assertInstanceOf(Fluent::class, $query);
        $query->execute();
        $row = DB::select('table1', '*')->where('id = %i', $id)->fetch();
        self::assertNotNull($row);
        /** @phpstan-ignore-next-line */
        self::assertEquals('hello!', $row->name);
        /** @phpstan-ignore-next-line */
        self::assertEquals(null, $row->age);
    }

    #[Depends('testInitSqlite')]
    public function test_delete_get(): void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        $id = DB::getInsertId();
        /** @var Result $query */
        $query = DB::deleteGet('table1')->where('id = %i', $id)->execute();
        self::assertEquals(1, $query->count());
    }

    #[Depends('testInitSqlite')]
    public function test_delete(): void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        $id = DB::getInsertId();
        $count = DB::delete('table1', ['id = %i', $id]);
        self::assertEquals(1, $count);
    }

    #[Depends('testInitMysql')]
    public function test_replace(): void {
        $this->initMysql();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        $id = DB::getInsertId();
        DB::replace(
            'table1',
            [
                'id'   => $id,
                'name' => 'name',
                'age'  => 1,
            ],
        );
        /** @var Row|null $row */
        $row = DB::select('table1', '*')->where('id = %i', $id)->fetch(cache: false);
        self::assertNotNull($row);
        self::assertEquals('name', $row->name);
        self::assertEquals(1, $row->age);
        DB::replace(
            'table1',
            [
                [
                    'id'   => $id,
                    'name' => 'name2',
                    'age'  => 12,
                    'date' => null,
                ],
                [
                    'id'   => $id + 1,
                    'name' => 'abc',
                    'age'  => 30,
                    'date' => new DateTime('now'),
                ],
            ],
        );
        $row = DB::select('table1', '*')->where('id = %i', $id)->fetch(cache: false);
        self::assertNotNull($row);
        /** @phpstan-ignore-next-line */
        self::assertEquals('name2', $row->name);
        /** @phpstan-ignore-next-line */
        self::assertEquals(12, $row->age);
        $row = DB::select('table1', '*')->where('id = %i', $id + 1)->fetch(cache: false);
        self::assertNotNull($row);
        /** @phpstan-ignore-next-line */
        self::assertEquals('abc', $row->name);
        /** @phpstan-ignore-next-line */
        self::assertEquals(30, $row->age);
    }

    #[Depends('testInsert')]
    public function test_select(): void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'test2',
                'age'  => 12,
            ],
        );
        //$id2 = DB::getInsertId();
        DB::insert(
            'table2',
            [
                'name'       => 'test3',
                'table_1_id' => $id1,
            ],
        );
        //$id3 = DB::getInsertId();
        DB::insert(
            'table2',
            [
                'name'       => 'test4',
                'table_1_id' => null,
            ],
        );
        //$id4 = DB::getInsertId();

        // Simple select
        $rows = DB::select('table1', '*')->fetchAll(cache: false);
        self::assertCount(2, $rows);

        // Join select with alias
        $rows = DB::select(['table1', 'a'], 'a.id, a.name, a.age, b.name as value')
            ->join('table2', 'b')
            ->on('a.id = b.table_1_id')
            ->fetchAll(cache: false);
        self::assertCount(1, $rows);
        /** @var Row $row */
        $row = first($rows);
        self::assertEquals($id1, $row->id);
        self::assertEquals('test1', $row->name);
        self::assertEquals(null, $row->age);
        self::assertEquals('test3', $row->value);

        // Test select with no table
        $rows = DB::select()->from('table1', 'a')->fetchAll(cache: false);
        self::assertCount(2, $rows);

        // Test from starter followed by select
        $rows = DB::from('table1')
            ->select('*')
            ->fetchAll(cache: false);
        self::assertCount(2, $rows);

        // Test from starter with alias followed by select
        $rows = DB::from(['table1', 'a'])
            ->select('a.id, a.name')
            ->fetchAll(cache: false);
        self::assertCount(2, $rows);
    }

    #[Depends('testInitSqlite')]
    public function test_from_requires_select(): void {
        $this->initSqlite();

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('requires a later select() call');

        DB::from('table1')->fetchAll(cache: false);
    }

    #[Depends('testSelect')]
    public function test_select_dto(): void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'Hello',
                'age'  => null,
            ],
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'AAAAAAAA',
                'age'  => 69,
            ],
        );
        $id2 = DB::getInsertId();

        $rows = DB::select('table1', '*')->fetchAllDto(Table1Dto::class, cache: false);
        self::assertCount(2, $rows);
        foreach ($rows as $row) {
            self::assertInstanceOf(Table1Dto::class, $row);
        }

        self::assertEquals($id1, $rows[0]->id);
        self::assertEquals($id2, $rows[1]->id);
        self::assertEquals('Hello', $rows[0]->name);
        self::assertEquals('AAAAAAAA', $rows[1]->name);
        self::assertEquals(null, $rows[0]->age);
        self::assertEquals(69, $rows[1]->age);
    }

    #[Depends('testSelect')]
    public function test_select_iterator(): void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'dasdads',
                'age'  => null,
            ],
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'jijlkmn',
                'age'  => 90,
            ],
        );
        $id2 = DB::getInsertId();

        $rows = DB::select('table1', '*')->fetchIterator(cache: false);
        $count = 0;
        foreach ($rows as $key => $row) {
            self::assertInstanceOf(Row::class, $row);

            switch ($key) {
                case 0:
                    self::assertEquals($id1, $row->id);
                    self::assertEquals('dasdads', $row->name);
                    self::assertEquals(null, $row->age);
                    break;
                case 1:
                    self::assertEquals($id2, $row->id);
                    self::assertEquals('jijlkmn', $row->name);
                    self::assertEquals(90, $row->age);
                    break;
            }

            $count++;
        }
        self::assertEquals(2, $count);


    }

    #[Depends('testSelect')]
    public function test_select_iterator_dto(): void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'ijoink',
                'age'  => null,
            ],
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'uuuuuuuuu',
                'age'  => 456,
            ],
        );
        $id2 = DB::getInsertId();

        $rows = DB::select('table1', '*')->fetchIteratorDto(Table1Dto::class, cache: false);
        $count = 0;
        foreach ($rows as $key => $row) {
            self::assertInstanceOf(Table1Dto::class, $row);

            switch ($key) {
                case 0:
                    self::assertEquals($id1, $row->id);
                    self::assertEquals('ijoink', $row->name);
                    self::assertEquals(null, $row->age);
                    break;
                case 1:
                    self::assertEquals($id2, $row->id);
                    self::assertEquals('uuuuuuuuu', $row->name);
                    self::assertEquals(456, $row->age);
                    break;
            }

            $count++;
        }
        self::assertEquals(2, $count);


    }

    #[Depends('testInsert')]
    public function test_select_cache(): void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ],
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'test2',
                'age'  => 12,
            ],
        );
        //$id2 = DB::getInsertId();
        DB::insert(
            'table2',
            [
                'name'       => 'test3',
                'table_1_id' => $id1,
            ],
        );
        //$id3 = DB::getInsertId();
        DB::insert(
            'table2',
            [
                'name'       => 'test4',
                'table_1_id' => null,
            ],
        );
        //$id4 = DB::getInsertId();

        // Simple select
        $rows = DB::select('table1', '*')->fetchAll(cache: true);
        self::assertCount(2, $rows);

        // Join select with alias
        $rows = DB::select(['table1', 'a'], 'a.id, a.name, a.age, b.name as value')
            ->join('table2', 'b')
            ->on('a.id = b.table_1_id')
            ->fetchAll(cache: true);
        self::assertCount(1, $rows);
        /** @var Row $row */
        $row = first($rows);
        self::assertEquals($id1, $row->id);
        self::assertEquals('test1', $row->name);
        self::assertEquals(null, $row->age);
        self::assertEquals('test3', $row->value);
    }

    #[Depends('testSelect')]
    public function test_select_dto_cache(): void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'Hello',
                'age'  => null,
            ],
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'AAAAAAAA',
                'age'  => 69,
            ],
        );
        $id2 = DB::getInsertId();

        $rows = DB::select('table1', '*')->fetchAllDto(Table1Dto::class, cache: true);
        self::assertCount(2, $rows);
        foreach ($rows as $row) {
            self::assertInstanceOf(Table1Dto::class, $row);
        }

        self::assertEquals($id1, $rows[0]->id);
        self::assertEquals($id2, $rows[1]->id);
        self::assertEquals('Hello', $rows[0]->name);
        self::assertEquals('AAAAAAAA', $rows[1]->name);
        self::assertEquals(null, $rows[0]->age);
        self::assertEquals(69, $rows[1]->age);
    }

    #[Depends('testSelect')]
    public function test_select_iterator_cache(): void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'dasdads',
                'age'  => null,
            ],
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'jijlkmn',
                'age'  => 90,
            ],
        );
        $id2 = DB::getInsertId();

        $rows = DB::select('table1', '*')->fetchIterator(cache: true);
        $count = 0;
        foreach ($rows as $key => $row) {
            self::assertInstanceOf(Row::class, $row);

            switch ($key) {
                case 0:
                    self::assertEquals($id1, $row->id);
                    self::assertEquals('dasdads', $row->name);
                    self::assertEquals(null, $row->age);
                    break;
                case 1:
                    self::assertEquals($id2, $row->id);
                    self::assertEquals('jijlkmn', $row->name);
                    self::assertEquals(90, $row->age);
                    break;
            }

            $count++;
        }
        self::assertEquals(2, $count);


    }

    #[Depends('testSelect')]
    public function test_select_iterator_dto_cache(): void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'ijoink',
                'age'  => null,
            ],
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'uuuuuuuuu',
                'age'  => 456,
            ],
        );
        $id2 = DB::getInsertId();

        $rows = DB::select('table1', '*')->fetchIteratorDto(Table1Dto::class, cache: true);
        $count = 0;
        foreach ($rows as $key => $row) {
            self::assertInstanceOf(Table1Dto::class, $row);

            switch ($key) {
                case 0:
                    self::assertEquals($id1, $row->id);
                    self::assertEquals('ijoink', $row->name);
                    self::assertEquals(null, $row->age);
                    break;
                case 1:
                    self::assertEquals($id2, $row->id);
                    self::assertEquals('uuuuuuuuu', $row->name);
                    self::assertEquals(456, $row->age);
                    break;
            }

            $count++;
        }
        self::assertEquals(2, $count);


    }

    public function test_exists(): void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'ijoink',
                'age'  => 10,
            ],
        );
        DB::insert(
            'table1',
            [
                'name' => 'ijoink',
                'age'  => 20,
            ],
        );

        $result = DB::select('table1', '*')->where('age > 10')->exists();
        self::assertTrue($result);
        $result = DB::select('table1', '*')->where('age > 10')->exists(false);
        self::assertTrue($result);
        $result = DB::select('table1', '*')->where('age < 10')->exists();
        self::assertFalse($result);
        $result = DB::select('table1', '*')->where('age < 10')->exists(false);
        self::assertFalse($result);
    }

    protected function setUp(): void {
        $this->cache = new Cache(
            new DevNullStorage(),
        );
        $this->mapper = new Mapper(
            new Serializer(
                [
                    new ArrayDenormalizer(),
                    new DateTimeNormalizer(),
                    new DibiRowNormalizer(),
                    new BackedEnumNormalizer(),
                    new JsonSerializableNormalizer(),
                    new ObjectNormalizer(propertyTypeExtractor: new ReflectionExtractor(), ),
                ],
            ),
        );
    }
}

class Table1Dto
{
    public int $id;
    public string $name;
    public ?int $age;
}
