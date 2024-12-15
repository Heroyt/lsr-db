<?php
/** @noinspection PhpDocMissingThrowsInspection */
/** @noinspection PhpUndefinedFieldInspection */
/** @noinspection SqlResolve */
/** @noinspection PhpUnhandledExceptionInspection */

namespace TestCases;

use DateTime;
use Dibi\Result;
use Dibi\Row;
use Lsr\Caching\Cache;
use Lsr\Db\Connection;
use Lsr\Db\DB;
use Lsr\Db\Dibi\Fluent;
use Lsr\Serializer\Mapper;
use Lsr\Serializer\Normalizer\DateTimeNormalizer;
use Lsr\Serializer\Normalizer\DibiRowNormalizer;
use Nette\Caching\Storages\DevNullStorage;
use Nette\Caching\Storages\SQLiteStorage;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Depends;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\PropertyInfo\Extractor\ReflectionExtractor;
use Symfony\Component\Serializer\Normalizer\ArrayDenormalizer;
use Symfony\Component\Serializer\Normalizer\BackedEnumNormalizer;
use Symfony\Component\Serializer\Normalizer\JsonSerializableNormalizer;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;
use Symfony\Component\Serializer\Serializer;

/**
 * Database abstraction test suite
 *
 * @author Tomáš Vojík
 */
class DBTest extends TestCase
{
    private Cache $cache;
    private Mapper $mapper;

    public function tearDown() : void {
        $this->dropTable();
        DB::close();
        $files = glob(TMP_DIR.'*.db');
        if (is_array($files)) {
            foreach ($files as $file) {
                unlink($file);
            }
        }
        parent::tearDown();
    }

    public function dropTable() : void {
        try {
            DB::getConnection()->query("DROP TABLE table2");
            DB::getConnection()->query("DROP TABLE table1");
        } catch (RuntimeException) {

        }
    }

    public function testUninitializedSelect() : void {
        $this->expectException(RuntimeException::class);
        DB::select('table1', '*');
    }

    public function testUninitializedInsert() : void {
        $this->expectException(RuntimeException::class);
        DB::insert('table1', []);
    }

    public function testUninitializedInsertIgnore() : void {
        $this->expectException(RuntimeException::class);
        DB::insertIgnore('table1', []);
    }

    public function testUninitializedInsertGet() : void {
        $this->expectException(RuntimeException::class);
        DB::insertGet('table1', []);
    }

    public function testUninitializedUpdate() : void {
        $this->expectException(RuntimeException::class);
        DB::update('table1', []);
    }

    public function testUninitializedDelete() : void {
        $this->expectException(RuntimeException::class);
        DB::delete('table1');
    }

    public function testUninitializedDeleteGet() : void {
        $this->expectException(RuntimeException::class);
        DB::deleteGet('table1');
    }

    public function testUninitializedReplace() : void {
        $this->expectException(RuntimeException::class);
        DB::replace('table1', []);
    }

    public function testUninitializedGetInsertId() : void {
        $this->expectException(RuntimeException::class);
        DB::getInsertId();
    }

    public function testUninitializedResetAutoincrement() : void {
        $this->expectException(RuntimeException::class);
        DB::resetAutoIncrement('table');
    }

    public function testUninitializedGetAffectedRows() : void {
        $this->expectException(RuntimeException::class);
        DB::getAffectedRows();
    }

    #[Depends('testInitSqlite')]
    public function testInsert() : void {
        $this->initSqlite();
        $count = DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
        );
        self::assertEquals(1, $count);
    }

    #[Depends('testInitSqlite')]
    public function testInsertTransactional() : void {
        $this->initSqlite();
        DB::transaction(
            static function (Connection $connection) {
            $connection->insert(
                'table1',
                [
                    'name' => 'transaction1',
                    'age'  => null,
                ]
            );
            $connection->insert(
                'table1',
                [
                    'name' => 'transaction2',
                    'age'  => null,
                ]
            );
            return true;
        });
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
                    ]
                );
                return false;
            }
        );
        $rows = DB::select('table1')
                  ->where('name IN %in', ['transaction3'])
                  ->fetchAll();
        $this->assertCount(0, $rows);

        try {
            DB::transaction(
                static function (Connection $connection) {
                    $connection->insert(
                        'table1',
                        [
                            'name' => 'transaction4',
                            'age'  => null,
                        ]
                    );
                    throw new \Exception('Thrown exception');
                }
            );
        } catch (\Exception $e) {
            $this->assertEquals('Thrown exception', $e->getMessage());
        }
        $rows = DB::select('table1')
                  ->where('name IN %in', ['transaction4'])
                  ->fetchAll();
        $this->assertCount(0, $rows);
    }

    public function initSqlite() : void {
        $fileName = uniqid('', true).'.db';
        DB::init(
            DB::getMain(
                $this->cache,
                $this->mapper,
                [
                    'database' => ROOT."tests/tmp/$fileName",
                    'driver'   => "sqlite",
                    'prefix'   => "",
                ]
            )
        );
        $this->initSqliteTable();
    }

    public function initSqliteTable() : void {
        DB::getConnection()->query(
            "
			CREATE TABLE table1 ( 
			    id integer PRIMARY KEY autoincrement NOT NULL , 
			    name char(60) NOT NULL, 
			    age int 
			);
		"
        );
        DB::getConnection()->query(
            "
			CREATE TABLE table2 ( 
			    id integer PRIMARY KEY autoincrement NOT NULL ,
			    table_1_id integer,
			    name varchar(60) NOT NULL 
			);
		"
        );
    }

    #[Depends('testInitMysql')]
    public function testInsertMultiple() : void {
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
            ]
        );
        self::assertEquals(3, DB::select('table1', 'count(*)')->fetchSingle());
        self::assertEquals(3, $count);
    }

    public function initMysql() : void {
        DB::init(
            DB::getMain(
                $this->cache,
                $this->mapper,
                [
                    'driver'   => 'mysqli',
                    'hosts'    => 'localhost',
                    'port'     => 3306,
                    'user'     => 'root',
                    'password' => '',
                    'database' => 'test',
                    'collate'  => 'utf8mb4',
                ]
            )
        );
        $this->initMysqlTable();
    }

    public function initMysqlTable() : void {
        DB::getConnection()->query(
            "
			CREATE TABLE IF NOT EXISTS table1 ( 
			    id int(11) UNSIGNED NOT NULL AUTO_INCREMENT, 
			    name varchar(60) NOT NULL, 
			    age int(10) UNSIGNED,
			    date datetime DEFAULT NULL,
			    PRIMARY KEY (`id`)
			);
		"
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
		"
        );
    }

    #[Depends('testInitMysql')]
    public function testInsertIgnore() : void {
        $this->initMysql();
        $count = DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
        );
        $id = DB::getInsertId();
        self::assertEquals(1, $count);
        $count = DB::insertIgnore(
            'table1',
            [
                'id'   => $id,
                'name' => 'test2',
            ]
        );
        self::assertEquals(0, $count);
    }

    #[Depends('testInitMysql')]
    public function testGetAffectedRows() : void {
        $this->initMysql();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
        );
        self::assertEquals(1, DB::getAffectedRows());
    }

    public function testInitSqlite() : void {
        // Init SQLite
        $this->initSqlite();
        self::assertTrue(DB::getConnection()->isConnected());
        DB::close();
        self::assertFalse(DB::getConnection()->isConnected());
    }

    public function testInitMysql() : void {
        // Init MySQL
        $this->initMysql();
        self::assertTrue(DB::getConnection()->isConnected());
        DB::close();
        self::assertFalse(DB::getConnection()->isConnected());
    }

    #[Depends('testInitMysql')]
    public function testResetAutoIncrement() : void {
        $this->initMysql();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
        );
        DB::insert(
            'table1',
            [
                'name' => 'test2',
                'age'  => 10,
            ]
        );
        DB::delete('table1');
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
        );
        self::assertEquals(3, DB::getInsertId());
        DB::delete('table1');
        DB::resetAutoIncrement('table1');
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
        );
        self::assertEquals(1, DB::getInsertId());
    }

    #[Depends('testInitSqlite')]
    public function testInsertGet() : void {
        $this->initSqlite();
        $query = DB::insertGet(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
        );
        /** @noinspection UnnecessaryAssertionInspection */
        self::assertInstanceOf(Fluent::class, $query);
        /** @var Result $count */
        $count = $query->execute();
        self::assertEquals(1, $count->count());
    }

    #[Depends('testInitMysql')]
    public function testUpdate() : void {
        $this->initMysql();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
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
            ]
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
            ]
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
    public function testDeleteGet() : void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
        );
        $id = DB::getInsertId();
        /** @var Result $query */
        $query = DB::deleteGet('table1')->where('id = %i', $id)->execute();
        self::assertEquals(1, $query->count());
    }

    #[Depends('testInitSqlite')]
    public function testDelete() : void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
        );
        $id = DB::getInsertId();
        $count = DB::delete('table1', ['id = %i', $id]);
        self::assertEquals(1, $count);
    }

    #[Depends('testInitMysql')]
    public function testReplace() : void {
        $this->initMysql();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
        );
        $id = DB::getInsertId();
        DB::replace(
            'table1',
            [
                'id'   => $id,
                'name' => 'name',
                'age'  => 1,
            ]
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
            ]
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
    public function testSelect() : void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'test2',
                'age'  => 12,
            ]
        );
        //$id2 = DB::getInsertId();
        DB::insert(
            'table2',
            [
                'name'       => 'test3',
                'table_1_id' => $id1,
            ]
        );
        //$id3 = DB::getInsertId();
        DB::insert(
            'table2',
            [
                'name'       => 'test4',
                'table_1_id' => null,
            ]
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
    }

    #[Depends('testSelect')]
    public function testSelectDto() : void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'Hello',
                'age'  => null,
            ]
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'AAAAAAAA',
                'age'  => 69,
            ]
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
    public function testSelectIterator() : void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'dasdads',
                'age'  => null,
            ]
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'jijlkmn',
                'age'  => 90,
            ]
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
    public function testSelectIteratorDto() : void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'ijoink',
                'age'  => null,
            ]
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'uuuuuuuuu',
                'age'  => 456,
            ]
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
    public function testSelectCache() : void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'test1',
                'age'  => null,
            ]
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'test2',
                'age'  => 12,
            ]
        );
        //$id2 = DB::getInsertId();
        DB::insert(
            'table2',
            [
                'name'       => 'test3',
                'table_1_id' => $id1,
            ]
        );
        //$id3 = DB::getInsertId();
        DB::insert(
            'table2',
            [
                'name'       => 'test4',
                'table_1_id' => null,
            ]
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
    public function testSelectDtoCache() : void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'Hello',
                'age'  => null,
            ]
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'AAAAAAAA',
                'age'  => 69,
            ]
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
    public function testSelectIteratorCache() : void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'dasdads',
                'age'  => null,
            ]
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'jijlkmn',
                'age'  => 90,
            ]
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
    public function testSelectIteratorDtoCache() : void {
        $this->initSqlite();
        DB::insert(
            'table1',
            [
                'name' => 'ijoink',
                'age'  => null,
            ]
        );
        $id1 = DB::getInsertId();
        DB::insert(
            'table1',
            [
                'name' => 'uuuuuuuuu',
                'age'  => 456,
            ]
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

    protected function setUp() : void {
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
                    new ObjectNormalizer(propertyTypeExtractor: new ReflectionExtractor(),),
                ]
            )
        );
    }
}

class Table1Dto
{
    public int $id;
    public string $name;
    public ?int $age;
}
