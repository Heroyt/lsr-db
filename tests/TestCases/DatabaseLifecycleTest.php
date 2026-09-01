<?php

declare(strict_types=1);

namespace TestCases;

use Dibi\DriverException;
use Lsr\Caching\Cache;
use Lsr\Db\Connection;
use Lsr\Db\Lifecycle\DatabaseLifecycleEvent;
use Lsr\Serializer\Mapper;
use Nette\Caching\Storages\DevNullStorage;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use Symfony\Component\Serializer\Serializer;

final class DatabaseLifecycleTest extends TestCase
{
    /** @var list<string> */
    private array $databaseFiles = [];

    protected function tearDown(): void {
        foreach ($this->databaseFiles as $databaseFile) {
            if (is_file($databaseFile)) {
                unlink($databaseFile);
            }
        }
    }

    public function testReportsSafeDatabaseOperations(): void {
        $hook = new RecordingDatabaseLifecycleHook();
        $connection = $this->connection('reporting')->setLifecycleHook($hook);

        $connection->query('CREATE TABLE private_table (id INTEGER PRIMARY KEY, secret TEXT)');
        $connection->query('INSERT INTO private_table (secret) VALUES (%s)', 'private-value');
        $result = $connection->query('SELECT secret FROM private_table');

        self::assertSame('private-value', $result->fetchSingle());
        self::assertSame([
            DatabaseLifecycleEvent::CONNECT,
            DatabaseLifecycleEvent::QUERY,
            DatabaseLifecycleEvent::INSERT,
            DatabaseLifecycleEvent::SELECT,
        ], array_map(static fn(DatabaseLifecycleEvent $event): string => $event->operation, $hook->events));

        foreach ($hook->events as $event) {
            self::assertSame(DatabaseLifecycleEvent::SUCCESS, $event->outcome);
            self::assertSame('sqlite', $event->system);
            self::assertSame('reporting', $event->connectionName);
            self::assertGreaterThanOrEqual(0.0, $event->durationSeconds);
            self::assertNull($event->errorType);
            self::assertNull($event->sql);
        }
    }

    public function testRawSqlCaptureIsExplicitlyOptIn(): void {
        $hook = new RecordingDatabaseLifecycleHook();
        $connection = $this->connection()->setLifecycleHook($hook, includeRawSql: true);

        self::assertSame('private-value', $connection->query('SELECT %s', 'private-value')->fetchSingle());
        self::assertCount(2, $hook->events);
        self::assertNull($hook->events[0]->sql);
        self::assertNotNull($hook->events[1]->sql);
        self::assertStringContainsString('private-value', $hook->events[1]->sql);
    }

    public function testDerivesPdoDatabaseSystemFromDsn(): void {
        $hook = new RecordingDatabaseLifecycleHook();
        $connection = $this->connection(config: [
            'driver' => 'pdo',
            'dsn' => 'sqlite::memory:',
            'lazy' => true,
        ])->setLifecycleHook($hook);

        self::assertSame(1, $connection->query('SELECT 1')->fetchSingle());
        foreach ($hook->events as $event) {
            self::assertSame('sqlite', $event->system);
        }
    }

    public function testHookCanBeAttachedAfterConnectionInitialization(): void {
        $connection = $this->connection();
        self::assertSame(1, $connection->query('SELECT 1')->fetchSingle());
        $hook = new RecordingDatabaseLifecycleHook();

        $connection->setLifecycleHook($hook);

        self::assertSame(2, $connection->query('SELECT 2')->fetchSingle());
        self::assertCount(1, $hook->events);
        self::assertSame(DatabaseLifecycleEvent::SELECT, $hook->events[0]->operation);
    }

    public function testReportsErrorsWithoutChangingTheThrownException(): void {
        $hook = new RecordingDatabaseLifecycleHook();
        $connection = $this->connection()->setLifecycleHook($hook);

        try {
            $connection->query('SELECT * FROM table_that_does_not_exist');
            self::fail('Expected the database query to fail.');
        } catch (DriverException $exception) {
            self::assertSame($exception::class, $hook->events[1]->errorType);
            self::assertSame(DatabaseLifecycleEvent::ERROR, $hook->events[1]->outcome);
            self::assertNull($hook->events[1]->sql);
        }
    }

    public function testHookFailureDoesNotAffectDatabaseResult(): void {
        $hook = new RecordingDatabaseLifecycleHook();
        $hook->fail = true;
        $connection = $this->connection()->setLifecycleHook($hook);

        self::assertSame(1, $connection->query('SELECT 1')->fetchSingle());
    }

    public function testHookDoesNotChangeSerializedState(): void {
        /** @var Connection $connection */
        $connection = (new ReflectionClass(Connection::class))->newInstanceWithoutConstructor();
        $serialized = serialize($connection);

        $connection->setLifecycleHook(new RecordingDatabaseLifecycleHook(), includeRawSql: true);

        self::assertSame($serialized, serialize($connection));
    }

    /**
     * @param array<string, mixed>|null $config
     */
    private function connection(?string $name = 'main', ?array $config = null): Connection {
        if ($config === null) {
            $databaseFile = tempnam(dirname(__DIR__) . '/tmp', 'lsr-db-lifecycle-');
            self::assertNotFalse($databaseFile);
            $this->databaseFiles[] = $databaseFile;
            $config = [
                'driver' => 'sqlite',
                'database' => $databaseFile,
            ];
        }
        return new Connection(
            new Cache(new DevNullStorage()),
            new Mapper(new Serializer([])),
            $config,
            $name,
        );
    }
}
