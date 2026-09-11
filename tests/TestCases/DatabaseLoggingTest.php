<?php

declare(strict_types=1);

namespace TestCases;

use Dibi\DriverException;
use Dibi\Event;
use Lsr\Caching\Cache;
use Lsr\Db\Connection;
use Lsr\Db\DB;
use Lsr\Db\Logging\DibiEventLogger;
use Lsr\Logging\Logger;
use Lsr\Serializer\Mapper;
use Nette\Caching\Storages\DevNullStorage;
use Nette\Utils\FileSystem;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\Serializer\Serializer;

final class DatabaseLoggingTest extends TestCase
{
    /** @return iterable<string, array{string}> */
    public static function entrypoints(): iterable {
        yield 'constructor' => ['constructor'];
        yield 'factory' => ['factory'];
        yield 'main factory' => ['main'];
    }

    #[DataProvider('entrypoints')]
    public function test_sqlite_failures_reach_plain_psr_logger_without_logging_successes(string $entrypoint): void {
        $cache = new Cache(new DevNullStorage());
        $mapper = new Mapper(new Serializer([]));
        $config = ['driver' => 'pdo', 'dsn' => 'sqlite::memory:'];
        $logger = new RecordingPsrLogger();
        $connection = match ($entrypoint) {
            'constructor' => new Connection($cache, $mapper, $config, 'standalone', $logger),
            'factory' => DB::createConnection($cache, $mapper, $config, 'standalone', $logger),
            'main' => DB::getMain($cache, $mapper, $config, $logger),
        };

        try {
            self::assertSame(42, $connection->query('SELECT 42')->fetchSingle());
            self::assertSame([], $logger->records);

            $failure = $this->queryMissingTable($connection);
            self::assertSame($this->expectedRecords($failure), $logger->records);
        } finally {
            $connection->disconnect();
        }
    }

    public function test_default_file_output_matches_legacy_helper_and_ignores_successes(): void {
        $directory = TMP_DIR . 'legacy-log-' . bin2hex(random_bytes(6));
        $file = LOG_DIR . 'db-' . date('Y-m-d') . '.log';
        $before = is_file($file) ? FileSystem::read($file) : '';
        $connection = DB::getMain(
            new Cache(new DevNullStorage()),
            new Mapper(new Serializer([])),
            ['driver' => 'pdo', 'dsn' => 'sqlite::memory:'],
        );

        try {
            $connection->query('SELECT 42');
            self::assertSame($before, is_file($file) ? FileSystem::read($file) : '');

            $failure = $this->queryMissingTable($connection);
            $legacy = new Logger($directory, 'reference');
            $legacy->logDb(new Event($connection->connection, Event::QUERY)->done($failure));
            $expected = FileSystem::read($directory . '/reference-' . date('Y-m-d') . '.log');
            $actual = substr(FileSystem::read($file), strlen($before));
            $timestamp = '/^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]/m';
            self::assertSame(
                preg_replace($timestamp, '[timestamp]', $expected),
                preg_replace($timestamp, '[timestamp]', $actual),
            );
        } finally {
            $connection->disconnect();
            FileSystem::delete($directory);
        }
    }

    /** @return iterable<string, array{int|string, ?string, list<array{level: string, message: string, context: array{}}>}> */
    public static function failureEvents(): iterable {
        yield 'zero code without SQL' => [0, null, [
            ['level' => 'error', 'message' => 'Failed query', 'context' => []],
        ]];
        yield 'SQLSTATE with SQL' => ['42000', 'SELECT missing', [
            ['level' => 'error', 'message' => '(42000) Failed query', 'context' => []],
            ['level' => 'debug', 'message' => 'SQL: SELECT missing', 'context' => []],
        ]];
        yield 'legacy empty SQL string' => [12, '', [
            ['level' => 'error', 'message' => '(12) Failed query', 'context' => []],
        ]];
        yield 'legacy zero-like SQL and code' => ['0', '0', [
            ['level' => 'error', 'message' => 'Failed query', 'context' => []],
        ]];
    }

    /** @param list<array{level: string, message: string, context: array{}}> $expected */
    #[DataProvider('failureEvents')]
    public function test_adapter_preserves_error_and_sql_record_boundaries(int|string $code, ?string $sql, array $expected): void {
        $logger = new RecordingPsrLogger();
        $adapter = new DibiEventLogger($logger);
        $connection = new \Dibi\Connection(['driver' => 'pdo', 'dsn' => 'sqlite::memory:', 'lazy' => true]);
        $event = new Event($connection, Event::QUERY, 'Event SQL must not replace exception SQL');
        $adapter($event->done());
        self::assertSame([], $logger->records);

        $adapter($event->done(new DriverException('Failed query', $code, $sql)));
        self::assertSame($expected, $logger->records);
    }

    public function test_logger_failure_propagates_synchronously_without_attempting_debug(): void {
        $logger = new RecordingPsrLogger();
        $logger->failure = new RuntimeException('Log storage is unavailable');
        $connection = DB::createConnection(
            new Cache(new DevNullStorage()),
            new Mapper(new Serializer([])),
            ['driver' => 'pdo', 'dsn' => 'sqlite::memory:'],
            logger: $logger,
        );

        try {
            $connection->query('SELECT * FROM missing_logging_table');
            self::fail('The logger failure must escape the query.');
        } catch (RuntimeException $failure) {
            self::assertSame($logger->failure, $failure);
            self::assertSame(['error'], array_column($logger->records, 'level'));
        } finally {
            $connection->disconnect();
        }
    }

    private function queryMissingTable(Connection $connection): DriverException {
        try {
            $connection->query('SELECT * FROM missing_logging_table');
            self::fail('The missing table query must fail.');
        } catch (DriverException $failure) {
            return $failure;
        }
    }

    /** @return list<array{level: string, message: string, context: array{}}> */
    private function expectedRecords(DriverException $failure): array {
        $message = $failure->getCode() ? '(' . $failure->getCode() . ') ' : '';
        return [
            ['level' => 'error', 'message' => $message . $failure->getMessage(), 'context' => []],
            ['level' => 'debug', 'message' => 'SQL: ' . $failure->getSql(), 'context' => []],
        ];
    }
}
