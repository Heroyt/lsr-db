<?php

declare(strict_types=1);

namespace TestCases;

use Dibi\DriverException;
use Dibi\Fluent;
use Lsr\Caching\Cache;
use Lsr\Db\Connection;
use Lsr\Serializer\Mapper;
use mysqli;
use Nette\Caching\Storages\DevNullStorage;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\Serializer\Serializer;
use Throwable;

/**
 * Integration tests require a disposable MySQL/MariaDB instance with database
 * reconnect_test and root/empty password, listening on 127.0.0.1.
 *
 * LSR_DB_TEST_PORT=13376 vendor/bin/phpunit --no-coverage
 *
 * Sessions are deliberately killed; never point this at an application database.
 */
final class ConnectionReconnectTest extends TestCase
{
    private ?mysqli $admin = null;
    private int $port;
    private string $table;
    /** @var list<Connection> */
    private array $connections = [];

    protected function setUp(): void {
        $port = getenv('LSR_DB_TEST_PORT');
        if ($port === false || $port === '') {
            self::markTestSkipped('Set LSR_DB_TEST_PORT to an isolated MySQL integration server.');
        }
        if ( ! extension_loaded('mysqli')) {
            self::markTestSkipped('The mysqli extension is required to kill the tested MySQL sessions.');
        }
        $port = filter_var($port, FILTER_VALIDATE_INT, ['options' => ['min_range' => 1, 'max_range' => 65535]]);
        self::assertNotFalse($port, 'LSR_DB_TEST_PORT must be a valid TCP port.');
        $this->port = $port;
        $this->table = 'lsr_reconnect_' . bin2hex(random_bytes(8));
        $this->admin = new mysqli('127.0.0.1', 'root', '', 'reconnect_test', $this->port);
        self::assertTrue($this->admin->query(
            'CREATE TABLE `' . $this->table . '` ('
            . 'id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, '
            . 'value VARCHAR(100) NOT NULL UNIQUE'
            . ') ENGINE=InnoDB',
        ));
    }

    protected function tearDown(): void {
        try {
            foreach ($this->connections as $connection) {
                $connection->close();
            }
        } finally {
            if ($this->admin !== null) {
                try {
                    $this->admin->query('DROP TABLE IF EXISTS `' . $this->table . '`');
                } finally {
                    $this->admin->close();
                }
            }
        }
    }

    /** @return iterable<string, array{string}> */
    public static function drivers(): iterable {
        yield 'mysqli' => ['mysqli'];
        yield 'pdo_mysql' => ['pdo_mysql'];
    }

    /** @return iterable<string, array{string, bool|null}> */
    public static function disabledConnections(): iterable {
        foreach (self::drivers() as $name => [$driver]) {
            yield $name . ' default' => [$driver, null];
            yield $name . ' explicit opt-out' => [$driver, false];
        }
    }

    /** @return iterable<string, array{string, bool}> */
    public static function transactionDepths(): iterable {
        foreach (self::drivers() as $name => [$driver]) {
            yield $name . ' outer transaction' => [$driver, false];
            yield $name . ' nested transaction' => [$driver, true];
        }
    }

    #[DataProvider('drivers')]
    public function test_raw_query_reconnects_before_application_sql(string $driver): void {
        $connection = $this->connection($driver);
        $connection->insert($this->table, ['value' => 'persisted']);
        $session = $this->sessionId($connection);
        $this->kill($session);

        self::assertSame('persisted', $connection->query('SELECT value FROM %n', $this->table)->fetchSingle());
        self::assertNotSame($session, $this->sessionId($connection));
        self::assertSame(['persisted'], $this->values());
    }

    #[DataProvider('drivers')]
    public function test_fluent_built_before_disconnect_can_fetch_and_execute(string $driver): void {
        $connection = $this->connection($driver);
        $connection->insert($this->table, ['value' => 'existing']);
        $select = $connection->select($this->table, 'value')->where('value = %s', 'existing');
        $insert = $connection->insertGet($this->table, ['value' => 'fluent-insert']);
        $firstSession = $this->sessionId($connection);
        $this->kill($firstSession);

        self::assertSame('existing', $select->fetchSingleValue());
        $secondSession = $this->sessionId($connection);
        self::assertNotSame($firstSession, $secondSession);
        $this->kill($secondSession);

        self::assertSame(1, $insert->execute(Fluent::AffectedRows));
        self::assertNotSame($secondSession, $this->sessionId($connection));
        self::assertSame(['existing', 'fluent-insert'], $this->values());
    }

    #[DataProvider('drivers')]
    public function test_count_built_before_disconnect_reconnects_with_and_without_cache(string $driver): void {
        $connection = $this->connection($driver);
        $connection->insert($this->table, ['value' => 'first'], ['value' => 'second']);
        $query = $connection->select($this->table);
        $session = $this->sessionId($connection);
        $this->kill($session);

        self::assertSame(2, $query->count());
        $nextSession = $this->sessionId($connection);
        self::assertNotSame($session, $nextSession);
        $this->kill($nextSession);

        self::assertSame(2, $query->count(false));
        self::assertNotSame($nextSession, $this->sessionId($connection));
    }

    #[DataProvider('drivers')]
    public function test_cached_fluent_does_not_replay_sql_interrupted_during_execution(string $driver): void {
        if ( ! function_exists('proc_open')) {
            self::markTestSkipped('proc_open is required to kill a query while it is executing.');
        }
        $connection = $this->connection($driver);
        $session = $this->sessionId($connection);
        $query = $connection->select(null, 'SLEEP(3)');
        $killer = <<<'PHP'
            $admin = new mysqli('127.0.0.1', 'root', '', 'reconnect_test', (int) $argv[1]);
            $session = (int) $argv[2];
            echo "ready\n";
            flush();
            $deadline = microtime(true) + 5;
            do {
                $sql = $admin->query(
                    'SELECT INFO FROM information_schema.PROCESSLIST WHERE ID = ' . $session
                )->fetch_column();
                if (is_string($sql) && str_contains($sql, 'SLEEP(3)')) {
                    $admin->query('KILL CONNECTION ' . $session);
                    echo "killed\n";
                    $admin->close();
                    exit(0);
                }
                usleep(10000);
            } while (microtime(true) < $deadline);
            fwrite(STDERR, "The application query was not observed before the deadline.\n");
            exit(1);
            PHP;
        $process = proc_open(
            [PHP_BINARY, '-r', $killer, (string) $this->port, (string) $session],
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        self::assertIsResource($process);
        fclose($pipes[0]);
        $error = null;
        try {
            self::assertSame("ready\n", fgets($pipes[1]));
            try {
                $query->fetchSingle();
            } catch (DriverException $exception) {
                $error = $exception;
            }
            $output = stream_get_contents($pipes[1]);
            $stderr = stream_get_contents($pipes[2]);
        } finally {
            fclose($pipes[1]);
            fclose($pipes[2]);
            $exitCode = proc_close($process);
        }

        self::assertSame(0, $exitCode, $stderr);
        self::assertSame("killed\n", $output);
        self::assertInstanceOf(DriverException::class, $error, 'The interrupted SQL must fail, not be replayed.');
        self::assertContains($error->getCode(), [2006, 2013, 2055]);
        self::assertNotSame($session, $this->sessionId($connection));
    }

    #[DataProvider('drivers')]
    public function test_write_helper_reconnects_and_preserves_affected_rows_and_insert_id(string $driver): void {
        $connection = $this->connection($driver);
        $session = $this->sessionId($connection);
        $this->kill($session);

        self::assertSame(1, $connection->insert($this->table, ['value' => 'exactly-once']));
        self::assertSame(1, $connection->getAffectedRows());
        $insertId = $connection->getInsertId();
        self::assertSame(1, $insertId);
        self::assertSame(1, $connection->getAffectedRows());
        self::assertSame($insertId, (int) $this->admin->query(
            'SELECT id FROM `' . $this->table . '` WHERE value = \'exactly-once\'',
        )->fetch_column());
        self::assertSame(['exactly-once'], $this->values());
        self::assertNotSame($session, $this->sessionId($connection));
    }

    #[DataProvider('disabledConnections')]
    public function test_disabled_reconnect_preserves_gone_away_error(string $driver, ?bool $autoReconnect): void {
        $connection = $this->connection($driver, $autoReconnect);
        $this->kill($this->sessionId($connection));

        try {
            $connection->insert($this->table, ['value' => 'must-not-write']);
            self::fail('A disconnected opt-out connection must fail.');
        } catch (DriverException $exception) {
            self::assertSame(2006, $exception->getCode());
        }
        self::assertSame([], $this->values());
    }

    #[DataProvider('transactionDepths')]
    public function test_disconnect_does_not_resume_managed_transaction(string $driver, bool $nested): void {
        $connection = $this->connection($driver);
        $disconnectAndWrite = function (Connection $connection): bool {
            $connection->insert($this->table, ['value' => 'before-disconnect']);
            $this->kill($this->sessionId($connection));
            $connection->insert($this->table, ['value' => 'must-not-resume']);
            return true;
        };

        try {
            $connection->transaction(function (Connection $connection) use ($nested, $disconnectAndWrite): bool {
                if ($nested) {
                    $connection->insert($this->table, ['value' => 'outer-uncommitted']);
                    $connection->transaction($disconnectAndWrite);
                    return true;
                }
                return $disconnectAndWrite($connection);
            });
            self::fail('Connection loss inside a transaction must escape the callback.');
        } catch (DriverException $exception) {
            self::assertContains($exception->getCode(), [2006, 2013, 2055]);
        }

        self::assertSame([], $this->values());
        self::assertSame(1, $connection->insert($this->table, ['value' => 'recovered-after-unwind']));
        self::assertSame(['recovered-after-unwind'], $this->values());
    }

    #[DataProvider('drivers')]
    public function test_caught_nested_failure_does_not_allow_outer_transaction_to_reconnect(string $driver): void {
        $connection = $this->connection($driver);
        $nestedFailed = false;
        try {
            $connection->transaction(function (Connection $connection) use (&$nestedFailed): bool {
                $connection->insert($this->table, ['value' => 'outer-uncommitted']);
                try {
                    $connection->transaction(function (Connection $connection): bool {
                        $this->kill($this->sessionId($connection));
                        $connection->insert($this->table, ['value' => 'nested-must-not-resume']);
                        return true;
                    });
                } catch (DriverException $exception) {
                    self::assertContains($exception->getCode(), [2006, 2013, 2055]);
                    $nestedFailed = true;
                }
                $connection->insert($this->table, ['value' => 'outer-must-not-resume']);
                return true;
            });
            self::fail('The outer transaction must remain unusable after its session is lost.');
        } catch (DriverException $exception) {
            self::assertContains($exception->getCode(), [2006, 2013, 2055]);
        }
        self::assertTrue($nestedFailed);
        self::assertSame([], $this->values());
    }

    #[DataProvider('transactionDepths')]
    public function test_callback_exception_survives_rollback_failure_and_stack_unwinds(string $driver, bool $nested): void {
        $connection = $this->connection($driver);
        $original = new RuntimeException('Application callback failed.');
        $fail = function (Connection $connection) use ($original): bool {
            $connection->insert($this->table, ['value' => 'uncommitted']);
            $this->kill($this->sessionId($connection));
            throw $original;
        };

        try {
            $connection->transaction(static function (Connection $connection) use ($nested, $fail): bool {
                if ($nested) {
                    $connection->transaction($fail);
                    return true;
                }
                return $fail($connection);
            });
            self::fail('The callback exception must escape the transaction.');
        } catch (Throwable $exception) {
            self::assertSame($original, $exception);
        }

        self::assertSame([], $this->values());
        self::assertSame(1, $connection->insert($this->table, ['value' => 'recovered']));
        $connection->transaction(function (Connection $connection): bool {
            $connection->insert($this->table, ['value' => 'rolled-back-after-recovery']);
            return false;
        });
        self::assertSame(['recovered'], $this->values());
    }

    #[DataProvider('drivers')]
    public function test_failed_rollback_preserves_nested_savepoints_and_ancestor_rollback_unwinds_them(string $driver): void {
        $connection = $this->connection($driver);
        $connection->begin();
        $connection->insert($this->table, ['value' => 'outer']);
        $connection->begin('ancestor');
        $connection->insert($this->table, ['value' => 'ancestor']);
        $connection->begin('inner');
        $connection->insert($this->table, ['value' => 'inner']);

        try {
            $connection->rollback('missing');
            self::fail('A missing savepoint must fail.');
        } catch (DriverException $exception) {
            self::assertSame(1305, $exception->getCode());
        }
        $connection->rollback();
        self::assertSame(
            ['outer', 'ancestor'],
            array_column($connection->query('SELECT value FROM %n ORDER BY id', $this->table)->fetchAll(), 'value'),
        );
        $connection->begin('new-inner');
        $connection->rollback('ancestor');
        $connection->rollback();

        self::assertSame([], $this->values());
        self::assertSame(1, $connection->insert($this->table, ['value' => 'after-rollback']));
        self::assertSame(['after-rollback'], $this->values());
    }

    #[DataProvider('drivers')]
    public function test_close_clears_lost_nested_transaction_and_allows_fresh_work(string $driver): void {
        $connection = $this->connection($driver);
        $connection->begin();
        $connection->insert($this->table, ['value' => 'uncommitted']);
        $connection->begin();
        $this->kill($this->sessionId($connection));
        $connection->close();

        $connection->transaction(function (Connection $connection): bool {
            $connection->insert($this->table, ['value' => 'fresh-but-rolled-back']);
            return false;
        });
        self::assertSame([], $this->values());
        self::assertSame(1, $connection->insert($this->table, ['value' => 'fresh-committed']));
        self::assertSame(['fresh-committed'], $this->values());
    }

    #[DataProvider('drivers')]
    public function test_sql_errors_do_not_replace_session_or_replay_writes(string $driver): void {
        $connection = $this->connection($driver);
        $connection->insert($this->table, ['value' => 'unique-value']);
        $session = $this->sessionId($connection);

        try {
            $connection->query('SELEC value FROM %n', $this->table);
            self::fail('Malformed SQL must fail.');
        } catch (DriverException $exception) {
            self::assertSame(1064, $exception->getCode());
        }
        self::assertSame($session, $this->sessionId($connection));

        try {
            $connection->insert($this->table, ['value' => 'unique-value']);
            self::fail('The unique constraint must reject the duplicate.');
        } catch (DriverException $exception) {
            self::assertSame(1062, $exception->getCode());
        }
        self::assertSame($session, $this->sessionId($connection));
        self::assertSame(['unique-value'], $this->values());
        self::assertSame(1, $connection->insert($this->table, ['value' => 'after-errors']));
        self::assertSame(['unique-value', 'after-errors'], $this->values());
    }

    private function connection(string $driver, ?bool $autoReconnect = true): Connection {
        if ($driver === 'pdo_mysql' && ! extension_loaded('pdo_mysql')) {
            self::markTestSkipped('The pdo_mysql extension is required for this driver.');
        }
        $config = [
            'driver' => $driver === 'pdo_mysql' ? 'pdo' : 'mysqli',
            'host' => '127.0.0.1',
            'port' => $this->port,
            'username' => 'root',
            'password' => '',
            'database' => 'reconnect_test',
        ];
        if ($driver === 'pdo_mysql') {
            $config['dsn'] = 'mysql:host=127.0.0.1;port=' . $this->port . ';dbname=reconnect_test';
        }
        if ($autoReconnect !== null) {
            $config['autoReconnect'] = $autoReconnect;
        }
        $connection = new Connection(
            new Cache(new DevNullStorage()),
            new Mapper(new Serializer([])),
            $config,
        );
        $this->connections[] = $connection;
        return $connection;
    }

    private function sessionId(Connection $connection): int {
        return (int) $connection->query('SELECT CONNECTION_ID()')->fetchSingle();
    }

    private function kill(int $session): void {
        self::assertTrue($this->admin->query('KILL CONNECTION ' . $session));
    }

    /** @return list<string> */
    private function values(): array {
        return array_column(
            $this->admin->query('SELECT value FROM `' . $this->table . '` ORDER BY id')->fetch_all(MYSQLI_ASSOC),
            'value',
        );
    }
}
