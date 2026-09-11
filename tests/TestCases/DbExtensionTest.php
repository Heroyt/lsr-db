<?php

declare(strict_types=1);

namespace TestCases;

use Dibi\DriverException;
use Lsr\Caching\Cache;
use Lsr\Db\Connection;
use Lsr\Db\DB;
use Lsr\Db\DI\DbExtension;
use Lsr\Logging\Logger;
use Lsr\Serializer\Mapper;
use Nette\Caching\Storage;
use Nette\Caching\Storages\DevNullStorage;
use Nette\Caching\Storages\MemoryStorage;
use Nette\DI\Compiler;
use Nette\DI\Container;
use Nette\DI\ContainerLoader;
use Nette\DI\Definitions\Reference;
use Nette\DI\Extensions\DIExtension;
use Nette\DI\InvalidConfigurationException;
use Nette\Utils\FileSystem;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Symfony\Component\Serializer\Normalizer\DenormalizerInterface;
use Symfony\Component\Serializer\Serializer;

final class DbExtensionTest extends TestCase
{
    private string $directory;

    protected function setUp(): void {
        DB::resetConnections();
        $this->directory = TMP_DIR . 'di-' . bin2hex(random_bytes(6));
        FileSystem::createDir($this->directory);
    }

    protected function tearDown(): void {
        DB::close();
        DB::resetConnections();
        FileSystem::delete($this->directory);
    }

    public function test_connection_registry_remains_uninitialized_until_main_service_is_resolved(): void {
        $this->createContainer([
            'main' => $this->sqliteConfig('main'),
        ]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Database is not initialized');
        DB::getConnection();
    }

    public function test_resolving_main_service_registers_main_and_named_connections(): void {
        $container = $this->createContainer([
            'main' => [
                'driver' => 'pdo',
                'lazy' => true,
                'host' => 'db',
                'port' => 3306,
                'charset' => 'utf8mb4',
                'database' => 'app',
                'user' => 'root',
                'password' => 'secret',
            ],
            'reporting' => $this->sqliteConfig('reporting'),
        ]);
        self::assertSame(
            ['db.connection', 'db.connection.reporting'],
            $container->findByType(Connection::class),
        );


        $main = $container->getService('db.connection');
        $reporting = $container->getService('db.connection.reporting');

        self::assertInstanceOf(Connection::class, $main);
        self::assertInstanceOf(Connection::class, $reporting);
        self::assertSame($main, DB::getConnection());
        self::assertSame($main, DB::getConnection('main'));
        self::assertSame($reporting, DB::getConnection('reporting'));
        self::assertSame($main, $container->getByType(Connection::class));
    }

    public function test_main_connection_is_required(): void {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The main database connection must be configured.');

        $this->createContainer([
            'reporting' => $this->sqliteConfig('reporting'),
        ]);
    }

    public function test_connection_overrides_share_or_isolate_loggers_without_changing_root_selection(): void {
        $container = $this->createContainer([
            'main' => $this->sqliteConfig('main') + ['logger' => '@test.separateLogger'],
            'reporting' => $this->sqliteConfig('reporting'),
            'archive' => $this->sqliteConfig('archive') + ['logger' => new Reference('test.sharedLogger')],
        ], '@test.sharedLogger');
        $shared = $container->getService('test.sharedLogger');
        $separate = $container->getService('test.separateLogger');
        $application = $container->getService('logger');
        self::assertInstanceOf(RecordingPsrLogger::class, $shared);
        self::assertInstanceOf(RecordingPsrLogger::class, $separate);
        self::assertInstanceOf(RecordingPsrLogger::class, $application);
        self::assertSame($application, $container->getByType(LoggerInterface::class));
        self::assertSame($shared, $container->getService('db.logger'));
        self::assertSame($separate, $container->getService('db.logger.main'));
        self::assertSame($shared, $container->getService('db.logger.reporting'));
        self::assertSame($shared, $container->getService('db.logger.archive'));

        foreach (['main', 'reporting', 'archive'] as $name) {
            $service = $name === 'main' ? 'db.connection' : 'db.connection.' . $name;
            $connection = $container->getService($service);
            self::assertInstanceOf(Connection::class, $connection);
            self::assertSame(42, $connection->query('SELECT 42')->fetchSingle());
            $this->queryMissingTable($connection, $name);
        }

        self::assertSame(['error', 'debug', 'error', 'debug'], array_column($shared->records, 'level'));
        self::assertSame('SQL: SELECT * FROM missing_reporting', $shared->records[1]['message']);
        self::assertSame('SQL: SELECT * FROM missing_archive', $shared->records[3]['message']);
        self::assertSame(['error', 'debug'], array_column($separate->records, 'level'));
        self::assertSame('SQL: SELECT * FROM missing_main', $separate->records[1]['message']);
        self::assertSame([], $application->records);
    }

    public function test_named_override_leaves_unconfigured_loggers_on_the_dedicated_default(): void {
        $container = $this->createContainer([
            'main' => $this->sqliteConfig('main'),
            'reporting' => $this->sqliteConfig('reporting') + ['logger' => '@test.separateLogger'],
            'archive' => $this->sqliteConfig('archive'),
        ]);
        $default = $container->getService('db.logger');
        $archiveLogger = $container->getService('db.logger.archive');
        self::assertInstanceOf(Logger::class, $default);
        self::assertInstanceOf(Logger::class, $archiveLogger);
        self::assertSame($default, $container->getService('db.logger.main'));
        self::assertNotSame($default, $archiveLogger);

        $file = LOG_DIR . 'db-' . date('Y-m-d') . '.log';
        $before = is_file($file) ? FileSystem::read($file) : '';
        $main = $container->getService('db.connection');
        $archive = $container->getService('db.connection.archive');
        self::assertInstanceOf(Connection::class, $main);
        self::assertInstanceOf(Connection::class, $archive);
        $this->queryMissingTable($main, 'default_main');
        $this->queryMissingTable($archive, 'default_archive');
        $written = substr(FileSystem::read($file), strlen($before));
        self::assertStringContainsString('DEBUG: SQL: SELECT * FROM missing_default_main', $written);
        self::assertStringContainsString('DEBUG: SQL: SELECT * FROM missing_default_archive', $written);

        $application = $container->getService('logger');
        $separate = $container->getService('test.separateLogger');
        self::assertInstanceOf(RecordingPsrLogger::class, $application);
        self::assertInstanceOf(RecordingPsrLogger::class, $separate);
        self::assertSame([], $application->records);
        self::assertSame([], $separate->records);
    }

    public function test_changing_logger_selection_does_not_change_the_connection_cache_namespace(): void {
        $cache = new Cache(new MemoryStorage());
        $connections = [
            'main' => $this->sqliteConfig('main'),
            'reporting' => $this->sqliteConfig('reporting') + ['logger' => '@test.sharedLogger'],
        ];
        $firstContainer = $this->createContainer($connections);
        $connections['reporting']['logger'] = '@test.separateLogger';
        $secondContainer = $this->createContainer($connections);
        $firstContainer->addService('test.cache', $cache);
        $secondContainer->addService('test.cache', $cache);
        $first = $firstContainer->getService('db.connection.reporting');
        $second = $secondContainer->getService('db.connection.reporting');
        self::assertInstanceOf(Connection::class, $first);
        self::assertInstanceOf(Connection::class, $second);

        try {
            $first->query('CREATE TABLE cache_values (value TEXT)');
            $first->query("INSERT INTO cache_values VALUES ('cached')");
            self::assertSame('cached', $first->select('cache_values', 'value')->fetchSingle());
            $first->query("UPDATE cache_values SET value = 'fresh'");
            self::assertSame('cached', $second->select('cache_values', 'value')->fetchSingle());
            self::assertSame('fresh', $second->select('cache_values', 'value')->fetchSingle(cache: false));
        } finally {
            $first->disconnect();
            $second->disconnect();
        }
    }

    private function queryMissingTable(Connection $connection, string $name): void {
        try {
            $connection->query('SELECT * FROM missing_' . $name);
            self::fail('The missing table query must fail.');
        } catch (DriverException) {
        }
    }

    /**
     * @param array<string, array<string, mixed>> $connections
     */
    private function createContainer(array $connections, string|Reference|null $logger = null): Container {
        $loader = new ContainerLoader($this->directory, true);
        /** @var class-string<Container> $containerClass */
        $containerClass = $loader->load(
            function (Compiler $compiler) use ($connections, $logger): ?string {
                $builder = $compiler->getContainerBuilder();
                $builder->addDefinition('test.cacheStorage')
                    ->setType(Storage::class)
                    ->setFactory(DevNullStorage::class);
                $builder->addDefinition('test.cache')
                    ->setType(Cache::class)
                    ->setFactory(Cache::class, ['@test.cacheStorage']);
                $builder->addDefinition('test.serializer')
                    ->setType(DenormalizerInterface::class)
                    ->setFactory(Serializer::class);
                $builder->addDefinition('test.mapper')
                    ->setType(Mapper::class)
                    ->setFactory(Mapper::class, ['@test.serializer']);
                $builder->addDefinition('logger')
                    ->setFactory(RecordingPsrLogger::class);
                $builder->addDefinition('test.sharedLogger')
                    ->setFactory(RecordingPsrLogger::class)
                    ->setAutowired(false);
                $builder->addDefinition('test.separateLogger')
                    ->setFactory(RecordingPsrLogger::class)
                    ->setAutowired(false);

                $compiler->addExtension('di', new DIExtension());
                $compiler->addExtension('db', new DbExtension());
                $compiler->addConfig([
                    'di' => [
                        'lazy' => true,
                    ],
                    'db' => [
                        'logger' => $logger,
                        'connections' => $connections,
                    ],
                ]);
                return null;
            },
            hash('sha256', serialize([$connections, $logger])),
        );

        return new $containerClass();
    }

    /**
     * @return array{driver: string, database: string, lazy: bool}
     */
    private function sqliteConfig(string $name): array {
        return [
            'driver' => 'sqlite',
            'database' => $this->directory . '/' . $name . '.db',
            'lazy' => true,
        ];
    }
}
