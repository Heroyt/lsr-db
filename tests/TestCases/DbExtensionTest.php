<?php

declare(strict_types=1);

namespace TestCases;

use Lsr\Caching\Cache;
use Lsr\Db\Connection;
use Lsr\Db\DB;
use Lsr\Db\DI\DbExtension;
use Lsr\Serializer\Mapper;
use Nette\Caching\Storage;
use Nette\Caching\Storages\DevNullStorage;
use Nette\DI\Compiler;
use Nette\DI\Container;
use Nette\DI\ContainerLoader;
use Nette\DI\Extensions\DIExtension;
use Nette\DI\InvalidConfigurationException;
use Nette\Utils\FileSystem;
use PHPUnit\Framework\TestCase;
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

    /**
     * @param array<string, array<string, mixed>> $connections
     */
    private function createContainer(array $connections): Container {
        $loader = new ContainerLoader($this->directory, true);
        /** @var class-string<Container> $containerClass */
        $containerClass = $loader->load(
            function (Compiler $compiler) use ($connections): ?string {
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

                $compiler->addExtension('di', new DIExtension());
                $compiler->addExtension('db', new DbExtension());
                $compiler->addConfig([
                    'di' => [
                        'lazy' => true,
                    ],
                    'db' => [
                        'connections' => $connections,
                    ],
                ]);
                return null;
            },
            hash('sha256', serialize($connections)),
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
