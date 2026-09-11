<?php

declare(strict_types=1);

namespace Lsr\Db\DI;

use Lsr\Db\Connection;
use Lsr\Db\DB;
use Lsr\Logging\Logger;
use Nette\DI\CompilerExtension;
use Nette\DI\Definitions\Reference;
use Nette\PhpGenerator\Literal;
use Nette\Schema\Expect;
use Nette\Schema\Schema;
use Psr\Log\LoggerInterface;

/**
 * Registers the main connection as "<extension>.connection" and named
 * connections as "<extension>.connection.<name>". Resolving the main service
 * initializes the static DB facade; applications that do not register this
 * extension keep using the existing manual DB::init() lifecycle.
 *
 * @property-read object{
 *     logger: string|Reference|null,
 *     connections: array<string, object{
 *         logger: string|Reference|null,
 *         driver: string,
 *         dsn: string|null,
 *         pdoDriver: string|null,
 *         host: string|null,
 *         port: int|null,
 *         user: string|null,
 *         username: string|null,
 *         password: string|null,
 *         pass: string|null,
 *         database: string|null,
 *         collate: string|null,
 *         charset: string|null,
 *         prefix: string|null,
 *         lazy: bool,
 *         options: array<array-key, mixed>,
 *         strictSelectForUpdate: bool,
 *         autoReconnect: bool
 *     }>
 * } $config
 */
final class DbExtension extends CompilerExtension
{
    public function getConfigSchema(): Schema {
        $connection = Expect::structure([
            'logger' => Expect::anyOf(
                Expect::string()->pattern('@.+'),
                Expect::type(Reference::class),
            )->nullable()->default(null),
            'driver' => Expect::string()->default('mysqli')->assert(
                static fn (mixed $driver): bool => is_string($driver) && $driver !== '',
                'Database driver cannot be empty.',
            ),
            'dsn' => Expect::string()->nullable()->default(null),
            'pdoDriver' => Expect::string()->nullable()->default(null),
            'host' => Expect::string()->nullable()->default(null),
            'port' => Expect::int()->nullable()->default(null),
            'user' => Expect::string()->nullable()->default(null),
            'username' => Expect::string()->nullable()->default(null),
            'password' => Expect::string()->nullable()->default(null),
            'pass' => Expect::string()->nullable()->default(null),
            'database' => Expect::string()->nullable()->default(null),
            'collate' => Expect::string()->nullable()->default(null),
            'charset' => Expect::string()->nullable()->default(null),
            'prefix' => Expect::string()->nullable()->default(null),
            'lazy' => Expect::bool()->default(false),
            'options' => Expect::array()->default([]),
            'strictSelectForUpdate' => Expect::bool()->default(Connection::DEFAULT_STRICT_SELECT_FOR_UPDATE),
            'autoReconnect' => Expect::bool()->default(false),
        ]);

        return Expect::structure([
            'logger' => Expect::anyOf(
                Expect::string()->pattern('@.+'),
                Expect::type(Reference::class),
            )->nullable()->default(null),
            'connections' => Expect::arrayOf(
                $connection,
                Expect::string()->pattern('[A-Za-z0-9_]+'),
            )->required()->assert(
                static fn (mixed $connections): bool => is_array($connections)
                    && array_key_exists('main', $connections),
                'The main database connection must be configured.',
            ),
        ]);
    }

    public function loadConfiguration(): void {
        $builder = $this->getContainerBuilder();
        $definitions = [];
        $this->registerLogger($this->prefix('logger'), $this->config->logger);

        foreach ($this->config->connections as $name => $config) {
            $loggerName = $this->prefix('logger.' . $name);
            $logger = $config->logger;
            if ($logger === null && ($this->config->logger !== null || $name === 'main')) {
                $logger = new Reference($this->prefix('logger'));
            }
            $this->registerLogger($loggerName, $logger);
            $serviceName = $name === 'main'
                ? $this->prefix('connection')
                : $this->prefix('connection.' . $name);
            $definitions[$name] = $builder->addDefinition($serviceName)
                ->setType(Connection::class)
                ->setFactory(
                    [DB::class, 'createConnection'],
                    [
                        'config' => self::normalizeConfig($config),
                        'name' => $name,
                        'logger' => new Reference($loggerName),
                    ],
                )
                ->setAutowired($name === 'main')
                ->setTags([
                    'lsr' => true,
                    'db' => $name,
                ]);
        }

        $main = $definitions['main'];
        $main->addSetup(
            [DB::class, 'init'],
            [new Reference(Reference::Self)],
        );
        $main->lazy = false;
        foreach (array_keys($definitions) as $name) {
            if ($name === 'main') {
                continue;
            }
            $main->addSetup(
                [DB::class, 'initNamed'],
                [$name, new Reference($this->prefix('connection.' . $name))],
            );
        }
    }

    private function registerLogger(string $name, string|Reference|null $logger): void {
        $definition = $this->getContainerBuilder()->addDefinition($name)
            ->setAutowired(false);
        if ($logger !== null) {
            $definition->setType(LoggerInterface::class)
                ->setFactory(is_string($logger) ? new Reference(substr($logger, 1)) : $logger);
            return;
        }

        $definition->setFactory(Logger::class, [new Literal('LOG_DIR'), 'db']);
        $definition->lazy = true;
    }

    /**
     * @return array<string, mixed>
     */
    private static function normalizeConfig(object $config): array {
        $normalized = [];
        foreach ((array) $config as $name => $value) {
            if ($name !== 'logger' && $value !== null) {
                $normalized[(string) $name] = $value;
            }
        }
        return $normalized;
    }
}
