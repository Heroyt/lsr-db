<?php
declare(strict_types=1);

namespace TestCases;

use ErrorException;
use Lsr\Caching\Cache;
use Lsr\Db\Connection;
use Lsr\Serializer\Mapper;
use Nette\Caching\Storages\DevNullStorage;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Symfony\Component\Serializer\Serializer;

final class Php85CompatibilityTest extends TestCase
{
    public function testInspectingDibiClausesDoesNotTriggerDeprecation() : void {
        $cache = new Cache(new DevNullStorage());
        $mapper = new Mapper(new Serializer());
        $connection = new Connection(
            $cache,
            $mapper,
            [
                'driver'    => 'pdo',
                'pdoDriver' => 'sqlite',
                'dsn'       => 'sqlite::memory:',
                'lazy'      => true,
            ]
        );
        $fluent = $connection->select('table1')->limit(1);
        $method = new ReflectionMethod($fluent, 'shouldAddSingleRowLimit');

        set_error_handler(
            static function (int $severity, string $message) : never {
                throw new ErrorException($message, 0, $severity);
            },
            E_DEPRECATED
        );
        try {
            self::assertFalse($method->invoke($fluent));
        } finally {
            restore_error_handler();
        }
    }
}
