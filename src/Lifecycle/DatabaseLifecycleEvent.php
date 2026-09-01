<?php

declare(strict_types=1);

namespace Lsr\Db\Lifecycle;

final readonly class DatabaseLifecycleEvent
{
    public const string CONNECT = 'connect';
    public const string SELECT = 'select';
    public const string INSERT = 'insert';
    public const string UPDATE = 'update';
    public const string DELETE = 'delete';
    public const string QUERY = 'query';
    public const string BEGIN = 'begin';
    public const string COMMIT = 'commit';
    public const string ROLLBACK = 'rollback';

    public const string SUCCESS = 'success';
    public const string ERROR = 'error';

    public function __construct(
        public string $operation,
        public string $outcome,
        public float $durationSeconds,
        public string $system,
        public ?string $connectionName = null,
        public ?int $rowCount = null,
        public ?string $errorType = null,
        public ?string $sql = null,
    ) {
    }
}
