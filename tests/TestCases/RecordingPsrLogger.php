<?php

declare(strict_types=1);

namespace TestCases;

use Psr\Log\AbstractLogger;
use Throwable;

final class RecordingPsrLogger extends AbstractLogger
{
    /** @var list<array{level: mixed, message: string, context: array<string, mixed>}> */
    public array $records = [];
    public ?Throwable $failure = null;

    public function log($level, $message, array $context = []): void {
        $this->records[] = ['level' => $level, 'message' => (string) $message, 'context' => $context];
        if ($this->failure !== null) {
            throw $this->failure;
        }
    }
}
