<?php

declare(strict_types=1);

namespace TestCases;

use Lsr\Db\Lifecycle\DatabaseLifecycleEvent;
use Lsr\Db\Lifecycle\DatabaseLifecycleHookInterface;
use RuntimeException;

final class RecordingDatabaseLifecycleHook implements DatabaseLifecycleHookInterface
{
    /** @var list<DatabaseLifecycleEvent> */
    public array $events = [];
    public bool $fail = false;

    public function record(DatabaseLifecycleEvent $event): void {
        if ($this->fail) {
            throw new RuntimeException('Lifecycle hook failure.');
        }
        $this->events[] = $event;
    }
}
