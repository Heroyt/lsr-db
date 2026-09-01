<?php

declare(strict_types=1);

namespace Lsr\Db\Lifecycle;

interface DatabaseLifecycleHookInterface
{
    public function record(DatabaseLifecycleEvent $event): void;
}
