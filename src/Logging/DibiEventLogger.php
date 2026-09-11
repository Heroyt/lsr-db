<?php

declare(strict_types=1);

namespace Lsr\Db\Logging;

use Dibi\Event;
use Dibi\Exception;
use Psr\Log\LoggerInterface;

/** Translates failed Dibi events into the legacy database log records. */
final readonly class DibiEventLogger
{
    public function __construct(private LoggerInterface $logger) {
    }

    public function __invoke(Event $event): void {
        if ( ! $event->result instanceof Exception) {
            return;
        }

        $message = $event->result->getMessage();
        if ($code = $event->result->getCode()) {
            $message = '(' . $code . ') ' . $message;
        }
        $this->logger->error($message);

        $sql = $event->result->getSql();
        if ( ! empty($sql)) {
            $this->logger->debug('SQL: ' . $sql);
        }
    }
}
