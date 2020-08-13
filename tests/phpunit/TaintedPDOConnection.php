<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Adapter\PDO\PdoConnection;
use Psr\Log\LoggerInterface;

class TaintedPDOConnection extends PdoConnection
{
    protected TaintedPDO $taintedPDO;

    public function __construct(TaintedPDO $taintedPDO, LoggerInterface $logger)
    {
        $this->taintedPDO = $taintedPDO;
        parent::__construct($logger, '', '', '', [], null);
    }

    protected function connect(): void
    {
        // Use TaintedPDO on reconnect too
        $this->pdo = $this->taintedPDO;
    }
}
