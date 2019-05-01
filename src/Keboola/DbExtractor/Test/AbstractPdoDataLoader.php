<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

use PDO;
use PDOException;
use RuntimeException;

abstract class AbstractPdoDataLoader extends AbstractDataLoader
{
    /** @var PDO */
    protected $db;

    public function __construct(PDO $db)
    {
        $this->db = $db;
    }

    protected function executeQuery(string $query): void
    {
        try {
            $this->db->exec($query);
        } catch (PDOException $e) {
            throw new RuntimeException(sprintf('"%s" resulted in "%s"', $query, $e->getMessage()), 0, $e);
        }
    }

    protected function quote(string $string): string
    {
        return $this->db->quote($string);
    }

    protected function quoteIdentifier(string $identifier): string
    {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }
}
