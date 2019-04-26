<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

use PDO;

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
        $this->db->exec($query);
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
