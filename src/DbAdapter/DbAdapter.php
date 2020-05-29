<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\DbAdapter;

use Keboola\DbExtractor\DbAdapter\QueryResult\QueryResult;

interface DbAdapter
{
    public const CONNECT_MAX_RETRIES = 5;

    public function testConnection(): void;

    /**
     * Returns low-level connection resource or object.
     * @return resource|object
     */
    public function getConnection();

    public function quote(string $str): string;

    public function quoteIdentifier(string $str): string;

    public function query(string $query, int $maxRetries): QueryResult;

    /**
     * @param callable $processor (QueryResult $dbResult): array
     * @return mixed - returned value from $processor
     */
    public function queryAndProcess(string $query, int $maxRetries, callable $processor);
}
