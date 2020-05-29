<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\DbAdapter;

use ErrorException;
use Keboola\DbExtractor\DbAdapter\QueryResult\OdbcQueryResult;
use Keboola\DbExtractor\Exception\OdbcException;
use Throwable;
use PDOException;
use PDOStatement;
use Psr\Log\LoggerInterface;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\DbAdapter\QueryResult\PdoQueryResult;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\DbAdapter\QueryResult\QueryResult;

class OdbcDbAdapter extends BaseDbAdapter
{
    private string $dsn;

    private string $user;

    private string $password;

    /** @var callable|null */
    private $init;

    /** @var resource */
    private $connection;

    public function __construct(
        LoggerInterface $logger,
        string $dsn,
        string $user,
        string $password,
        ?callable $init = null
    ) {
        $this->dsn = $dsn;
        $this->user = $user;
        $this->password = $password;
        $this->init = $init;
        parent::__construct($logger);
    }

    public function testConnection(): void
    {
        $this->query('SELECT 1', 1);
    }

    /**
     * @return resource
     */
    public function getConnection()
    {
        return $this->connection;
    }

    protected function createConnection(): void
    {
        try {
            $this->connection = $this
                ->createRetryProxy(self::CONNECT_MAX_RETRIES)
                ->call(function () {
                    $connection = $this->checkError(
                        odbc_connect($this->dsn, $this->user, $this->password) // intentionally @
                    );
                    if ($this->init) {
                        ($this->init)($connection);
                    }
                    return $connection;
                });
        } catch (Throwable $e) {
            throw new UserException('Error connecting to DB: ' . $e->getMessage(), 0, $e);
        }
    }

    public function quote(string $str): string
    {
        return "'" . str_replace("'", "''", $str) . "'";
    }

    public function quoteIdentifier(string $str): string
    {
        return '`' . str_replace('`', '``', $str) . '`';
    }

    protected function doQuery(string $query): QueryResult
    {
        $stmt = $this->checkError(
            @odbc_exec($this->connection, $query) // intentionally @
        );
        return new OdbcQueryResult($stmt);
    }

    /**
     * @param resource|false $resource
     * @return resource
     */
    protected function checkError($resource)
    {
        if ($resource === false) {
            throw new OdbcException(
                odbc_errormsg($this->connection) . ' ' . odbc_error($this->connection),
                0
            );
        }

        return $resource;
    }

    protected function getExpectedExceptionClasses(): array
    {
        return [
            OdbcException::class,
            DeadConnectionException::class, // see BaseDbAdapter:isAlive()
        ];
    }
}
