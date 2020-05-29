<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\DbAdapter;

use ErrorException;
use Throwable;
use PDO;
use PDOException;
use PDOStatement;
use Psr\Log\LoggerInterface;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\DbAdapter\QueryResult\PdoQueryResult;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\DbAdapter\QueryResult\QueryResult;

class PdoDbAdapter extends BaseDbAdapter
{
    private string $dsn;

    private string $user;

    private string $password;

    private array $options;

    /** @var callable|null */
    private $init;

    private PDO $pdo;

    public function __construct(
        LoggerInterface $logger,
        string $dsn,
        string $user,
        string $password,
        array $options,
        ?callable $init
    ) {
        $this->dsn = $dsn;
        $this->user = $user;
        $this->password = $password;
        $this->options = $options;
        $this->init = $init;
        parent::__construct($logger);
    }

    public function testConnection(): void
    {
        $this->query('SELECT 1', 1);
    }

    public function getConnection(): PDO
    {
        return $this->pdo;
    }

    protected function createConnection(): void
    {
        try {
            $this->pdo = $this
                ->createRetryProxy(self::CONNECT_MAX_RETRIES)
                ->call(function (): PDO {
                    $pdo = new PDO($this->dsn, $this->user, $this->password, $this->options);
                    if ($this->init) {
                        ($this->init)($pdo);
                    }
                    return $pdo;
                });
        } catch (Throwable $e) {
            if (strstr(strtolower($e->getMessage()), 'could not find driver')) {
                throw new ApplicationException('Missing driver: ' . $e->getMessage());
            }
            throw new UserException('Error connecting to DB: ' . $e->getMessage(), 0, $e);
        }
    }

    public function quote(string $str): string
    {
        return $this->pdo->quote($str);
    }

    public function quoteIdentifier(string $str): string
    {
        return "`{$str}`";
    }

    protected function doQuery(string $query): QueryResult
    {
        /** @var PDOStatement $stmt */
        $stmt = $this->pdo->prepare($query);
        $stmt->execute();
        return new PdoQueryResult($stmt);
    }

    protected function getExpectedExceptionClasses(): array
    {
        return [
            PDOException::class,
            ErrorException::class, // eg. ErrorException: Warning: Empty row packet body
            DeadConnectionException::class, // see BaseDbAdapter:isAlive()
        ];
    }
}
