<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use PDO;
use Psr\Log\LoggerInterface;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\MySQL;
use Keboola\DbExtractor\DbAdapter\DbAdapter;
use Keboola\DbExtractor\DbAdapter\PdoDbAdapter;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;

class Common extends BaseExtractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];


    private CommonMetadataProvider $metadataProvider;

    protected string $incrementalFetchingColType;

    public function __construct(array $parameters, array $state, LoggerInterface $logger)
    {
        parent::__construct($parameters, $state, $logger);
        $this->metadataProvider = new CommonMetadataProvider($this->dbAdapter, $parameters['db']['database']);
    }

    public function createDbAdapter(array $params): DbAdapter
    {
        // check params
        foreach (['host', 'database', 'user', '#password'] as $r) {
            if (!isset($params[$r])) {
                throw new UserException(sprintf('Parameter "%s" is missing.', $r));
            }
        }

        $port = isset($params['port']) ? $params['port'] : '3306';
        $dsn = sprintf('mysql:host=%s;port=%s;dbname=%s;charset=utf8', $params['host'], $port, $params['database']);
        return new PdoDbAdapter(
            $this->logger,
            $dsn,
            $params['user'],
            $params['#password'],
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, // convert errors to PDOExceptions
                PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => false,
            ],
            function (PDO $pdo): void {
                $pdo->exec('SET NAMES utf8;');
            }
        );
    }

    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        $sql = sprintf(
            'SELECT * FROM INFORMATION_SCHEMA.COLUMNS as cols 
                            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s',
            $this->dbAdapter->quote($exportConfig->getTable()->getSchema()),
            $this->dbAdapter->quote($exportConfig->getTable()->getName()),
            $this->dbAdapter->quote($exportConfig->getIncrementalFetchingColumn())
        );
        $res = $this->dbAdapter->query($sql, $exportConfig->getMaxRetries());
        $columns = $res->fetchAll();
        if (count($columns) === 0) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching was not found in the table',
                    $exportConfig->getIncrementalFetchingColumn()
                )
            );
        }
        try {
            $datatype = new MySQL($columns[0]['DATA_TYPE']);
            if (in_array($datatype->getBasetype(), self::NUMERIC_BASE_TYPES)) {
                $this->incrementalFetchingColType = self::INCREMENT_TYPE_NUMERIC;
            } else if ($datatype->getBasetype() === 'TIMESTAMP') {
                $this->incrementalFetchingColType = self::INCREMENT_TYPE_TIMESTAMP;
            } else {
                throw new UserException('invalid incremental fetching column type');
            }
        } catch (InvalidLengthException | UserException $exception) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not a numeric or timestamp type column',
                    $exportConfig->getIncrementalFetchingColumn()
                )
            );
        }
    }

    public function simpleQuery(ExportConfig $exportConfig): string
    {
        $sql = [];

        if ($exportConfig->hasColumns()) {
            $sql[] = sprintf('SELECT %s', implode(', ', array_map(
                fn(string $c) => $this->dbAdapter->quoteIdentifier($c),
                $exportConfig->getColumns()
            )));
        } else {
            $sql[] = 'SELECT *';
        }

        $sql[] = sprintf(
            'FROM %s.%s',
            $this->dbAdapter->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->dbAdapter->quoteIdentifier($exportConfig->getTable()->getName())
        );

        if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
            if ($this->incrementalFetchingColType === self::INCREMENT_TYPE_NUMERIC) {
                $sql[] = sprintf(
                    // intentionally ">=" last row should be included, it is handled by storage deduplication process
                    'WHERE %s >= %d',
                    $this->dbAdapter->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                    (int) $this->state['lastFetchedRow']
                );
            } else if ($this->incrementalFetchingColType === self::INCREMENT_TYPE_TIMESTAMP) {
                $sql[] = sprintf(
                    // intentionally ">=" last row should be included, it is handled by storage deduplication process
                    'WHERE %s >= \'%s\'',
                    $this->dbAdapter->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                    $this->state['lastFetchedRow']
                );
            } else {
                throw new ApplicationException(
                    sprintf('Unknown incremental fetching column type %s', $this->incrementalFetchingColType)
                );
            }
        }

        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $sql[] = sprintf(
                'ORDER BY %s LIMIT %d',
                $this->dbAdapter->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                $exportConfig->getIncrementalFetchingLimit()
            );
        }

        return implode(' ', $sql);
    }

    public function getMetadataProvider(): MetadataProvider
    {
        return $this->metadataProvider;
    }

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        $sql = sprintf(
            'SELECT MAX(%s) as %s FROM %s.%s',
            $this->dbAdapter->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->dbAdapter->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->dbAdapter->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->dbAdapter->quoteIdentifier($exportConfig->getTable()->getName())
        );
        $result = $this->dbAdapter->query($sql, $exportConfig->getMaxRetries())->fetchAll();
        return $result ? $result[0][$exportConfig->getIncrementalFetchingColumn()] : null;
    }
}
