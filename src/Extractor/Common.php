<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\MySQL;
use Keboola\DbExtractor\Configuration\ExportConfig;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Metadata\MetadataProvider;
use PDO;
use Psr\Log\LoggerInterface;

class Common extends BaseExtractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];

    protected string $database;

    protected string $incrementalFetchingColType;

    private CommonMetadataProvider $metadataProvider;

    public function __construct(array $parameters, array $state, LoggerInterface $logger)
    {
        parent::__construct($parameters, $state, $logger);
        $this->metadataProvider = new CommonMetadataProvider($this->db, $parameters['db']['database']);
    }

    public function createConnection(array $params): PDO
    {
        // convert errors to PDOExceptions
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ];

        // check params
        foreach (['host', 'database', 'user', '#password'] as $r) {
            if (!isset($params[$r])) {
                throw new UserException(sprintf('Parameter "%s" is missing.', $r));
            }
        }

        $this->database = $params['database'];

        $port = isset($params['port']) ? $params['port'] : '3306';
        $dsn = sprintf('mysql:host=%s;port=%s;dbname=%s;charset=utf8', $params['host'], $port, $params['database']);

        $pdo = new PDO($dsn, $params['user'], $params['#password'], $options);
        $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        $pdo->exec('SET NAMES utf8;');

        return $pdo;
    }

    public function testConnection(): void
    {
        $this->db->query('SELECT 1');
    }

    public function validateIncrementalFetching(ExportConfig $export): void
    {
        $res = $this->db->query(
            sprintf(
                'SELECT * FROM INFORMATION_SCHEMA.COLUMNS as cols 
                            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s',
                $this->db->quote($export->getTable()->getSchema()),
                $this->db->quote($export->getTable()->getName()),
                $this->db->quote($export->getIncrementalColumn())
            )
        );
        $columns = $res->fetchAll();
        if (count($columns) === 0) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching was not found in the table',
                    $export->getIncrementalColumn()
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
                    $export->getIncrementalColumn()
                )
            );
        }
    }

    public function simpleQuery(ExportConfig $export): string
    {
        $sql = [];

        if ($export->hasColumns()) {
            $sql[] = sprintf('SELECT %s', implode(', ', array_map(
                fn(string $c) => $this->quote($c),
                $export->getColumns()
            )));
        } else {
            $sql[] = 'SELECT *';
        }

        $sql[] = sprintf(
            'FROM %s.%s',
            $this->quote($export->getTable()->getSchema()),
            $this->quote($export->getTable()->getName())
        );

        if ($export->isIncremental() && isset($this->state['lastFetchedRow'])) {
            if ($this->incrementalFetchingColType === self::INCREMENT_TYPE_NUMERIC) {
                $sql[] = sprintf(
                    'WHERE %s > %d',
                    $this->quote($export->getIncrementalColumn()),
                    (int) $this->state['lastFetchedRow']
                );
            } else if ($this->incrementalFetchingColType === self::INCREMENT_TYPE_TIMESTAMP) {
                $sql[] = sprintf(
                    'WHERE %s > \'%s\'',
                    $this->quote($export->getIncrementalColumn()),
                    $this->state['lastFetchedRow']
                );
            } else {
                throw new ApplicationException(
                    sprintf('Unknown incremental fetching column type %s', $this->incrementalFetchingColType)
                );
            }
        }

        if ($export->hasIncrementalLimit()) {
            $sql[] = sprintf(
                'ORDER BY %s LIMIT %d',
                $this->quote($export->getIncrementalColumn()),
                $export->getIncrementalLimit()
            );
        }

        return implode(' ', $sql);
    }

    public function getMetadataProvider(): MetadataProvider
    {
        return $this->metadataProvider;
    }

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $export): ?string
    {
        $sql = sprintf(
            'SELECT MAX(%s) as %s FROM %s.%s',
            $this->quote($export->getIncrementalColumn()),
            $this->quote($export->getIncrementalColumn()),
            $this->quote($export->getTable()->getSchema()),
            $this->quote($export->getTable()->getName())
        );
        $result = $this->db->query($sql)->fetchAll();
        return $result ? $result[0][$export->getIncrementalColumn()] : null;
    }

    private function quote(string $obj): string
    {
        return "`{$obj}`";
    }
}
