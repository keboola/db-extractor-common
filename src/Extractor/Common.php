<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\MySQL;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\Temp\Temp;
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

    public function createConnection(DatabaseConfig $databaseConfig): PDO
    {
        // convert errors to PDOExceptions
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ];

        // check params
        if (!$databaseConfig->hasDatabase()) {
            throw new UserException(sprintf('Parameter "database" is missing.'));
        }

        // ssl encryption
        if ($databaseConfig->hasSSLConnection()) {
            $sslConnectionConfig = $databaseConfig->getSslConnectionConfig();
            $temp = new Temp();

            if ($sslConnectionConfig->hasKey()) {
                $options[PDO::MYSQL_ATTR_SSL_KEY] = SslHelper::createSSLFile($temp, $sslConnectionConfig->getKey());
            }
            if ($sslConnectionConfig->hasCert()) {
                $options[PDO::MYSQL_ATTR_SSL_CERT] = SslHelper::createSSLFile($temp, $sslConnectionConfig->getCert());
            }
            if ($sslConnectionConfig->hasCa()) {
                $options[PDO::MYSQL_ATTR_SSL_CA] = SslHelper::createSSLFile($temp, $sslConnectionConfig->getCa());
            }
            if ($sslConnectionConfig->hasCipher()) {
                $options[PDO::MYSQL_ATTR_SSL_CIPHER] = (string) $sslConnectionConfig->getCipher();
            } else {
                $options[PDO::MYSQL_ATTR_SSL_CIPHER] = 'DEFAULT@SECLEVEL=1';
            }
            $options[PDO::MYSQL_ATTR_SSL_VERIFY_SERVER_CERT] = $sslConnectionConfig->isVerifyServerCert();
        }

        $this->database = $databaseConfig->getDatabase();

        $dsn = sprintf(
            'mysql:host=%s;port=%s;dbname=%s;charset=utf8',
            $databaseConfig->getHost(),
            !$databaseConfig->hasPort() ? $databaseConfig->getPort() : '3306',
            $databaseConfig->getDatabase()
        );

        $pdo = new PDO($dsn, $databaseConfig->getUsername(), $databaseConfig->getPassword(), $options);
        $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        $pdo->exec('SET NAMES utf8;');

        if ($databaseConfig->hasSSLConnection()) {
            /** @var \PDOStatement $res */
            $res = $pdo->query("SHOW STATUS LIKE 'Ssl_cipher';");
            $status = $res->fetch(PDO::FETCH_ASSOC);

            if (empty($status['Value'])) {
                throw new UserException(sprintf('Connection is not encrypted'));
            } else {
                $this->logger->info('Using SSL cipher: ' . $status['Value']);
            }
        }

        return $pdo;
    }

    public function testConnection(): void
    {
        $this->db->query('SELECT 1');
    }

    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        $res = $this->db->query(
            sprintf(
                'SELECT * FROM INFORMATION_SCHEMA.COLUMNS as cols 
                            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s',
                $this->db->quote($exportConfig->getTable()->getSchema()),
                $this->db->quote($exportConfig->getTable()->getName()),
                $this->db->quote($exportConfig->getIncrementalFetchingColumn())
            )
        );
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
                fn(string $c) => $this->quote($c),
                $exportConfig->getColumns()
            )));
        } else {
            $sql[] = 'SELECT *';
        }

        $sql[] = sprintf(
            'FROM %s.%s',
            $this->quote($exportConfig->getTable()->getSchema()),
            $this->quote($exportConfig->getTable()->getName())
        );

        if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
            if ($this->incrementalFetchingColType === self::INCREMENT_TYPE_NUMERIC) {
                $sql[] = sprintf(
                    // intentionally ">=" last row should be included, it is handled by storage deduplication process
                    'WHERE %s >= %d',
                    $this->quote($exportConfig->getIncrementalFetchingColumn()),
                    (int) $this->state['lastFetchedRow']
                );
            } else if ($this->incrementalFetchingColType === self::INCREMENT_TYPE_TIMESTAMP) {
                $sql[] = sprintf(
                    // intentionally ">=" last row should be included, it is handled by storage deduplication process
                    'WHERE %s >= \'%s\'',
                    $this->quote($exportConfig->getIncrementalFetchingColumn()),
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
                $this->quote($exportConfig->getIncrementalFetchingColumn()),
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
            $this->quote($exportConfig->getIncrementalFetchingColumn()),
            $this->quote($exportConfig->getIncrementalFetchingColumn()),
            $this->quote($exportConfig->getTable()->getSchema()),
            $this->quote($exportConfig->getTable()->getName())
        );
        $result = $this->db->query($sql)->fetchAll();
        return $result ? $result[0][$exportConfig->getIncrementalFetchingColumn()] : null;
    }

    private function quote(string $obj): string
    {
        return "`{$obj}`";
    }
}
