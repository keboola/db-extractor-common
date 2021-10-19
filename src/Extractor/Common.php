<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\MySQL;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\PDO\PdoConnection;
use Keboola\DbExtractor\Adapter\PDO\PdoExportAdapter;
use Keboola\DbExtractor\Adapter\Query\DefaultQueryFactory;
use Keboola\DbExtractor\Adapter\ResultWriter\DefaultResultWriter;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Temp\Temp;
use PDO;

class Common extends BaseExtractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];

    protected string $incrementalFetchingColType;

    protected PdoConnection $connection;

    protected function createMetadataProvider(): MetadataProvider
    {
        return new CommonMetadataProvider($this->connection, $this->getDatabaseConfig()->getDatabase());
    }

    protected function createExportAdapter(): ExportAdapter
    {
        $resultWriter = new DefaultResultWriter($this->state);
        $simpleQueryFactory = new DefaultQueryFactory($this->state);
        return new PdoExportAdapter(
            $this->logger,
            $this->connection,
            $simpleQueryFactory,
            $resultWriter,
            $this->dataDir,
            $this->state
        );
    }

    protected function createConnection(DatabaseConfig $databaseConfig): void
    {
        $options = [];

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

        $dsn = sprintf(
            'mysql:host=%s;port=%s;dbname=%s;charset=utf8',
            $databaseConfig->getHost(),
            $databaseConfig->hasPort() ? $databaseConfig->getPort() : '3306',
            $databaseConfig->getDatabase()
        );

        // Disable connect retries for sync actions
        $connectRetries = $this->isSyncAction() ? 1 : PdoConnection::CONNECT_DEFAULT_MAX_RETRIES;

        // Create connection
        $this->connection = new PdoConnection(
            $this->logger,
            $dsn,
            $databaseConfig->getUsername(),
            $databaseConfig->getPassword(),
            $options,
            function (PDO $pdo): void {
                $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
                $pdo->exec('SET NAMES utf8;');
            },
            $connectRetries,
            $databaseConfig->getInitQueries()
        );

        // Check SSL
        if ($databaseConfig->hasSSLConnection()) {
            $status = $this->connection->query("SHOW STATUS LIKE 'Ssl_cipher';", 1)->fetch();
            if (empty($status['Value'])) {
                throw new UserException(sprintf('Connection is not encrypted'));
            } else {
                $this->logger->info('Using SSL cipher: ' . $status['Value']);
            }
        }
    }

    public function testConnection(): void
    {
        $this->connection->testConnection();
    }

    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        $res = $this->connection->query(
            sprintf(
                'SELECT * FROM INFORMATION_SCHEMA.COLUMNS as cols 
                            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s',
                $this->connection->quote($exportConfig->getTable()->getSchema()),
                $this->connection->quote($exportConfig->getTable()->getName()),
                $this->connection->quote($exportConfig->getIncrementalFetchingColumn())
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
            } elseif ($datatype->getBasetype() === 'TIMESTAMP') {
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

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        $sql = sprintf(
            'SELECT MAX(%s) as %s FROM %s.%s',
            $this->connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
            $this->connection->quoteIdentifier($exportConfig->getTable()->getSchema()),
            $this->connection->quoteIdentifier($exportConfig->getTable()->getName())
        );
        $result = $this->connection->query($sql)->fetchAll();
        return $result ? $result[0][$exportConfig->getIncrementalFetchingColumn()] : null;
    }
}
