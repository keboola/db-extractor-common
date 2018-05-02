<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\Csv\Exception as CsvException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Keboola\SSHTunnel\SSH;
use Keboola\SSHTunnel\SSHException;
use Nette\Utils;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

abstract class Extractor
{
    public const DEFAULT_MAX_TRIES = 5;

    /**
     * @var \PDO
     */
    protected $db;

    /**
     * @var  array
     */
    protected $state;

    /**
     * @var  array|null with keys type (autoIncrement or timestamp), column, and limit
     */
    protected $incrementalFetching;

    /**
     * @var Logger
     */
    protected $logger;

    /**
     * @var mixed
     */
    protected $dataDir;

    /**
     * @var array|mixed
     */
    private $dbParameters;

    public function __construct(array $parameters, array $state = [], ?Logger $logger = null)
    {
        if ($logger) {
            $this->logger = $logger;
        }
        $this->dataDir = $parameters['data_dir'];
        $this->state = $state;

        if (isset($parameters['db']['ssh']['enabled']) && $parameters['db']['ssh']['enabled']) {
            $parameters['db'] = $this->createSshTunnel($parameters['db']);
        }
        $this->dbParameters = $parameters['db'];

        try {
            $this->db = $this->createConnection($this->dbParameters);
        } catch (\Throwable $e) {
            if (strstr(strtolower($e->getMessage()), 'could not find driver')) {
                throw new ApplicationException("Missing driver: " . $e->getMessage());
            }
            throw new UserException("Error connecting to DB: " . $e->getMessage(), 0, $e);
        }
        if (isset($parameters['incrementalFetchingColumn'])) {
            $this->validateIncrementalFetching(
                $parameters['table'],
                $parameters['incrementalFetchingColumn'],
                isset($parameters['incrementalFetchingLimit']) ? $parameters['incrementalFetchingLimit'] : null
            );
        }
    }

    public function createSshTunnel(array $dbConfig): array
    {
        $sshConfig = $dbConfig['ssh'];
        // check params
        foreach (['keys', 'sshHost'] as $k) {
            if (empty($sshConfig[$k])) {
                throw new UserException(sprintf("Parameter '%s' is missing.", $k));
            }
        }
        
        $sshConfig['remoteHost'] = $dbConfig['host'];
        $sshConfig['remotePort'] = $dbConfig['port'];

        if (empty($sshConfig['user'])) {
            $sshConfig['user'] = $dbConfig['user'];
        }
        if (empty($sshConfig['localPort'])) {
            $sshConfig['localPort'] = 33006;
        }
        if (empty($sshConfig['sshPort'])) {
            $sshConfig['sshPort'] = 22;
        }
        $sshConfig['privateKey'] = isset($sshConfig['keys']['#private'])
            ?$sshConfig['keys']['#private']
            :$sshConfig['keys']['private'];
        $tunnelParams = array_intersect_key(
            $sshConfig,
            array_flip(
                [
                'user', 'sshHost', 'sshPort', 'localPort', 'remoteHost', 'remotePort', 'privateKey',
                ]
            )
        );
        $this->logger->info("Creating SSH tunnel to '" . $tunnelParams['sshHost'] . "'");
        try {
            $ssh = new SSH();
            $ssh->openTunnel($tunnelParams);
        } catch (SSHException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }

        $dbConfig['host'] = '127.0.0.1';
        $dbConfig['port'] = $sshConfig['localPort'];

        return $dbConfig;
    }

    /**
     * @param array $params
     * @return \PDO|mixed
     */
    abstract public function createConnection(array $params);

    /**
     * @return void|mixed
     */
    abstract public function testConnection();

    /**
     * @param array|null $tables - an optional array of tables with tableName and schema properties
     * @return mixed
     */
    abstract public function getTables(?array $tables = null): array;

    abstract public function simpleQuery(array $table, array $columns = array()): string;

    /**
     * @param array    $table
     * @param string   $columnName
     * @param int|null $limit
     * @throws UserException
     */
    public function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        throw new UserException('Incremental Fetching is not supported by this extractor.');
    }

    public function export(array $table): array
    {
        $outputTable = $table['outputTable'];
        $csv = $this->createOutputCsv($outputTable);

        $this->logger->info("Exporting to " . $outputTable);

        $isAdvancedQuery = true;
        if (array_key_exists('table', $table) && !array_key_exists('query', $table)) {
            $isAdvancedQuery = false;
            $query = $this->simpleQuery($table['table'], $table['columns']);
        } else {
            $query = $table['query'];
        }

        try {
            /** @var \PDOStatement $stmt */
            $stmt = $this->executeQuery(
                $query,
                isset($table['retries']) ? (int) $table['retries'] : self::DEFAULT_MAX_TRIES
            );
        } catch (\Throwable $e) {
            throw $this->handleDbError($e, $table);
        }

        try {
            $result = $this->writeToCsv($stmt, $csv, $isAdvancedQuery);
        } catch (CsvException $e) {
             throw new ApplicationException("Write to CSV failed: " . $e->getMessage(), 0, $e);
        }

        if ($result['rows'] > 0) {
            $this->createManifest($table);
        } else {
            $this->logger->warn(
                sprintf(
                    "Query returned empty result. Nothing was imported for table [%s]",
                    $table['name']
                )
            );
        }

        $output = [
            "outputTable"=> $outputTable,
            "rows" => $result['rows'],
        ];
        // output state
        if (!empty($result['lastFetchedRow'])) {
            $output["state"]['lastFetchedRow'] = $result['lastFetchedRow'];
        }
        return $output;
    }

    private function handleDbError(\Throwable $e, ?array $table = null, ?int $counter = null): UserException
    {
        $message = "";
        if ($table) {
            $message = sprintf("[%s]: ", $table['name']);
        }
        $message .= sprintf('DB query failed: %s', $e->getMessage());
        if ($counter) {
            $message .= sprintf(' Tried %d times.', $counter);
        }
        $exception = new UserException($message, 0, $e);
        return $exception;
    }

    protected function executeQuery(string $query, int $maxTries): \PDOStatement
    {
        $retryPolicy = new SimpleRetryPolicy($maxTries, ['PDOException', 'ErrorException']);
        $backOffPolicy = new ExponentialBackOffPolicy(1000);
        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);
        $counter = 0;
        /** @var \Exception $lastException */
        $lastException = null;
        try {
            $stmt = $proxy->call(function () use ($query, &$counter, &$lastException) {
                if ($counter > 0) {
                    $this->logger->info(sprintf('%s. Retrying... [%dx]', $lastException->getMessage(), $counter));
                    try {
                        $this->db = $this->createConnection($this->dbParameters);
                    } catch (\Throwable $e) {
                    };
                }
                try {
                    /** @var \PDOStatement $stmt */
                    $stmt = @$this->db->prepare($query);
                    @$stmt->execute();
                    return $stmt;
                } catch (\Throwable $e) {
                    $lastException = $this->handleDbError($e, null, $counter + 1);
                    $counter++;
                    throw $e;
                }
            });
        } catch (\Throwable $e) {
            if ($lastException) {
                throw $lastException;
            }
            throw $e;
        }
        return $stmt;
    }

    /**
     * @param \PDOStatement $stmt
     * @param CsvFile       $csv
     * @param boolean       $includeHeader
     * @return array ['rows', 'lastFetchedRow']
     * @throws CsvException|UserException
     */
    protected function writeToCsv(\PDOStatement $stmt, CsvFile $csv, bool $includeHeader = true): array
    {
        $output = [];
        $resultRow = @$stmt->fetch(\PDO::FETCH_ASSOC);

        if (is_array($resultRow) && !empty($resultRow)) {
            // write header and first line
            if ($includeHeader) {
                $csv->writeRow(array_keys($resultRow));
            }
            $csv->writeRow($resultRow);

            // write the rest
            $numRows = 1;
            $lastRow = $resultRow;
            while ($resultRow = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                $csv->writeRow($resultRow);
                $lastRow = $resultRow;
                $numRows++;
            }
            if (isset($this->incrementalFetching['column'])) {
                if (!array_key_exists($this->incrementalFetching['column'], $lastRow)) {
                    throw new UserException(
                        sprintf(
                            "The specified incremental fetching column %s not found in the table",
                            $this->incrementalFetching['column']
                        )
                    );
                }
                $output['lastFetchedRow'] = $lastRow[$this->incrementalFetching['column']];
            }
            $output['rows'] = $numRows;
            return $output;
        }
        // no rows found.  If incremental fetching is turned on, we need to preserve the last state
        if ($this->incrementalFetching['column'] && isset($this->state['lastFetchedRow'])) {
            $output = $this->state;
        }
        $output['rows'] = 0;
        return $output;
    }

    protected function createOutputCsv(string $outputTable): CsvFile
    {
        $outTablesDir = $this->dataDir . '/out/tables';
        if (!is_dir($outTablesDir)) {
            mkdir($outTablesDir, 0777, true);
        }
        return new CsvFile($this->getOutputFilename($outputTable));
    }

    /**
     * @param array $table
     * @return bool|int
     */
    protected function createManifest(array $table)
    {
        $outFilename = $this->getOutputFilename($table['outputTable']) . '.manifest';

        $manifestData = [
            'destination' => $table['outputTable'],
            'incremental' => $table['incremental'],
        ];

        if (!empty($table['primaryKey'])) {
            $manifestData['primary_key'] = $table['primaryKey'];
        }

        $manifestColumns = [];

        if (isset($table['table']) && !is_null($table['table'])) {
            $tables = $this->getTables([$table['table']]);
            if (count($tables) > 0) {
                $tableDetails = $tables[0];
                $columnMetadata = [];
                $sanitizedPks = [];
                foreach ($tableDetails['columns'] as $column) {
                    if (count($table['columns']) > 0 && !in_array($column['name'], $table['columns'])) {
                        continue;
                    }
                    // use sanitized name for primary key if available
                    if (in_array($column['name'], $table['primaryKey']) && array_key_exists('sanitizedName', $column)) {
                        $sanitizedPks[] = $column['sanitizedName'];
                    }
                    $datatypeKeys = ['type', 'length', 'nullable', 'default', 'format'];
                    $datatype = new GenericStorage(
                        $column['type'],
                        array_intersect_key($column, array_flip($datatypeKeys))
                    );
                    $columnName = $column['name'];
                    if (array_key_exists('sanitizedName', $column)) {
                        $columnName = $column['sanitizedName'];
                    }
                    $columnMetadata[$columnName] = $datatype->toMetadata();
                    $nonDatatypeKeys = array_diff_key($column, array_flip($datatypeKeys));
                    foreach ($nonDatatypeKeys as $key => $value) {
                        if ($key === 'name') {
                            $columnMetadata[$columnName][] = [
                                'key' => "KBC.sourceName",
                                'value' => $value,
                            ];
                        } else {
                            $columnMetadata[$columnName][] = [
                                'key' => "KBC." . $key,
                                'value' => $value,
                            ];
                        }
                    }
                    $manifestColumns[] = $columnName;
                }
                unset($tableDetails['columns']);
                foreach ($tableDetails as $key => $value) {
                    $manifestData['metadata'][] = [
                        "key" => "KBC." . $key,
                        "value" => $value,
                    ];
                }
                $manifestData['column_metadata'] = $columnMetadata;
                $manifestData['columns'] = $manifestColumns;
                if (!empty($sanitizedPks)) {
                    $manifestData['primary_key'] = $sanitizedPks;
                }
            }
        }
        return file_put_contents($outFilename, json_encode($manifestData));
    }

    protected function getOutputFilename(string $outputTableName): string
    {
        $sanitizedTablename = Utils\Strings::webalize($outputTableName, '._');
        return $this->dataDir . '/out/tables/' . $sanitizedTablename . '.csv';
    }
}
