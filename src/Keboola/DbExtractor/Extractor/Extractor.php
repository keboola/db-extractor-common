<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 13:04
 */

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
    const DEFAULT_MAX_TRIES = 5;

    /** @var \PDO */
    protected $db;

    /** @var Logger */
    protected $logger;

    protected $dataDir;

    private $dbParameters;

    public function __construct($parameters, Logger $logger)
    {
        $this->logger = $logger;
        $this->dataDir = $parameters['data_dir'];

        if (isset($parameters['db']['ssh']['enabled']) && $parameters['db']['ssh']['enabled']) {
            $parameters['db'] = $this->createSshTunnel($parameters['db']);
        }
        $this->dbParameters = $parameters['db'];

        try {
            $this->db = $this->createConnection($this->dbParameters);
        } catch (\Exception $e) {
            if (strstr(strtolower($e->getMessage()), 'could not find driver')) {
                throw new ApplicationException("Missing driver: " . $e->getMessage());
            }
            throw new UserException("Error connecting to DB: " . $e->getMessage(), 0, $e);
        }
    }

    public function createSshTunnel($dbConfig)
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
        $tunnelParams = array_intersect_key($sshConfig, array_flip([
            'user', 'sshHost', 'sshPort', 'localPort', 'remoteHost', 'remotePort', 'privateKey'
        ]));
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

    abstract public function createConnection($params);

    abstract public function testConnection();

    /**
     * @param array|null $tables - an optional array of table names
     * @return mixed
     */
    abstract public function getTables(array $tables = null);

    abstract public function simpleQuery(array $table, array $columns = array());

    public function export(array $table)
    {
        $outputTable = $table['outputTable'];
        $csv = $this->createOutputCsv($outputTable);

        $this->logger->info("Exporting to " . $outputTable);

        if (array_key_exists('table', $table) && !array_key_exists('query', $table)) {
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
        } catch (\Exception $e) {
            throw $this->handleDbError($e, $table);
        }

        try {
            $rows = $this->writeToCsv($stmt, $csv);
        } catch (CsvException $e) {
             throw new ApplicationException("Write to CSV failed: " . $e->getMessage(), 0, $e);
        }


        if ($rows > 0) {
            $this->createManifest($table);
        } else {
            $this->logger->warn(sprintf(
                "Query returned empty result. Nothing was imported for table [%s]",
                $table['name']
            ));
        }

        return $outputTable;
    }

    private function handleDbError(\Exception $e, $table = null)
    {
        $message = "";
        if ($table) {
            $message = sprintf("[%s]: ", $table['name']);
        }
        $message .= sprintf('DB query failed: %s', $e->getMessage());
        $exception = new UserException($message, 0, $e);
        return $exception;
    }

    /**
     * @param $query
     * @return int Number of rows returned by query
     * @throws \PDOException|\ErrorException
     */
    protected function executeQuery($query, int $maxTries)
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
                    } catch (\Exception $e) {
                    };
                }
                try {
                    /** @var \PDOStatement $stmt */
                    $stmt = @$this->db->prepare($query);
                    @$stmt->execute();
                    return $stmt;
                } catch (\Exception $e) {
                    $lastException = $this->handleDbError($e);
                    $counter++;
                    throw $e;
                }
            });
        } catch (\Exception $e) {
            if ($lastException) {
                throw $lastException;
            }
            throw $e;
        }
        return $stmt;
    }

    /**
     * @param \PDOStatement $stmt
     * @param CsvFile $csv
     * @return int number of rows written to output file
     * @throws CsvException
     */
    protected function writeToCsv(\PDOStatement $stmt, CsvFile $csv)
    {
        $resultRow = @$stmt->fetch(\PDO::FETCH_ASSOC);

        if (is_array($resultRow) && !empty($resultRow)) {
            // write header and first line
            $csv->writeRow(array_keys($resultRow));
            $csv->writeRow($resultRow);

            // write the rest
            $numRows = 1;
            while ($resultRow = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                $csv->writeRow($resultRow);
                $numRows++;
            }

            return $numRows;
        }
        return 0;
    }

    protected function createOutputCsv($outputTable)
    {
        $outTablesDir = $this->dataDir . '/out/tables';
        if (!is_dir($outTablesDir)) {
            mkdir($outTablesDir, 0777, true);
        }
        return new CsvFile($this->getOutputFilename($outputTable));
    }

    protected function createManifest($table)
    {
        $outFilename = $this->getOutputFilename($table['outputTable']) . '.manifest';

        $manifestData = [
            'destination' => $table['outputTable'],
            'incremental' => $table['incremental']
        ];

        if (!empty($table['primaryKey'])) {
            $manifestData['primary_key'] = $table['primaryKey'];
        }

        if (isset($table['table']) && !is_null($table['table'])) {
            $tables = $this->getTables([$table['table']]);
            if (count($tables) > 0) {
                $tableDetails = $tables[0];
                $columnMetadata = [];
                foreach ($tableDetails['columns'] as $column) {
                    if (count($table['columns']) > 0 && !in_array($column['name'], $table['columns'])) {
                        continue;
                    }
                    $datatypeKeys = ['type', 'length', 'nullable', 'default', 'format'];
                    $datatype = new GenericStorage(
                        $column['type'],
                        array_intersect_key($column, array_flip($datatypeKeys))
                    );
                    $columnMetadata[$column['name']] = $datatype->toMetadata();
                    $nonDatatypeKeys = array_diff_key($column, array_flip($datatypeKeys));
                    foreach ($nonDatatypeKeys as $key => $value) {
                        if ($key !== 'name') {
                            $columnMetadata[$column['name']][] = [
                                'key' => "KBC." . $key,
                                'value'=> $value
                            ];
                        }
                    }
                }
                unset($tableDetails['columns']);
                foreach ($tableDetails as $key => $value) {
                    $manifestData['metadata'][] = [
                        "key" => "KBC." . $key,
                        "value" => $value
                    ];
                }
                $manifestData['column_metadata'] = $columnMetadata;
            }
        }
        return file_put_contents($outFilename, json_encode($manifestData));
    }

    protected function getOutputFilename($outputTableName)
    {
        $sanitizedTablename = Utils\Strings::webalize($outputTableName, '._');
        return $this->dataDir . '/out/tables/' . $sanitizedTablename . '.csv';
    }
}
