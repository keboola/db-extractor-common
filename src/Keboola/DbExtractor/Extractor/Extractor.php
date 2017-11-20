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
use Symfony\Component\Yaml\Yaml;

abstract class Extractor
{
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

    abstract protected function describeTable(array $table);

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

        $maxTries = (isset($table['retries']) && $table['retries'])?$table['retries']:5;
        $tries = 0;
        $exception = null;
        $rows = 0;

        while ($tries < $maxTries) {
            $exception = null;
            try {
                $rows = $this->executeQuery($query, $csv);
                break;
            } catch (\PDOException $e) {
                $exception = $this->handleDbError($e, $table);
                $this->logger->info(sprintf('%s. Retrying... [%dx]', $exception->getMessage(), $tries + 1));
            } catch (\ErrorException $e) {
                $exception = $this->handleDbError($e, $table);
                $this->logger->info(sprintf('%s. Retrying... [%dx]', $exception->getMessage(), $tries + 1));
            } catch (CsvException $e) {
                $exception = new ApplicationException("Write to CSV failed: " . $e->getMessage(), 0, $e);
            }
            sleep(pow($tries, 2));
            $tries++;
        }

        if ($exception) {
            throw $exception;
        }

        if ($rows > 0) {
            $this->createManifest($table);
        }

        return $outputTable;
    }

    private function handleDbError(\Exception $e, $table)
    {
        $message = sprintf('DB query [' . $table['name'] . '] failed: %s', $e->getMessage());
        $exception = new UserException($message, 0, $e);

        try {
            $this->db = $this->createConnection($this->dbParameters);
        } catch (\Exception $e) {
        };
        return $exception;
    }

    /**
     * @param $query
     * @param CsvFile $csv
     * @return int Number of rows returned by query
     * @throws CsvException
     */
    protected function executeQuery($query, CsvFile $csv)
    {
        $stmt = @$this->db->prepare($query);
        @$stmt->execute();

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
        $this->logger->warn("Query returned empty result. Nothing was imported.");

        return 0;
    }

    protected function createOutputCsv($outputTable)
    {
        $outTablesDir = $this->dataDir . '/out/tables';
        if (!is_dir($outTablesDir)) {
            mkdir($outTablesDir, 0777, true);
        }
        return new CsvFile($this->dataDir . '/out/tables/' . $outputTable . '.csv');
    }

    protected function createManifest($table)
    {
        $outFilename = $this->dataDir . '/out/tables/' . $table['outputTable'] . '.csv.manifest';

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
        return file_put_contents($outFilename, Yaml::dump($manifestData));
    }
}
