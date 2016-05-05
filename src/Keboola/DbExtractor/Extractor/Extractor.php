<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 13:04
 */

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Keboola\DbExtractor\SSH;
use Symfony\Component\Yaml\Yaml;

abstract class Extractor
{
    /** @var \PDO */
    protected $db;

    protected $logger;

    protected $dataDir;

    public function __construct($parameters, Logger $logger)
    {
        $this->logger = $logger;
        $this->dataDir = $parameters['data_dir'];

        if (isset($parameters['db']['ssh']['enabled']) && $parameters['db']['ssh']['enabled']) {
            $parameters['db'] = $this->createSshTunnel($parameters['db']);
        }

        try {
            $this->db = $this->createConnection($parameters['db']);
        } catch (\Exception $e) {
            if (strstr(strtolower($e->getMessage()), 'could not find driver')) {
                throw new ApplicationException("Missing driver: " . $e->getMessage());
            }
            throw new UserException("Error connecting to DB: " . $e->getMessage(), 0, $e);
        }
    }

    protected function createSshTunnel($dbConfig)
    {
        $sshConfig = $dbConfig['ssh'];
        // check params
        foreach (['keys', 'sshHost'] as $k) {
            if (empty($sshConfig[$k])) {
                throw new UserException(sprintf("Parameter %s is missing.", $k));
            }
        }

        if (empty($sshConfig['user'])) {
            $sshConfig['user'] = $dbConfig['user'];
        }

        if (empty($sshConfig['localPort'])) {
            $sshConfig['localPort'] = 33006;
        }

        if (empty($sshConfig['remoteHost'])) {
            $sshConfig['remoteHost'] = $dbConfig['host'];
        }

        if (empty($sshConfig['remotePort'])) {
            $sshConfig['remotePort'] = $dbConfig['port'];
        }

        if (empty($sshConfig['sshPort'])) {
            $sshConfig['sshPort'] = 22;
        }

        $privateKey = isset($sshConfig['keys']['#private'])
            ?$sshConfig['keys']['#private']
            :$sshConfig['keys']['private'];

        $ssh = new SSH();
        $ssh->openTunnel(
            $sshConfig['user'],
            $sshConfig['sshHost'],
            $sshConfig['localPort'],
            $sshConfig['remoteHost'],
            $sshConfig['remotePort'],
            $privateKey,
            $sshConfig['sshPort']
        );

        $dbConfig['host'] = '127.0.0.1';
        $dbConfig['port'] = $sshConfig['localPort'];

        return $dbConfig;
    }

    public abstract function createConnection($params);

    public abstract function testConnection();

    public function export(array $table)
    {
        $outputTable = $table['outputTable'];
        $csv = $this->createOutputCsv($outputTable);

        $this->logger->info("Exporting to " . $outputTable);

        $query = $table['query'];

        $maxTries = (isset($table['retries']) && $table['retries'])?$table['retries']:1;
        $tries = 0;
        $exception = null;

        while ($tries < $maxTries) {
            $exception = null;
            try {
                $this->executeQuery($query, $csv);
            } catch (\PDOException $e) {
                $exception = new UserException("DB query failed: " . $e->getMessage(), 0, $e);
            }
            sleep(pow($tries, 2));
            $tries++;
        }

        if ($exception) {
            throw $exception;
        }

        if ($this->createManifest($table) === false) {
            throw new ApplicationException("Unable to create manifest", 0, null, [
                'table' => $table
            ]);
        }

        return $outputTable;
    }

    protected function executeQuery($query, CsvFile $csv)
    {
        $stmt = $this->db->prepare($query);
        $stmt->execute();
        $resultRow = $stmt->fetch(\PDO::FETCH_ASSOC);

        if (is_array($resultRow) && !empty($resultRow)) {
            // write header and first line
            $csv->writeRow(array_keys($resultRow));
            $csv->writeRow($resultRow);

            // write the rest
            while ($resultRow = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                $csv->writeRow($resultRow);
            }
        } else {
            $this->logger->warn("Query returned empty result. Nothing was imported.");
        }
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

        return file_put_contents($outFilename , Yaml::dump($manifestData));
    }
}
