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
use Symfony\Component\Yaml\Yaml;

abstract class Extractor
{
    /** @var \PDO */
    protected $db;

    protected $logger;

    protected $dataDir;

    public function __construct($config, Logger $logger)
    {
        $this->logger = $logger;
        $this->dataDir = $config['dataDir'];

        try {
            $this->db = $this->createConnection($config['parameters']['db']);
        } catch (\Exception $e) {
            if (strstr(strtolower($e->getMessage()), 'could not find driver')) {
                throw new ApplicationException("Missing driver: " . $e->getMessage());
            }
            throw new UserException("Error connecting to DB: " . $e->getMessage(), 0, $e);
        }
    }

    public abstract function createConnection($params);

    public function export(array $table)
    {
        //@todo check table attributes
        $outputTable = $table['outputTable'];
        $query = $table['query'];

        $this->logger->info("Exporting to " . $outputTable);

        $csv = $this->createOutputCsv($outputTable);

        try {
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

        } catch (\PDOException $e) {
            throw new UserException("DB query failed: " . $e->getMessage(), 0, $e);
        }

        if ($this->createManifest($table) === false) {
            throw new ApplicationException("Unable to create manifest", 0, null, [
                'table' => $table
            ]);
        }

        return $outputTable;
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