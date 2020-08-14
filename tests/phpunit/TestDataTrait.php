<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use PDO;
use Keboola\Csv\CsvWriter;
use Keboola\Temp\Temp;
use Keboola\DbExtractor\Test\DataLoader;
use Symfony\Component\Process\Process;

trait TestDataTrait
{
    protected PDO $db;

    protected Temp $temp;

    protected function closeSshTunnels(): void
    {
        # Close SSH tunnel if created
        $process = new Process(['sh', '-c', 'pgrep ssh | xargs -r kill']);
        $process->mustRun();
    }

    protected function initDatabase(): void
    {
        $dataLoader = $this->createDataLoader();
        $dataLoader->getPdo()->exec(
            sprintf(
                'DROP DATABASE IF EXISTS `%s`',
                (string) getEnv('COMMON_DB_DATABASE')
            )
        );
        $dataLoader->getPdo()->exec(
            sprintf(
                '
                    CREATE DATABASE `%s`
                    DEFAULT CHARACTER SET utf8
                    DEFAULT COLLATE utf8_general_ci
                ',
                (string) getEnv('COMMON_DB_DATABASE')
            )
        );

        $dataLoader->getPdo()->exec('USE ' . (string) getEnv('COMMON_DB_DATABASE'));

        $dataLoader->getPdo()->exec('SET NAMES utf8;');
        $dataLoader->getPdo()->exec(
            'CREATE TABLE escapingPK (
                                    col1 VARCHAR(155),
                                    col2 VARCHAR(155),
                                    PRIMARY KEY (col1, col2))'
        );

        $dataLoader->getPdo()->exec(
            "CREATE TABLE escaping (
                                  col1 VARCHAR(155) NOT NULL DEFAULT 'abc',
                                  col2 VARCHAR(155) NOT NULL DEFAULT 'abc',
                                  FOREIGN KEY (col1, col2) REFERENCES escapingPK(col1, col2))"
        );

        $dataLoader->getPdo()->exec(
            "CREATE TABLE simple (
                                  `_weird-I-d` VARCHAR(155) NOT NULL DEFAULT 'abc',
                                  `SãoPaulo` VARCHAR(155) NOT NULL DEFAULT 'abc',
                                  PRIMARY KEY (`_weird-I-d`))"
        );

        $inputFile = __DIR__ . '/data' . '/escaping.csv';
        $simpleFile = __DIR__ . '/data' . '/simple.csv';
        $dataLoader->load($inputFile, 'escapingPK');
        $dataLoader->load($inputFile, 'escaping');
        $dataLoader->load($simpleFile, 'simple', 0);

        // let other methods use the db connection
        $this->db = $dataLoader->getPdo();
    }

    protected function createLargeTable(int $rowCount = 5000000, string $tableName = 'large_test_table'): void
    {
        // Create CSV file with data
        $csvFileName = $this->temp->getTmpFolder() . '/' . $tableName . '.csv';
        $csv = new CsvWriter($csvFileName);
        $header = ['uuid', 'name', 'number', 'text'];
        $csv->writeRow($header);
        for ($i = 0; $i < $rowCount; $i++) {
            $csv->writeRow(
                [uniqid('g'), 'The Lakes', '1', 'RbaXcGgFJ9mox3wLBcM88g7mQ3QGCuA4fe2oPAPD']
            );
        }

        // Create table
        $dataLoader = $this->createDataLoader();
        $dataLoader->getPdo()->exec(sprintf(
            'CREATE TABLE %s (`uuid` VARCHAR(255) NOT NULL, `name` VARCHAR(255), `int` INT, `uuid2` VARCHAR(255));',
            $tableName
        ));

        // Load data to table
        $dataLoader->load($csvFileName, $tableName, 1);
        unlink($csvFileName);
    }

    protected function createDataLoader(): DataLoader
    {
        return new DataLoader(
            (string) getEnv('COMMON_DB_HOST'),
            (string) getEnv('COMMON_DB_PORT'),
            (string) getEnv('COMMON_DB_DATABASE'),
            (string) getEnv('COMMON_DB_USER'),
            (string) getEnv('COMMON_DB_PASSWORD')
        );
    }
}
