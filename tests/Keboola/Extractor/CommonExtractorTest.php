<?php

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Yaml\Yaml;

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 12/02/16
 * Time: 16:39
 */
class CommonExtractorTest extends ExtractorTest
{
    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-common');
        }

        $inputFile = ROOT_PATH . '/tests/data/escaping.csv';

        $dataLoader = new \Keboola\DbExtractor\Test\DataLoader(
            $this->getEnv('common', 'DB_HOST'),
            $this->getEnv('common', 'DB_PORT'),
            $this->getEnv('common', 'DB_DATABASE'),
            $this->getEnv('common', 'DB_USER'),
            $this->getEnv('common', 'DB_PASSWORD')
        );

        $dataLoader->getPdo()->exec(sprintf("DROP DATABASE IF EXISTS `%s`", $this->getEnv('common', 'DB_DATABASE')));
        $dataLoader->getPdo()->exec(sprintf("
            CREATE DATABASE `%s`
            DEFAULT CHARACTER SET utf8
            DEFAULT COLLATE utf8_general_ci
        ", $this->getEnv('common', 'DB_DATABASE')));

        $dataLoader->getPdo()->exec("USE " . $this->getEnv('common', 'DB_DATABASE'));

        $dataLoader->getPdo()->exec("SET NAMES utf8;");
        $dataLoader->getPdo()->exec("DROP TABLE IF EXISTS escaping");
        $dataLoader->getPdo()->exec("CREATE TABLE escaping (col1 VARCHAR(255) NOT NULL, col2 VARCHAR(255) NOT NULL)");
        $dataLoader->load($inputFile, 'escaping');
    }

    public function getConfig($driver = 'common')
    {
        $config = parent::getConfig($driver);
        $config['parameters']['extractor_class'] = 'Common';
        return $config;
    }

    public function testRun()
    {
        $this->assertRunResult((new Application($this->getConfig()))->run());
    }

    public function testRunWithSSH()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('common', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('common', 'DB_SSH_KEY_PUBLIC')
            ],
            'sshHost' => 'sshproxy'
        ];
        $this->assertRunResult((new Application($config))->run());
    }

    public function testRunWithSSHDeprecated()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('common', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('common', 'DB_SSH_KEY_PUBLIC')
            ],
            'sshHost' => 'sshproxy',
            'localPort' => '33306',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
        ];

        $result = (new Application($config))->run();
        $this->assertRunResult($result);
    }

    public function testRunWithSSHUserException()
    {
        $this->setExpectedException('Keboola\DbExtractor\Exception\UserException');

        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('common', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('common', 'DB_SSH_KEY_PUBLIC')
            ],
            'sshHost' => 'wronghost',
            'localPort' => '33306',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
        ];

        (new Application($config))->run();
    }

    public function testRunWithWrongCredentials()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['host'] = 'somebulshit';
        $config['parameters']['db']['#password'] = 'somecrap';

        try {
            (new Application($config))->run();
            $this->fail("Wrong credentials must raise error.");
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
        }
    }

    public function testRunEmptyQuery()
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        @unlink($outputCsvFile);
        @unlink($outputManifestFile);

        $config = $this->getConfig();
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM escaping WHERE col1 = '123'";

        $result = (new Application($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertFileNotExists($outputCsvFile);
        $this->assertFileNotExists($outputManifestFile);
    }

    public function testTestConnection()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);
        $app = new Application($config);
        $res = $app->run();

        $this->assertEquals('success', $res['status']);
    }

    public function testTestConnectionFailInTheMiddle()
    {
        $config = $this->getConfig();
        $config['parameters']['tables'][] = [
            'id' => 10,
            'name' => 'bad',
            'query' => 'KILL CONNECTION_ID();',
            'outputTable' => 'dummy'
        ];
        try {
            (new Application($config))->run();
            $this->fail("Failing query must raise exception.");
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
        }
    }

    public function testTestConnectionFailure()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);
        $config['parameters']['db']['#password'] = 'bullshit';
        $app = new Application($config);
        $exceptionThrown = false;
        try {
            $app->run();
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
            $exceptionThrown = true;
        }

        $this->assertTrue($exceptionThrown);
    }

    public function testGetTablesAction()
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';
        $app = new Application($config);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);

        $this->assertEquals('success', $result['status']);
        $this->assertCount(1, $result['tables']);
        $this->assertArrayHasKey('name', $result['tables'][0]);
        $this->assertEquals("escaping", $result['tables'][0]['name']);
        $this->assertArrayHasKey('columns', $result['tables'][0]);
        $this->assertCount(2, $result['tables'][0]['columns']);
        $this->assertArrayHasKey('name', $result['tables'][0]['columns'][0]);
        $this->assertEquals("col1", $result['tables'][0]['columns'][0]['name']);
        $this->assertArrayHasKey('type', $result['tables'][0]['columns'][0]);
        $this->assertEquals("varchar", $result['tables'][0]['columns'][0]['type']);
        $this->assertArrayHasKey('length', $result['tables'][0]['columns'][0]);
        $this->assertEquals(255, $result['tables'][0]['columns'][0]['length']);
        $this->assertArrayHasKey('nullable', $result['tables'][0]['columns'][0]);
        $this->assertFalse($result['tables'][0]['columns'][0]['nullable']);
        $this->assertArrayHasKey('default', $result['tables'][0]['columns'][0]);
        $this->assertNull($result['tables'][0]['columns'][0]['default']);
        $this->assertArrayHasKey('primary', $result['tables'][0]['columns'][0]);
        $this->assertFalse($result['tables'][0]['columns'][0]['primary']);
    }

    public function testColumnMetadataManifest()
    {
        $config = $this->getConfig();
        $config['parameters']['tables'][0]['columns'] = ['col1', 'col2'];

        $app = new Application($config);

        $result = $app->run();

        $this->assertRunResult($result);

        $outputManifest = Yaml::parse(
            file_get_contents($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest')
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(2, $outputManifest['column_metadata']);
        $this->assertArrayHasKey('col1', $outputManifest['column_metadata']);
        $this->assertArrayHasKey('col2', $outputManifest['column_metadata']);
        foreach ($outputManifest['column_metadata']['col1'] as $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            switch ($metadata['key']) {
                case 'KBC.datatype.type':
                    $this->assertEquals('varchar', $metadata['value']);
                    break;
                case 'KBC.datatype.basetype':
                    $this->assertEquals('STRING', $metadata['value']);
                    break;
                case 'KBC.datatype.nullable':
                    $this->assertFalse($metadata['value']);
                    break;
                case 'KBC.datatype.default':
                    $this->assertNull($metadata['value']);
                    break;
                case 'KBC.datatype.length':
                    $this->assertEquals('255', $metadata['value']);
                    break;
                case 'KBC.datatype.primary':
                    $this->assertFalse($metadata['value']);
                    break;
                default:
                    $this->fail("Unnexpected metadata key " . $metadata['key']);
            }
        }
    }

    public function testNonExistingAction()
    {
        $config = $this->getConfig();
        $config['action'] = 'sample';
        unset($config['parameters']['tables']);

        try {
            $app = new Application($config);
            $app->run();

            $this->fail('Running non-existing actions should fail with UserException');
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
        }
    }

    protected function assertRunResult($result)
    {
        $expectedCsvFile = ROOT_PATH . '/tests/data/escaping.csv';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
    }
}
