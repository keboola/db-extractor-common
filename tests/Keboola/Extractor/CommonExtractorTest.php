<?php

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Test\ExtractorTest;

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
        $this->runApp(new Application($this->getConfig()));
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
            'sshHost' => 'sshproxy',
            'localPort' => '33306',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
        ];
        $this->runApp(new Application($config));
    }

    public function testRunWithWrongCredentials()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['host'] = 'somebulshit';
        $config['parameters']['db']['#password'] = 'somecrap';

        try {
            $this->runApp(new Application($config));
            $this->fail("Wrong credentials must raise error.");
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
        }
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
            $this->runApp(new Application($config));
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

    /*
    public function testTestConnectionReconnect()
    {
        // TODO: kill connection to the server in the middle
        $config = $this->getConfig();
        $t = $config['parameters']['tables'][0];
        for ($i = 1; $i < 10; $i++) {
            $config['parameters']['tables'][] = $t;
        }
        $this->runApp(new Application($config));
    }
    */

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

    protected function runApp(Application $app)
    {
        $result = $app->run();
        $expectedCsvFile = ROOT_PATH . '/tests/data/escaping.csv';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
    }
}
