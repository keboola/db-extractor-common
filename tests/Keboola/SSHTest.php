<?php
use Keboola\DbExtractor\Test\ExtractorTest;

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 17/02/16
 * Time: 16:06
 */
class SSHTest extends ExtractorTest
{
    public function testGenerateKeyPair()
    {
        $ssh = new \Keboola\DbExtractor\SSH();
        $keys = $ssh->generateKeyPair();

        $this->assertArrayHasKey('private', $keys);
        $this->assertArrayHasKey('public', $keys);
        $this->assertNotEmpty($keys['private']);
        $this->assertNotEmpty($keys['public']);
    }

    public function testOpenTunnel()
    {
        $ssh = new \Keboola\DbExtractor\SSH();
        $privateKey = $this->getEnv('common', 'DB_SSH_KEY_PRIVATE');
        $tunnelProcess = $ssh->openTunnel(
            'root',
            'sshproxy',
            '33306',
            'mysql',
            '3306',
            $privateKey
        );

        while($tunnelProcess->isRunning()) {
            sleep(1);
            $tunnelProcess->getOutput();
            $tunnelProcess->getErrorOutput();
        }

        $inputFile = ROOT_PATH . '/tests/data/escaping.csv';
        $dataLoader = new \Keboola\DbExtractor\Test\DataLoader('127.0.0.1', 33306, 'testdb', 'root', 'somePassword');
        $dataLoader->getPdo()->exec("SET NAMES utf8;");
        $dataLoader->getPdo()->exec("DROP TABLE IF EXISTS escaping");
        $dataLoader->getPdo()->exec("CREATE TABLE escaping (col1 VARCHAR(255) NOT NULL, col2 VARCHAR(255) NOT NULL) CHARSET=utf8");
        $dataLoader->load($inputFile, 'escaping');

        $stmt = $dataLoader->getPdo()->query("SELECT * FROM escaping");
        $res = $stmt->fetchAll();

        $this->assertEquals("line with enclosure", $res[0]['col1']);

        $tunnelProcess->stop();
    }
}
