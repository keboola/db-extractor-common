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
}
