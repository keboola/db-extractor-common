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
    /** @var Application */
    protected $app;

    public function setUp()
    {
        define('APP_NAME', 'ex-db-common');
        $this->app = new Application($this->getConfig('common'));
    }

    public function testRun()
    {
        $result = $this->app->run();
        $expectedCsvFile = ROOT_PATH . '/tests/data/escaping.csv';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('ok', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
    }

}
