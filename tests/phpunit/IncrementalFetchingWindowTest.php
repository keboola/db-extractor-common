<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Tests\Fixtures\IncrementalFetchingWindow\FakeExtractorWithoutWindowSupport;
use Keboola\DbExtractor\Tests\Fixtures\IncrementalFetchingWindow\FakeExtractorWithWindowSupport;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests (no live DB) for the incremental fetching WINDOW + watermark LOOKBACK hooks + guards
 * added to BaseExtractor:
 * - getIncrementalFetchingColumnType() (opt-in hook, default null)
 * - export() threading the resolved column type into the ExportConfig handed to the adapter
 * - guardIncrementalFetchingOverlap() (primary-key guard for lookback/window-start + absolute-end warning)
 */
class IncrementalFetchingWindowTest extends TestCase
{
    private string $dataDir;

    protected function setUp(): void
    {
        parent::setUp();
        $this->dataDir = sys_get_temp_dir() . '/db-extractor-common-window-test-' . uniqid();
        mkdir($this->dataDir . '/out/tables', 0777, true);
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->removeDirectory($this->dataDir);
    }

    private function removeDirectory(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }
        $items = scandir($dir);
        if ($items === false) {
            return;
        }
        foreach ($items as $item) {
            if ($item === '.' || $item === '..') {
                continue;
            }
            $path = $dir . '/' . $item;
            if (is_dir($path)) {
                $this->removeDirectory($path);
            } else {
                unlink($path);
            }
        }
        rmdir($dir);
    }

    private function createExtractorParameters(): array
    {
        return [
            'data_dir' => $this->dataDir,
            'extractor_class' => 'Common',
            'db' => [
                'host' => 'localhost',
                'user' => 'root',
                '#password' => 'test',
            ],
        ];
    }

    private function buildExportConfig(array $overrides = []): ExportConfig
    {
        $data = array_merge(
            [
                'table' => ['tableName' => 'my_table', 'schema' => 'public'],
                'incremental' => false,
                'incrementalFetchingColumn' => 'updated_at',
                'columns' => [],
                'outputTable' => 'in.c-main.my_table',
                'primaryKey' => [],
                'retries' => 5,
            ],
            $overrides,
        );

        return ExportConfig::fromArray($data);
    }

    private function assertColumnTypeNotThreaded(ExportConfig $exportConfig): void
    {
        try {
            $exportConfig->getIncrementalColumnType();
            self::fail('Expected PropertyNotSetException: column type must not be threaded here.');
        } catch (PropertyNotSetException) {
            // expected: withIncrementalColumnType() was never called
        }
    }

    // --- Extractor NOT opted in (no override of getIncrementalFetchingColumnType) ---

    public function testExtractorWithoutWindowSupportBehavesUnchangedWhenNoWindowIsSet(): void
    {
        $extractor = new FakeExtractorWithoutWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test'),
        );
        $exportConfig = $this->buildExportConfig();

        $result = $extractor->export($exportConfig);

        self::assertSame('in.c-main.my_table', $result['outputTable']);
        self::assertSame(1, $result['rows']);

        $captured = $extractor->getCapturedExportConfig();
        self::assertNotNull($captured);
        self::assertTrue($captured->isIncrementalFetching());
        self::assertFalse($captured->hasIncrementalFetchingWindow());
        $this->assertColumnTypeNotThreaded($captured);
    }

    public function testExtractorWithoutWindowSupportRejectsWindowConfig(): void
    {
        $extractor = new FakeExtractorWithoutWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test'),
        );
        $exportConfig = $this->buildExportConfig([
            'incrementalFetchingMode' => 'window',
            'incrementalFetchingStart' => '20 minutes ago',
        ]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Incremental fetching window/lookback is not supported by this extractor.');
        $extractor->export($exportConfig);
    }

    // --- Extractor opted in (overrides getIncrementalFetchingColumnType) ---

    public function testExtractorWithWindowSupportDoesNotThreadTypeWhenNoWindowIsSet(): void
    {
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test'),
        );
        $exportConfig = $this->buildExportConfig();

        $extractor->export($exportConfig);

        $captured = $extractor->getCapturedExportConfig();
        self::assertNotNull($captured);
        self::assertFalse($captured->hasIncrementalFetchingWindow());
        $this->assertColumnTypeNotThreaded($captured);
    }

    public function testExtractorWithWindowSupportThreadsColumnTypeIntoAdapter(): void
    {
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test'),
        );
        $exportConfig = $this->buildExportConfig([
            'incrementalFetchingMode' => 'window',
            'incrementalFetchingStart' => '20 minutes ago',
            'incrementalFetchingEnd' => 'now',
        ]);

        $extractor->export($exportConfig);

        $captured = $extractor->getCapturedExportConfig();
        self::assertNotNull($captured);
        self::assertTrue($captured->hasIncrementalFetchingWindow());
        self::assertSame('TIMESTAMP', $captured->getIncrementalColumnType());
    }

    public function testWindowModeWithoutBoundsThrows(): void
    {
        // Window mode ignores the watermark; with no start/end it would be a full-table scan. Reject it.
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test'),
        );
        $exportConfig = $this->buildExportConfig([
            'incrementalFetchingMode' => 'window',
        ]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('"window" mode requires at least one of "incrementalFetchingStart"');
        $extractor->export($exportConfig);
    }

    // --- Primary-key guard: window "start" + incremental loading + no PK => hard error ---

    public function testGuardThrowsWhenWindowStartWithIncrementalLoadingAndNoPrimaryKey(): void
    {
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test'),
        );
        $exportConfig = $this->buildExportConfig([
            'incremental' => true,
            'primaryKey' => [],
            'incrementalFetchingMode' => 'window',
            'incrementalFetchingStart' => '20 minutes ago',
        ]);

        $this->expectException(UserException::class);
        $extractor->export($exportConfig);
    }

    public function testGuardAllowsWindowStartWithIncrementalLoadingWhenPrimaryKeySet(): void
    {
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test'),
        );
        $exportConfig = $this->buildExportConfig([
            'incremental' => true,
            'primaryKey' => ['id'],
            'incrementalFetchingMode' => 'window',
            'incrementalFetchingStart' => '20 minutes ago',
        ]);

        $result = $extractor->export($exportConfig);

        self::assertSame('in.c-main.my_table', $result['outputTable']);
    }

    public function testGuardAllowsWindowStartWhenIncrementalLoadingDisabled(): void
    {
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test'),
        );
        $exportConfig = $this->buildExportConfig([
            'incremental' => false,
            'primaryKey' => [],
            'incrementalFetchingMode' => 'window',
            'incrementalFetchingStart' => '20 minutes ago',
        ]);

        $result = $extractor->export($exportConfig);

        self::assertSame('in.c-main.my_table', $result['outputTable']);
    }

    public function testGuardIsNoOpWhenNoWindowConfiguredEvenWithoutPrimaryKey(): void
    {
        $handler = new TestHandler();
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test', [$handler]),
        );
        $exportConfig = $this->buildExportConfig([
            'incremental' => true,
            'primaryKey' => [],
        ])->withIncrementalColumnType('TIMESTAMP');

        // Must not throw: no window/lookback at all, so the PK guard never engages.
        $extractor->callGuardIncrementalFetchingOverlap($exportConfig);

        self::assertFalse($handler->hasWarningRecords());
    }

    // --- Absolute-end warning ---

    public function testAbsoluteWindowEndLogsWarning(): void
    {
        $handler = new TestHandler();
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test', [$handler]),
        );
        $exportConfig = $this->buildExportConfig([
            'incrementalFetchingMode' => 'window',
            'incrementalFetchingEnd' => '2026-01-01',
        ]);

        $extractor->export($exportConfig);

        self::assertTrue($handler->hasWarningThatContains('absolute value'));
    }

    public function testRelativeWindowEndDoesNotLogWarning(): void
    {
        $handler = new TestHandler();
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test', [$handler]),
        );
        $exportConfig = $this->buildExportConfig([
            'incrementalFetchingMode' => 'window',
            'incrementalFetchingEnd' => 'now',
        ]);

        $extractor->export($exportConfig);

        self::assertFalse($handler->hasWarningRecords());
    }

    public function testAbsoluteNumericWindowEndLogsWarning(): void
    {
        $handler = new TestHandler();
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test', [$handler]),
            'INTEGER',
        );
        $exportConfig = $this->buildExportConfig([
            'incrementalFetchingColumn' => 'id',
            'incrementalFetchingMode' => 'window',
            'incrementalFetchingEnd' => '50000',
        ]);

        $extractor->export($exportConfig);

        self::assertTrue($handler->hasWarningRecords());
    }

    public function testNoWindowEndDoesNotLogWarning(): void
    {
        $handler = new TestHandler();
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test', [$handler]),
        );
        $exportConfig = $this->buildExportConfig([
            'incrementalFetchingMode' => 'window',
            'incrementalFetchingStart' => '20 minutes ago',
        ]);

        $extractor->export($exportConfig);

        self::assertFalse($handler->hasWarningRecords());
    }

    // --- Watermark-mode lookback: threading + overlap guard ---

    public function testExtractorWithWindowSupportThreadsColumnTypeForLookback(): void
    {
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test'),
        );
        $exportConfig = $this->buildExportConfig(['incrementalFetchingLookback' => '20 minutes']);

        $extractor->export($exportConfig);

        $captured = $extractor->getCapturedExportConfig();
        self::assertNotNull($captured);
        self::assertFalse($captured->hasIncrementalFetchingWindow());
        self::assertTrue($captured->hasIncrementalFetchingLookback());
        self::assertSame('TIMESTAMP', $captured->getIncrementalColumnType());
    }

    public function testGuardThrowsWhenLookbackWithIncrementalLoadingAndNoPrimaryKey(): void
    {
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test'),
        );
        $exportConfig = $this->buildExportConfig([
            'incremental' => true,
            'primaryKey' => [],
            'incrementalFetchingLookback' => '20 minutes',
        ]);

        $this->expectException(UserException::class);
        $extractor->export($exportConfig);
    }

    public function testGuardAllowsLookbackWithIncrementalLoadingWhenPrimaryKeySet(): void
    {
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test'),
        );
        $exportConfig = $this->buildExportConfig([
            'incremental' => true,
            'primaryKey' => ['id'],
            'incrementalFetchingLookback' => '20 minutes',
        ]);

        $result = $extractor->export($exportConfig);

        self::assertSame('in.c-main.my_table', $result['outputTable']);
    }

    public function testGuardThrowsWhenLookbackCombinedWithLimit(): void
    {
        // Lookback + limit would persist an older row as the watermark and move it backwards; reject it.
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test'),
        );
        $exportConfig = $this->buildExportConfig([
            'incrementalFetchingLookback' => '20 minutes',
            'incrementalFetchingLimit' => 100,
        ]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('lookback cannot be combined with "incrementalFetchingLimit"');
        $extractor->export($exportConfig);
    }

    public function testLookbackDoesNotLogAbsoluteEndWarning(): void
    {
        $handler = new TestHandler();
        $extractor = new FakeExtractorWithWindowSupport(
            $this->createExtractorParameters(),
            [],
            new Logger('test', [$handler]),
        );
        $exportConfig = $this->buildExportConfig(['incrementalFetchingLookback' => '20 minutes']);

        $extractor->export($exportConfig);

        // The absolute-end warning is a window-mode concern only.
        self::assertFalse($handler->hasWarningRecords());
    }
}
