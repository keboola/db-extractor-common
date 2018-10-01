<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Component\BaseComponent;
use Keboola\Component\UserException;
use Keboola\Csv\CsvWriter;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Configuration\ActionConfigDefinition;
use Keboola\DbExtractor\Configuration\BaseExtractorConfig;
use Keboola\DbExtractor\Configuration\ConfigDefinition;
use Keboola\DbExtractor\Configuration\ConfigRowDefinition;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Nette\Utils\Strings;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\Exception as ConfigException;

abstract class BaseExtractor extends BaseComponent
{
    public const DEFAULT_MAX_TRIES = 5;

    /** @var string */
    private $action;

    /** @var bool */
    private $isConfigRow;

    /** @var array */
    protected $dbParameters;

    /** @var array */
    protected $state;

    public function __construct(LoggerInterface $logger, string $action, array $state, bool $isConfigRow)
    {
        $this->action = $action;
        $this->isConfigRow = $isConfigRow;
        parent::__construct($logger);

        $this->state = $state;
    }

    abstract public function extract(array $tables): array;

    abstract public function getTables(array $tables = []): array;

    abstract public function testConnection(): void;

    public function isConfigRow(): bool
    {
        return $this->isConfigRow;
    }

    public function validateParameters(array $parameters): void
    {
        try {
            if (!$this->isConfigRow()) {
                foreach ($parameters['tables'] as $table) {
                    $this->validateTableParameters($table);
                }
            } else {
                $this->validateTableParameters($parameters);
            }
        } catch (ConfigException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    protected function createManifest(array $table): void
    {
        $outFilename = $this->getOutputFilename($table['outputTable']) . '.manifest';

        $manifestData = [
            'destination' => $table['outputTable'],
            'incremental' => $table['incremental'],
        ];

        if (!empty($table['primaryKey'])) {
            $manifestData['primary_key'] = $table['primaryKey'];
        }

        $manifestColumns = [];

        if (isset($table['table']) && !is_null($table['table'])) {
            $tables = $this->getTables([$table['table']]);
            if (count($tables) > 0) {
                $tableDetails = $tables[0];
                $columnMetadata = [];
                $sanitizedPks = [];
                $iterColumns = $table['columns'];
                if (count($iterColumns) === 0) {
                    $iterColumns = array_map(function ($column) {
                        return $column['name'];
                    }, $tableDetails['columns']);
                }
                foreach ($iterColumns as $ind => $columnName) {
                    $column = null;
                    foreach ($tableDetails['columns'] as $detailColumn) {
                        if ($detailColumn['name'] === $columnName) {
                            $column = $detailColumn;
                        }
                    }
                    if (!$column) {
                        throw new UserException(
                            sprintf("The given column '%s' was not found in the table.", $columnName)
                        );
                    }
                    // use sanitized name for primary key if available
                    if (in_array($column['name'], $table['primaryKey']) && array_key_exists('sanitizedName', $column)) {
                        $sanitizedPks[] = $column['sanitizedName'];
                    }
                    $columnName = $column['name'];
                    if (array_key_exists('sanitizedName', $column)) {
                        $columnName = $column['sanitizedName'];
                    }
                    $columnMetadata[$columnName] = $this->getColumnMetadata($column);
                    $manifestColumns[] = $columnName;
                }
                $manifestData['metadata'] = $this->getTableLevelMetadata($tableDetails);

                $manifestData['column_metadata'] = $columnMetadata;
                $manifestData['columns'] = $manifestColumns;
                if (!empty($sanitizedPks)) {
                    $manifestData['primary_key'] = $sanitizedPks;
                }
            }
        }
        file_put_contents($outFilename, json_encode($manifestData));
    }

    protected function createOutputCsv(string $outputTable): CsvWriter
    {
        $outTablesDir = $this->getDataDir() . '/out/tables';
        if (!is_dir($outTablesDir)) {
            mkdir($outTablesDir, 0777, true);
        }
        return new CsvWriter($this->getOutputFilename($outputTable));
    }

    protected function getOutputFilename(string $outputTableName): string
    {
        $sanitizedTableName = Strings::webalize($outputTableName, '._');
        return $this->getDataDir() . '/out/tables/' . $sanitizedTableName . '.csv';
    }

    protected function isAlive(): void
    {
        try {
            $this->testConnection();
        } catch (\Throwable $e) {
            throw new DeadConnectionException("Dead connection: " . $e->getMessage());
        }
    }

    protected function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        throw new UserException('Incremental Fetching is not supported by this extractor.');
    }

    protected function quote(string $obj): string
    {
        return "`{$obj}`";
    }

    protected function getConfigDefinitionClass(): string
    {
        if ($this->action !== 'run') {
            return ActionConfigDefinition::class;
        } elseif (!$this->isConfigRow()) {
            return ConfigDefinition::class;
        } else {
            return ConfigRowDefinition::class;
        }
    }

    protected function getConfigClass(): string
    {
        return BaseExtractorConfig::class;
    }

    private function getColumnMetadata(array $column): array
    {
        $datatypeKeys = ['type', 'length', 'nullable', 'default', 'format'];
        $datatype = new GenericStorage(
            $column['type'],
            array_intersect_key($column, array_flip($datatypeKeys))
        );
        $columnMetadata = $datatype->toMetadata();
        $nonDatatypeKeys = array_diff_key($column, array_flip($datatypeKeys));
        foreach ($nonDatatypeKeys as $key => $value) {
            if ($key === 'name') {
                $columnMetadata[] = [
                    'key' => "KBC.sourceName",
                    'value' => $value,
                ];
            } else {
                $columnMetadata[] = [
                    'key' => "KBC." . $key,
                    'value' => $value,
                ];
            }
        }
        return $columnMetadata;
    }

    private function getTableLevelMetadata(array $tableDetails): array
    {
        $metadata = [];
        foreach ($tableDetails as $key => $value) {
            if ($key === 'columns') {
                continue;
            }
            $metadata[] = [
                "key" => "KBC." . $key,
                "value" => $value,
            ];
        }
        return $metadata;
    }

    private function validateTableParameters(array $table): void
    {
        if (isset($table['incrementalFetchingColumn'])
            && $table['incrementalFetchingColumn'] !== "") {
            $this->validateIncrementalFetching(
                $table['table'],
                $table['incrementalFetchingColumn'],
                $table['incrementalFetchingLimit']?? null
            );
        }

        if (isset($table['incrementalFetching']['autoIncrementColumn']) && empty($table['primaryKey'])) {
            $this->getLogger()->warning(
                "An import autoIncrement column is being used but output primary key is not set."
            );
        }
    }
}
