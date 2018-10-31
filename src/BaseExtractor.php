<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon;

use Keboola\Component\BaseComponent;
use Keboola\Component\JsonHelper;
use Keboola\Component\Logger;
use Keboola\Component\Manifest\ManifestManager\Options\OutTableManifestOptions;
use Keboola\Component\UserException;
use Keboola\Csv\CsvWriter;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractorCommon\Configuration\Definition\ActionConfigDefinition;
use Keboola\DbExtractorCommon\Configuration\BaseExtractorConfig;
use Keboola\DbExtractorCommon\Configuration\Definition\ConfigDefinition;
use Keboola\DbExtractorCommon\Configuration\Definition\ConfigRowDefinition;
use Keboola\DbExtractorCommon\Configuration\SshParametersInterface;
use Keboola\DbExtractorCommon\Configuration\TableDetailParameters;
use Keboola\DbExtractorCommon\Configuration\TableParameters;
use Keboola\DbExtractorCommon\Exception\DeadConnectionException;
use Keboola\SSHTunnel\SSH;
use Keboola\SSHTunnel\SSHException;
use Monolog\Handler\NullHandler;
use Nette\Utils\Strings;
use Symfony\Component\Config\Definition\Exception\Exception as ConfigException;

abstract class BaseExtractor extends BaseComponent
{
    public const DATATYPE_KEYS = ['type', 'length', 'nullable', 'default', 'format'];

    public function __construct(Logger $logger)
    {
        parent::__construct($logger);
        if ($this->getConfig()->getAction() !== 'run') {
            $logger->setHandlers([new NullHandler(Logger::INFO)]);
        }
    }

    abstract public function extract(BaseExtractorConfig $config): array;

    /**
     * @param TableDetailParameters[] $tables
     *
     * @return array
     */
    abstract public function getTables(array $tables = []): array;

    abstract protected function testConnection(): void;

    public function run(): void
    {
        /** @var BaseExtractorConfig $config */
        $config = $this->getConfig();
        $sshParameters = $config->getDbParameters()->getSsh();
        $action = $config->getAction();

        if ($sshParameters
            && $sshParameters->isEnabled()
        ) {
            $this->createSshTunnel($sshParameters);
        }

        try {
            switch ($action) {
                case 'run':
                    $this->validateParameters($config);
                    $result = $this->extract($config);
                    if (!empty($result['state'])) {
                        $this->writeOutputStateToFile($result['state']);
                    }
                    break;
                case 'testConnection':
                    $this->checkConnectionIsAlive();
                    $result = ['status' => 'success'];
                    break;
                case 'getTables':
                    $result = [
                        'tables' => $this->getTables(),
                        'status' => 'success',
                    ];
                    break;
                default:
                    throw new UserException(sprintf('Undefined action "%s".', $action));
            }
        } catch (\Throwable $exception) {
            throw $exception;
        }

        print JsonHelper::encode($result);
    }

    public function validateParameters(BaseExtractorConfig $config): void
    {
        try {
            if ($config->isConfigRow()) {
                $this->validateTableParameters(TableParameters::fromRaw($config->getParameters()));
            } else {
                foreach ($config->getTables() as $table) {
                    $this->validateTableParameters($table);
                }
            }
        } catch (ConfigException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    protected function createManifest(TableParameters $table): void
    {
        $tableManifestOptions = new OutTableManifestOptions();
        $tableManifestOptions->setDestination($table->getOutputTable());
        $tableManifestOptions->setIncremental($table->isIncremental());

        if ($table->getPrimaryKey()) {
            $tableManifestOptions->setPrimaryKeyColumns($table->getPrimaryKey());
        }

        $manifestColumns = [];

        if ($table->getTableDetail()) {
            $tables = $this->getTables([$table->getTableDetail()]);
            if (count($tables) > 0) {
                $tableDetails = $tables[0];
                $columnMetadata = [];
                $sanitizedPks = [];
                $iterColumns = $table->getColumns();
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
                    if (in_array($column['name'], $table->getPrimaryKey())
                        && array_key_exists('sanitizedName', $column)
                    ) {
                        $sanitizedPks[] = $column['sanitizedName'];
                    }
                    $columnName = $column['name'];
                    if (array_key_exists('sanitizedName', $column)) {
                        $columnName = $column['sanitizedName'];
                    }
                    $columnMetadata[$columnName] = self::getColumnMetadata($column);
                    $manifestColumns[] = $columnName;
                }
                $tableManifestOptions->setMetadata($this->getTableLevelMetadata($tableDetails));
                $tableManifestOptions->setColumnMetadata($columnMetadata);
                $tableManifestOptions->setColumns($manifestColumns);

                if (!empty($sanitizedPks)) {
                    $tableManifestOptions->setPrimaryKeyColumns($sanitizedPks);
                }
            }
        }

        $this->getManifestManager()->writeTableManifest(
            $this->getOutputFileName($table->getOutputTable()),
            $tableManifestOptions
        );
    }

    protected function createOutputCsv(string $outputTable): CsvWriter
    {
        return new CsvWriter($this->getOutputFilePath($outputTable));
    }

    protected function createSshTunnel(SshParametersInterface $sshParameters): void
    {
        $tunnelParams = $sshParameters->toArray();
        $this->getLogger()->info("Creating SSH tunnel to '" . $tunnelParams['sshHost'] . "'");
        $proxy = new RetryProxy(
            $this->getLogger(),
            RetryProxy::DEFAULT_MAX_TRIES,
            RetryProxy::DEFAULT_BACKOFF_INTERVAL,
            ['SSHException', 'Exception']
        );
        try {
            $proxy->call(function () use ($tunnelParams):void {
                $ssh = new SSH();
                $ssh->openTunnel($tunnelParams);
            });
        } catch (SSHException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    protected function getConfigDefinitionClass(): string
    {
        $configRaw = $this->getRawConfig();
        $action = $configRaw['action'];
        $isConfigRow = !isset($configRaw['parameters']['tables']);

        if ($action !== 'run') {
            return ActionConfigDefinition::class;
        } elseif ($isConfigRow) {
            return ConfigRowDefinition::class;
        } else {
            return ConfigDefinition::class;
        }
    }

    protected function getOutputFileName(string $outputTableName): string
    {
        $sanitizedTableName = Strings::webalize($outputTableName, '._');
        return $sanitizedTableName . '.csv';
    }

    protected function getOutputFilePath(string $outputTableName): string
    {
        return $this->getDataDir() . '/out/tables/' . $this->getOutputFileName($outputTableName);
    }

    protected function checkConnectionIsAlive(): void
    {
        try {
            $this->testConnection();
        } catch (\Throwable $e) {
            throw new DeadConnectionException("Dead connection: " . $e->getMessage());
        }
    }

    protected function validateIncrementalFetching(
        TableDetailParameters $table,
        string $columnName,
        ?int $limit = null
    ): void {
        throw new UserException('Incremental Fetching is not supported by this extractor.');
    }

    protected function quoteIdentifier(string $obj): string
    {
        return "\"{$obj}\"";
    }

    protected function quoteIdentifiers(array $identifiers): array
    {
        $quotedIdentifiers = [];
        foreach ($identifiers as $identifier) {
            $quotedIdentifiers[] = $this->quoteIdentifier($identifier);
        }
        return $quotedIdentifiers;
    }

    protected function getConfigClass(): string
    {
        return BaseExtractorConfig::class;
    }

    protected static function getDataTypeMetadata(array $column): array
    {
        $dataType = new GenericStorage(
            $column['type'],
            array_intersect_key($column, array_flip(self::DATATYPE_KEYS))
        );
        return $dataType->toMetadata();
    }

    public static function getColumnMetadata(array $column): array
    {
        $columnMetadata = self::getDataTypeMetadata($column);
        $nonDatatypeKeys = array_diff_key($column, array_flip(self::DATATYPE_KEYS));
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

    public static function getTableLevelMetadata(array $tableDetails): array
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

    private function validateTableParameters(TableParameters $table): void
    {
        if ($table->getIncrementalFetchingColumn()) {
            $this->validateIncrementalFetching(
                $table->getTableDetail(),
                $table->getIncrementalFetchingColumn(),
                $table->getIncrementalFetchingLimit()
            );
        }
    }
}
