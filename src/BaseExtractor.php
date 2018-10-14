<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon;

use Keboola\Component\BaseComponent;
use Keboola\Component\Config\BaseConfig;
use Keboola\Component\Logger;
use Keboola\Component\UserException;
use Keboola\Csv\CsvWriter;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractorCommon\Configuration\Definition\ActionConfigDefinition;
use Keboola\DbExtractorCommon\Configuration\BaseExtractorConfig;
use Keboola\DbExtractorCommon\Configuration\Definition\ConfigDefinition;
use Keboola\DbExtractorCommon\Configuration\Definition\ConfigRowDefinition;
use Keboola\DbExtractorCommon\Configuration\DatabaseParameters;
use Keboola\DbExtractorCommon\Configuration\SshParameters;
use Keboola\DbExtractorCommon\Configuration\TableDetailParameters;
use Keboola\DbExtractorCommon\Configuration\TableParameters;
use Keboola\DbExtractorCommon\Exception\DeadConnectionException;
use Keboola\SSHTunnel\SSH;
use Keboola\SSHTunnel\SSHException;
use Monolog\Handler\NullHandler;
use Nette\Utils\Strings;
use Symfony\Component\Config\Definition\Exception\Exception as ConfigException;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Serializer\Encoder\JsonEncode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;

abstract class BaseExtractor extends BaseComponent
{
    public const DEFAULT_MAX_TRIES = 5;

    public const DATATYPE_KEYS = ['type', 'length', 'nullable', 'default', 'format'];

    /** @var array */
    protected $dbParameters;

    /** @var array */
    protected $state;

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

    abstract public function testConnection(): void;

    public function run(): void
    {
        /** @var BaseExtractorConfig $config */
        $config = $this->getConfig();
        $dbParameters = $config->getDbParameters();
        $sshParameters = $config->getSshParameters();
        $action = $config->getAction();

        if ($sshParameters
            && $sshParameters->getEnabled()
        ) {
            $dbParameters = $this->createSshTunnel($sshParameters, $dbParameters);
            $config->setDbParameters($dbParameters);
        }

        try {
            switch ($action) {
                case 'run':
                    $this->validateParameters($config);
                    $result = $this->extract($config);
                    if (!empty($result['state'])) {
                        // write state
                        $outputStateFile = $this->getDataDir() . '/out/state.json';
                        $jsonEncode = new JsonEncode();
                        file_put_contents(
                            $outputStateFile,
                            $jsonEncode->encode($result['state'], JsonEncoder::FORMAT)
                        );
                    }
                    break;
                case 'testConnection':
                    $this->testConnection();
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

        print json_encode($result, JSON_PRETTY_PRINT);
    }

    public function setState(array $state = []): void
    {
        $this->state = $state;
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
        $outFilename = $this->getOutputFilename($table->getOutputTable()) . '.manifest';

        $manifestData = [
            'destination' => $table->getOutputTable(),
            'incremental' => $table->isIncremental(),
        ];

        if ($table->getPrimaryKey()) {
            $manifestData['primary_key'] = $table->getPrimaryKey();
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

    protected function createSshTunnel(SshParameters $sshParameters, DatabaseParameters $dbConfig): DatabaseParameters
    {
        $privateKey = $sshParameters->getKeys()['#private'] ?? $sshParameters->getKeys()['private'];
        $sshParameters->setPrivateKey($privateKey);
        $sshParameters->setRemoteHost($dbConfig->getHost());
        $sshParameters->setRemotePort($dbConfig->getPort());

        if (!$sshParameters->getUser()) {
            $sshParameters->setUser($dbConfig->getUser());
        }

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

        $dbConfig->setHost('127.0.0.1');
        $dbConfig->setPort($sshParameters->getLocalPort());
        return $dbConfig;
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

    protected function loadConfig(): void
    {
        $configRaw = $this->getRawConfig();
        $configClass = $this->getConfigClass();
        $configDefinitionClass = $this->getConfigDefinition(
            $configRaw['action'],
            !isset($configRaw['parameters']['tables'])
        );

        try {
            /** @var BaseConfig $config */
            $config = new $configClass(
                $configRaw,
                new $configDefinitionClass()
            );
            $this->setConfig($config);
        } catch (InvalidConfigurationException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
        $this->getLogger()->debug('Config loaded');
    }

    protected function validateIncrementalFetching(
        TableDetailParameters $table,
        string $columnName,
        ?int $limit = null
    ): void {
        throw new UserException('Incremental Fetching is not supported by this extractor.');
    }

    protected function quote(string $obj): string
    {
        return "`{$obj}`";
    }

    protected function getConfigDefinition(string $action, bool $isConfigRow): string
    {
        if ($action !== 'run') {
            return ActionConfigDefinition::class;
        } elseif ($isConfigRow) {
            return ConfigRowDefinition::class;
        } else {
            return ConfigDefinition::class;
        }
    }

    protected function getConfigClass(): string
    {
        return BaseExtractorConfig::class;
    }

    private function getColumnMetadata(array $column): array
    {
        $datatype = new GenericStorage(
            $column['type'],
            array_intersect_key($column, array_flip(self::DATATYPE_KEYS))
        );
        $columnMetadata = $datatype->toMetadata();
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

    private function validateTableParameters(TableParameters $table): void
    {
        if ($table->getIncrementalFetchingColumn()
            && $table->getIncrementalFetchingColumn() !== "") {
            $this->validateIncrementalFetching(
                $table->getTableDetail(),
                $table->getIncrementalFetchingColumn(),
                $table->getIncrementalFetchingLimit()
            );
        }
    }
}
