<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Component\BaseComponent;
use Keboola\Component\Config\BaseConfig;
use Keboola\Component\UserException;
use Keboola\Csv\CsvWriter;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Configuration\ActionConfigDefinition;
use Keboola\DbExtractor\Configuration\BaseExtractorConfig;
use Keboola\DbExtractor\Configuration\ConfigDefinition;
use Keboola\DbExtractor\Configuration\ConfigRowDefinition;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\RetryProxy;
use Keboola\SSHTunnel\SSH;
use Keboola\SSHTunnel\SSHException;
use Nette\Utils\Strings;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\Exception as ConfigException;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

abstract class BaseExtractor extends BaseComponent
{
    public const DEFAULT_MAX_TRIES = 5;

    public const DATATYPE_KEYS = ['type', 'length', 'nullable', 'default', 'format'];

    /** @var array */
    protected $dbParameters;

    /** @var array */
    protected $state;

    abstract public function extract(array $tables): array;

    abstract public function getTables(array $tables = []): array;

    abstract public function testConnection(): void;

    public function run(): void
    {
        $action = $this->getConfig()->getAction();
        $parameters = $this->getConfig()->getParameters();

        if (isset($parameters['db']['ssh'])
            && isset($parameters['db']['ssh']['enabled'])
            && $parameters['db']['ssh']['enabled']
        ) {
            $parameters['db'] = $this->createSshTunnel($parameters['db']);
        }

        try {
            switch ($action) {
                case 'run':
                    $this->validateParameters($parameters);
                    $result = $this->extract($parameters); // @todo - save state into state file
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

    public function validateParameters(array $parameters): void
    {
        try {
            if (isset($parameters['tables'])) {
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

    protected function createSshTunnel(array $dbConfig): array
    {
        $sshConfig = $dbConfig['ssh'];
        // check params
        foreach (['keys', 'sshHost'] as $k) {
            if (empty($sshConfig[$k])) {
                throw new UserException(sprintf("Parameter '%s' is missing.", $k));
            }
        }

        $sshConfig['remoteHost'] = $dbConfig['host'];
        $sshConfig['remotePort'] = $dbConfig['port'];

        if (empty($sshConfig['user'])) {
            $sshConfig['user'] = $dbConfig['user'];
        }
        if (empty($sshConfig['localPort'])) {
            $sshConfig['localPort'] = 33006;
        }
        if (empty($sshConfig['sshPort'])) {
            $sshConfig['sshPort'] = 22;
        }
        $sshConfig['privateKey'] = isset($sshConfig['keys']['#private'])
            ?$sshConfig['keys']['#private']
            :$sshConfig['keys']['private'];
        $tunnelParams = array_intersect_key(
            $sshConfig,
            array_flip(
                [
                    'user', 'sshHost', 'sshPort', 'localPort', 'remoteHost', 'remotePort', 'privateKey',
                ]
            )
        );
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

        $dbConfig['host'] = '127.0.0.1';
        $dbConfig['port'] = $sshConfig['localPort'];

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

    protected function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        throw new UserException('Incremental Fetching is not supported by this extractor.');
    }

    protected function quote(string $obj): string
    {
        return "`{$obj}`";
    }

    protected function getConfigDefinition(string $action, bool $isConfigRow): string
    {
        if ($this->getCustomConfigDefinition()) {
            return $this->getCustomConfigDefinition();
        } elseif ($action !== 'run') {
            return ActionConfigDefinition::class;
        } elseif (!$isConfigRow) {
            return ConfigDefinition::class;
        } else {
            return ConfigRowDefinition::class;
        }
    }

    protected function getConfigClass(): string
    {
        return BaseExtractorConfig::class;
    }

    protected function getCustomConfigDefinition(): ?string
    {
        return null;
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
