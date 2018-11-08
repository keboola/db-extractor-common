<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Tests;

use Keboola\Component\UserException;
use Keboola\Csv\CsvWriter;
use Keboola\Csv\Exception as CsvException;
use Keboola\DbExtractorCommon\Configuration\BaseExtractorConfig;
use Keboola\DbExtractorCommon\Configuration\DatabaseParametersInterface;
use Keboola\DbExtractorCommon\Configuration\TableDetailParameters;
use Keboola\DbExtractorCommon\Configuration\TableParameters;
use Keboola\DbExtractorCommon\Exception\ApplicationException;
use Keboola\DbExtractorCommon\Exception\DeadConnectionException;
use Keboola\DbExtractorCommon\BaseExtractor;
use Keboola\DbExtractorCommon\IncrementalFetchingSettings;
use Keboola\DbExtractorCommon\RetryProxy;

class CommonExtractor extends BaseExtractor
{
    public const TYPE_AUTO_INCREMENT = 'autoIncrement';
    public const TYPE_TIMESTAMP = 'timestamp';

    /** @var \PDO|null */
    private $connection;

    /** @var IncrementalFetchingSettings|null */
    private $incrementalFetching;

    public function extract(BaseExtractorConfig $config): array
    {
        $imported = [];
        $outputState = [];

        $tableParameters = $config->getConfigRowTableParameters();
        if ($tableParameters) {
            $exportResults = $this->extractTable($tableParameters);
            if (isset($exportResults['state'])) {
                $outputState = $exportResults['state'];
                unset($exportResults['state']);
            }
            $imported = $exportResults;
        } else {
            foreach ($config->getEnabledTables() as $table) {
                $exportResults = $this->extractTable($table);
                $imported[] = $exportResults;
            }
        }

        return [
            'status' => 'success',
            'imported' => $imported,
            'state' => $outputState,
        ];
    }

    /**
     * @inheritdoc
     */
    public function getTables(array $tables = []): array
    {
        $db = $this->getConnection();
        /** @var BaseExtractorConfig $config */
        $config = $this->getConfig();

        $sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES as c";

        $whereClause = " WHERE c.TABLE_SCHEMA != 'performance_schema' 
                          AND c.TABLE_SCHEMA != 'mysql'
                          AND c.TABLE_SCHEMA != 'information_schema'";

        if ($config->getDbParameters()->getDatabase()) {
            $whereClause = sprintf(
                " WHERE c.TABLE_SCHEMA = %s",
                $db->quote($config->getDbParameters()->getDatabase())
            );
        }

        if (!empty($tables)) {
            $whereClause .= sprintf(
                " AND c.TABLE_NAME IN (%s) AND c.TABLE_SCHEMA IN (%s)",
                implode(
                    ',',
                    array_map(
                        function ($table) use ($db) {
                            return $db->quote($table->getTableName());
                        },
                        $tables
                    )
                ),
                implode(
                    ',',
                    array_map(
                        function ($table) use ($db) {
                            return $db->quote($table->getSchema());
                        },
                        $tables
                    )
                )
            );
        }

        $sql .= $whereClause;

        $sql .= " ORDER BY TABLE_SCHEMA, TABLE_NAME";

        /** @var \PDOStatement $res */
        $res = $db->query($sql);
        /** @var array $arr */
        $arr = $res->fetchAll(\PDO::FETCH_ASSOC);
        if (count($arr) === 0) {
            return [];
        }

        $tableNameArray = [];
        $tableDefs = [];

        foreach ($arr as $table) {
            $tableNameArray[] = $table['TABLE_NAME'];
            $tableNameWithSchema = $table['TABLE_SCHEMA'] . '.' . $table['TABLE_NAME'];
            $tableDefs[$tableNameWithSchema] = [
                'name' => $table['TABLE_NAME'],
                'sanitizedName' => \Keboola\Utils\sanitizeColumnName($table['TABLE_NAME']),
                'schema' => (isset($table['TABLE_SCHEMA'])) ? $table['TABLE_SCHEMA'] : '',
                'type' => (isset($table['TABLE_TYPE'])) ? $table['TABLE_TYPE'] : '',
                'rowCount' => (isset($table['TABLE_ROWS'])) ? $table['TABLE_ROWS'] : '',
            ];
            if ($table["AUTO_INCREMENT"]) {
                $tableDefs[$tableNameWithSchema]['autoIncrement'] = $table['AUTO_INCREMENT'];
            }
        }

        if (!empty($tables)) {
            $sql = "SELECT c.*, 
                    CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME, REFERENCED_TABLE_SCHEMA
                    FROM INFORMATION_SCHEMA.COLUMNS as c 
                    LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu
                    ON c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME";
        } else {
            $sql = "SELECT c.*
                    FROM INFORMATION_SCHEMA.COLUMNS as c";
        }

        $sql .= $whereClause;

        $sql .= " ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, ORDINAL_POSITION";

        /** @var \PDOStatement $res */
        $res = $this->getConnection()->query($sql);
        /** @var array $rows */
        $rows = $res->fetchAll(\PDO::FETCH_ASSOC);

        foreach ($rows as $i => $column) {
            $curTable = $column['TABLE_SCHEMA'] . '.' . $column['TABLE_NAME'];
            $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
            if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
                if ($column['NUMERIC_SCALE'] > 0) {
                    $length = $column['NUMERIC_PRECISION'] . "," . $column['NUMERIC_SCALE'];
                } else {
                    $length = $column['NUMERIC_PRECISION'];
                }
            }
            $curColumn = [
                "name" => $column['COLUMN_NAME'],
                "sanitizedName" => \Keboola\Utils\sanitizeColumnName($column['COLUMN_NAME']),
                "type" => $column['DATA_TYPE'],
                "primaryKey" => ($column['COLUMN_KEY'] === "PRI") ? true : false,
                "length" => $length,
                "nullable" => ($column['IS_NULLABLE'] === "NO") ? false : true,
                "default" => $column['COLUMN_DEFAULT'],
                "ordinalPosition" => $column['ORDINAL_POSITION'],
            ];

            if (array_key_exists('CONSTRAINT_NAME', $column) && !is_null($column['CONSTRAINT_NAME'])) {
                $curColumn['constraintName'] = $column['CONSTRAINT_NAME'];
            }
            if (array_key_exists('REFERENCED_TABLE_NAME', $column) && !is_null($column['REFERENCED_TABLE_NAME'])) {
                $curColumn['foreignKeyRefSchema'] = $column['REFERENCED_TABLE_SCHEMA'];
                $curColumn['foreignKeyRefTable'] = $column['REFERENCED_TABLE_NAME'];
                $curColumn['foreignKeyRefColumn'] = $column['REFERENCED_COLUMN_NAME'];
            }
            if ($column['EXTRA']) {
                $curColumn["extra"] = $column["EXTRA"];
                if ($column['EXTRA'] === 'auto_increment') {
                    $curColumn['autoIncrement'] = $tableDefs[$curTable]['autoIncrement'];
                }
                if ($column['EXTRA'] === 'on update CURRENT_TIMESTAMP'
                    && $column['COLUMN_DEFAULT'] === 'CURRENT_TIMESTAMP') {
                    $tableDefs[$curTable]['timestampUpdateColumn'] = $column['COLUMN_NAME'];
                }
            }
            $tableDefs[$curTable]['columns'][$column['ORDINAL_POSITION'] - 1] = $curColumn;
        }
        return array_values($tableDefs);
    }

    public function validateIncrementalFetching(
        TableDetailParameters $table,
        string $columnName,
        ?int $limit = null
    ): void {
        $db = $this->getConnection();
        /** @var \PDOStatement $res */
        $res = $db->query(
            sprintf(
                'SELECT * FROM INFORMATION_SCHEMA.COLUMNS as cols 
                            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s',
                $db->quote($table->getSchema()),
                $db->quote($table->getTableName()),
                $db->quote($columnName)
            )
        );

        /** @var array $columns */
        $columns = $res->fetchAll();
        if (count($columns) === 0) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching was not found in the table',
                    $columnName
                )
            );
        }
        if ($columns[0]['EXTRA'] === 'auto_increment') {
            $this->incrementalFetching = new IncrementalFetchingSettings(
                $columnName,
                IncrementalFetchingSettings::TYPE_AUTO_INCREMENT
            );
        } else if ($columns[0]['EXTRA'] === 'on update CURRENT_TIMESTAMP'
            && $columns[0]['COLUMN_DEFAULT'] === 'CURRENT_TIMESTAMP'
        ) {
            $this->incrementalFetching = new IncrementalFetchingSettings(
                $columnName,
                IncrementalFetchingSettings::TYPE_TIMESTAMP
            );
        } else {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not an auto increment'
                    . ' column or an auto update timestamp',
                    $columnName
                )
            );
        }
        if ($limit) {
            $this->incrementalFetching->setLimit($limit);
        }
    }

    protected function testConnection(): void
    {
        $connection = $this->getConnection();

        /** @var \PDOStatement $stmt */
        $stmt = $connection->query('SELECT 1');
        $stmt->execute();
    }

    protected function quoteIdentifier(string $obj): string
    {
        return "`{$obj}`";
    }

    private function writeToCsv(\PDOStatement $stmt, CsvWriter $csvWriter, bool $includeHeader = true): array
    {
        $output = [];

        $resultRow = $stmt->fetch(\PDO::FETCH_ASSOC);

        if (is_array($resultRow) && !empty($resultRow)) {
            // write header and first line
            if ($includeHeader) {
                $csvWriter->writeRow(array_keys($resultRow));
            }
            $csvWriter->writeRow($resultRow);

            // write the rest
            $numRows = 1;
            $lastRow = $resultRow;

            while ($resultRow = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                $csvWriter->writeRow($resultRow);
                $lastRow = $resultRow;
                $numRows++;
            }
            $stmt->closeCursor();

            if ($this->incrementalFetching) {
                if (!array_key_exists($this->incrementalFetching->getColumn(), $lastRow)) {
                    throw new UserException(
                        sprintf(
                            "The specified incremental fetching column %s not found in the table",
                            $this->incrementalFetching->getColumn()
                        )
                    );
                }
                $output['lastFetchedRow'] = $lastRow[$this->incrementalFetching->getColumn()];
            }
            $output['rows'] = $numRows;
            return $output;
        }
        // no rows found.  If incremental fetching is turned on, we need to preserve the last state
        if ($this->incrementalFetching && isset($this->getInputState()['lastFetchedRow'])) {
            $output = $this->getInputState();
        }
        $output['rows'] = 0;
        return $output;
    }

    protected function createConnection(DatabaseParametersInterface $parameters): \PDO
    {
        if ($parameters->getPort()) {
            $dsn = sprintf(
                "mysql:host=%s;port=%s;dbname=%s;charset=utf8",
                $parameters->getHost(),
                $parameters->getPort(),
                $parameters->getDatabase()
            );
        } else {
            $dsn = sprintf(
                "mysql:host=%s;dbname=%s;charset=utf8",
                $parameters->getHost(),
                $parameters->getDatabase()
            );
        }
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
        ];
        try {
            return new \PDO($dsn, $parameters->getUser(), $parameters->getPassword(), $options);
        } catch (\Throwable $exception) {
            throw new DeadConnectionException("Error connecting to DB: " . $exception->getMessage(), 0, $exception);
        }
    }

    private function executeQuery(string $query, ?int $maxTries): \PDOStatement
    {
        $proxy = new RetryProxy($this->getLogger(), $maxTries);
        $stmt = $proxy->call(function () use ($query) {
            try {
                /** @var \PDOStatement $stmt */
                $stmt = $this->getConnection()->prepare($query);
                $stmt->execute();
                return $stmt;
            } catch (\Throwable $e) {
                try {
                    /** @var BaseExtractorConfig $config */
                    $config = $this->getConfig();
                    $this->connection = $this->createConnection($config->getDbParameters());
                } catch (\Throwable $e) {
                };
                throw $e;
            }
        });
        return $stmt;
    }

    private function extractTable(TableParameters $table): array
    {
        $outputTable = $table->getOutputTable();

        $this->getLogger()->info("Exporting to " . $outputTable);

        if (!$table->isAdvancedQuery()) {
            $query = $this->simpleQuery($table->getTableDetail(), $table->getColumns());
        } else {
            $query = $table->getQuery();
        }
        $maxTries = $table->getRetries();

        // this will retry on CsvException
        $proxy = new RetryProxy(
            $this->getLogger(),
            $maxTries,
            RetryProxy::DEFAULT_BACKOFF_INTERVAL,
            [DeadConnectionException::class, \ErrorException::class]
        );
        try {
            $result = $proxy->call(function () use ($query, $maxTries, $outputTable, $table) {
                /** @var \PDOStatement $stmt */
                $stmt = $this->executeQuery($query, $maxTries);
                $csvWriter = $this->createOutputCsv($outputTable);
                $result = $this->writeToCsv($stmt, $csvWriter, $table->isAdvancedQuery());
                $this->checkConnectionIsAlive();
                return $result;
            });
        } catch (CsvException $e) {
            throw new ApplicationException("Failed writing CSV File: " . $e->getMessage(), $e->getCode(), $e);
        } catch (\PDOException $e) {
            throw $this->handleDbError($e, $table, $maxTries);
        } catch (\ErrorException $e) {
            throw $this->handleDbError($e, $table, $maxTries);
        } catch (DeadConnectionException $e) {
            throw $this->handleDbError($e, $table, $maxTries);
        }

        if ($result['rows'] > 0) {
            $this->createManifest($table);
        } else {
            unlink($this->getOutputFilePath($outputTable));
            $this->getLogger()->warning(
                sprintf(
                    "Query returned empty result. Nothing was imported to [%s]",
                    $table->getOutputTable()
                )
            );
        }

        $output = [
            "outputTable"=> $outputTable,
            "rows" => $result['rows'],
        ];
        // output state
        if (!empty($result['lastFetchedRow'])) {
            $output["state"]['lastFetchedRow'] = $result['lastFetchedRow'];
        }
        return $output;
    }

    protected function getConnection(): \PDO
    {
        if ($this->connection) {
            return $this->connection;
        }
        /** @var BaseExtractorConfig $config */
        $config = $this->getConfig();
        return $this->createConnection($config->getDbParameters());
    }

    private function handleDbError(\Throwable $e, ?TableParameters $table = null, ?int $counter = null): UserException
    {
        $message = "";
        if ($table) {
            $message = sprintf("[%s]: ", $table->getOutputTable());
        }
        $message .= sprintf('DB query failed: %s', $e->getMessage());
        if ($counter) {
            $message .= sprintf(' Tried %d times.', $counter);
        }
        return new UserException($message, 0, $e);
    }

    private function simpleQuery(TableDetailParameters $table, array $columns = array()): string
    {
        if (count($columns) > 0) {
            $columnQuery = implode(', ', $this->quoteIdentifiers($columns));
        } else {
            $columnQuery = '*';
        }

        $query = sprintf(
            "SELECT %s FROM %s.%s",
            $columnQuery,
            $this->quoteIdentifier($table->getSchema()),
            $this->quoteIdentifier($table->getTableName())
        );

        $incrementalAddon = null;
        if ($this->incrementalFetching && isset($this->getInputState()['lastFetchedRow'])) {
            if ($this->incrementalFetching->isTypeTimestamp()) {
                $incrementalAddon = sprintf(
                    ' %s > %d',
                    $this->quoteIdentifier($this->incrementalFetching->getColumn()),
                    (int) $this->getInputState()['lastFetchedRow']
                );
            } else if ($this->incrementalFetching->isTypeAutoIncrement()) {
                $incrementalAddon = sprintf(
                    " %s > '%s'",
                    $this->quoteIdentifier($this->incrementalFetching->getColumn()),
                    $this->getInputState()['lastFetchedRow']
                );
            } else {
                throw new ApplicationException(
                    sprintf('Unknown incremental fetching column type %s', $this->incrementalFetching->getType())
                );
            }
        }

        if ($incrementalAddon) {
            $query .= sprintf(
                " WHERE %s ORDER BY %s",
                $incrementalAddon,
                $this->quoteIdentifier($this->incrementalFetching->getColumn())
            );
        }
        if ($this->incrementalFetching && $this->incrementalFetching->getLimit()) {
            $query .= sprintf(
                " LIMIT %d",
                $this->incrementalFetching->getLimit()
            );
        }
        return $query;
    }
}
