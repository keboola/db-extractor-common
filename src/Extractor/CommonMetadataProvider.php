<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\PDO\PdoConnection;
use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\TableCollection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;

class CommonMetadataProvider implements MetadataProvider
{
    private PdoConnection $connection;

    private string $database;

    public function __construct(PdoConnection $connection, string $database)
    {
        $this->connection = $connection;
        $this->database = $database;
    }

    public function getTable(InputTable $table): Table
    {
        return $this
            ->listTables([$table])
            ->getByNameAndSchema($table->getName(), $table->getSchema());
    }

    /**
     * @param array|InputTable[] $whitelist
     * @param bool $loadColumns if false, columns metadata are NOT loaded, useful useful if there are a lot of tables
     */
    public function listTables(array $whitelist = [], bool $loadColumns = true): TableCollection
    {
        /** @var TableBuilder[] $tableBuilders */
        $tableBuilders = [];

        /** @var ColumnBuilder[] $columnBuilders */
        $columnBuilders = [];

        /** @var int[] $autoIncrements */
        $autoIncrements = [];

        // Process tables
        $tableRequiredProperties = ['schema', 'type', 'rowCount'];
        $columnRequiredProperties= ['ordinalPosition', 'nullable'];
        $builder = MetadataBuilder::create($tableRequiredProperties, $columnRequiredProperties);
        foreach ($this->queryTables($whitelist) as $data) {
            $tableId = $data['TABLE_SCHEMA'] . '.' . $data['TABLE_NAME'];
            $tableBuilder = $builder->addTable();
            $tableBuilders[$tableId] = $tableBuilder;

            if ($loadColumns === false) {
                $tableBuilder->setColumnsNotExpected();
            }

            $this->processTableData($tableBuilder, $data);
            $autoIncrements[$tableId] = $data['AUTO_INCREMENT'] ?: null;
        }

        // Process columns
        if ($loadColumns) {
            foreach ($this->queryColumnsAndConstraints($whitelist) as $data) {
                $tableId = $data['TABLE_SCHEMA'] . '.' . $data['TABLE_NAME'];
                $columnId = $tableId . '.' . $data['COLUMN_NAME'];

                // If the column has multiple constraints
                // ... then is present multiple times in results
                if (isset($columnBuilders[$columnId])) {
                    $columnBuilder = $columnBuilders[$columnId];
                    $initialize = false;
                } else {
                    $columnBuilder = $tableBuilders[$tableId]->addColumn();
                    $columnBuilders[$columnId] = $columnBuilder;
                    $initialize = true;
                }

                $autoIncrement = isset($autoIncrements[$tableId]) ? (int) $autoIncrements[$tableId] : null;
                $this->processColumnData($columnBuilder, $data, $autoIncrement, $initialize);
            }
        }

        return $builder->build();
    }

    private function processTableData(TableBuilder $builder, array $data): void
    {
        $builder
            ->setName($data['TABLE_NAME'])
            ->setSchema($data['TABLE_SCHEMA'])
            ->setType($data['TABLE_TYPE'])
            ->setRowCount((int) $data['TABLE_ROWS']);
    }

    private function processColumnData(ColumnBuilder $builder, array $data, ?int $autoIncrement, bool $initialize): void
    {
        // Foreign key
        if (isset($data['REFERENCED_TABLE_NAME'])) {
            $builder
                ->addForeignKey()
                ->setRefSchema($data['REFERENCED_TABLE_SCHEMA'])
                ->setRefTable($data['REFERENCED_TABLE_NAME'])
                ->setRefColumn($data['REFERENCED_COLUMN_NAME']);
        }

        // Constraints
        if (isset($data['CONSTRAINT_NAME'])) {
            $builder->addConstraint($data['CONSTRAINT_NAME']);
        }

        // Other values has been already set
        if ($initialize === false) {
            return;
        }

        // Basic values
        $builder
            ->setName($data['COLUMN_NAME'])
            ->setOrdinalPosition((int) $data['ORDINAL_POSITION'])
            ->setType($data['DATA_TYPE'])
            ->setPrimaryKey($data['COLUMN_KEY'] === 'PRI')
            ->setUniqueKey($data['COLUMN_KEY'] === 'UNI')
            ->setNullable($data['IS_NULLABLE'] === 'YES');

        // Default value, if IS_NULLABLE is NO and COLUMN_DEFAULT is null => it means no default value
        if ($data['IS_NULLABLE'] === 'YES' || $data['COLUMN_DEFAULT'] !== null) {
            $builder->setDefault($data['COLUMN_DEFAULT']);
        }

        // Length
        $builder->setLength(
            $data['CHARACTER_MAXIMUM_LENGTH'] ?:
                (
                $data['NUMERIC_SCALE'] > 0 ?
                    $data['NUMERIC_PRECISION'] . ',' . $data['NUMERIC_SCALE'] :
                    $data['NUMERIC_PRECISION']
                )
        );

        // Auto increment
        if ($data['EXTRA'] === 'auto_increment' && $autoIncrement !== null) {
            $builder->setAutoIncrementValue($autoIncrement);
        }
    }

    /**
     * @param array|InputTable[] $whitelist
     */
    private function queryTables(array $whitelist = []): array
    {
        // Build query, REF: https://dev.mysql.com/doc/refman/8.0/en/tables-table.html
        $sql = [];
        $sql[] = 'SELECT *';
        $sql[] = 'FROM INFORMATION_SCHEMA.TABLES as c';
        $sql[] = sprintf('WHERE c.TABLE_SCHEMA = %s', $this->connection->quote($this->database));

        if ($whitelist) {
            $sql[] =sprintf('AND c.TABLE_NAME IN (%s)', $this->quoteTables($whitelist));
        }

        $sql[] = 'ORDER BY TABLE_SCHEMA, TABLE_NAME';

        // Run query
        return $this->connection->query(implode(' ', $sql))->fetchAll();
    }

    /**
     * @param array|InputTable[] $whitelist
     */
    private function queryColumnsAndConstraints(?array $whitelist = null): array
    {
        // Build query, REF: https://dev.mysql.com/doc/refman/5.1/en/columns-table.html
        $sql = [];
        $sql[] = 'SELECT c.*, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME, REFERENCED_TABLE_SCHEMA';
        $sql[] = 'FROM INFORMATION_SCHEMA.COLUMNS as c';
        $sql[] = 'LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu';
        $sql[] = 'ON c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME';
        $sql[] = sprintf('WHERE c.TABLE_SCHEMA = %s', $this->connection->quote($this->database));

        if ($whitelist) {
            $sql[] = sprintf('AND c.TABLE_NAME IN (%s)', $this->quoteTables($whitelist));
        }

        $sql[] = ' ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, ORDINAL_POSITION';

        // Run query
        return $this->connection->query(implode(' ', $sql))->fetchAll();
    }


    private function quoteTables(array $whitelist): string
    {
        return implode(
            ', ',
            array_map(function (InputTable $table) {
                if ($table->getSchema() !== $this->database) {
                    throw new InvalidArgumentException(sprintf(
                        'Table "%s"."%s" is not from used database.',
                        $table->getSchema(),
                        $table->getName()
                    ));
                }

                return $this->connection->quote($table->getName());
            }, $whitelist)
        );
    }
}
