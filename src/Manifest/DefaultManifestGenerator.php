<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Manifest;

use Keboola\Component\Manifest\ManifestManager\Options\OutTable\ManifestOptions;
use Keboola\Component\Manifest\ManifestManager\Options\OutTable\ManifestOptionsSchema;
use Keboola\Datatype\Definition\Common;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\ManifestSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Column;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class DefaultManifestGenerator implements ManifestGenerator
{
    protected MetadataProvider $metadataProvider;
    protected ManifestSerializer $serializer;
    protected string $extractorClass;

    public function __construct(
        MetadataProvider $metadataProvider,
        ManifestSerializer $manifestSerializer,
        string $extractorClass,
    ) {
        $this->metadataProvider = $metadataProvider;
        $this->serializer = $manifestSerializer;
        $this->extractorClass = $extractorClass;
    }

    public function generate(ExportConfig $exportConfig, ExportResult $exportResult, bool $legacy = false): array
    {
        $manifestOptions = new ManifestOptions();
        $manifestOptions->setDestination($exportConfig->getOutputTable())
            ->setIncremental($exportConfig->isIncrementalLoading());

        // If the CSV has a header, then columns metadata is not generated.
        if (!$exportResult->hasCsvHeader()) {
            if ($exportConfig->hasQuery()) {
                // Custom query -> use QueryMetadata
                $this->generateColumnsFromQueryMetadata(
                    $manifestOptions,
                    $exportResult->getQueryMetadata(),
                    $exportConfig->hasPrimaryKey() ? $exportConfig->getPrimaryKey() : [],
                );
            } else {
                // No custom query -> no generated columns -> all metadata are present in table metadata
                $this->generateColumnsFromTableMetadata($manifestOptions, $exportConfig);
            }
        }

        return $manifestOptions->toArray($legacy);
    }

    protected function generateColumnsFromTableMetadata(
        ManifestOptions $manifestOptions,
        ExportConfig $exportConfig,
    ): void {
        $table = $this->metadataProvider->getTable($exportConfig->getTable());
        $columns = $exportConfig->hasColumns() ? $exportConfig->getColumns() : null;
        $primaryKeys = $exportConfig->hasPrimaryKey() ? $exportConfig->getPrimaryKey() : null;
        $schema = $this->generateSchema($table, $columns, $primaryKeys);
        $tableMetadata = $this->constructTableMetadata($table);

        if ($table->hasDescription()) {
            $manifestOptions->setDescription($table->getDescription());
        }
        $manifestOptions->setTableMetadata($tableMetadata);
        $manifestOptions->setSchema($schema);
    }

    protected function generateColumnsFromQueryMetadata(
        ManifestOptions $manifestOptions,
        QueryMetadata $queryMetadata,
        array $primaryKeys,
    ): void {
        $columns = $queryMetadata->getColumns();
        $schema = [];

        foreach ($columns as $column) {
            $schema[] = $this->createSchemaEntryFromColumn($column, in_array($column->getName(), $primaryKeys, true));
        }

        $manifestOptions->setSchema($schema);
    }

    private function generateSchema(Table $table, ?array $columns, ?array $primaryKeys): array
    {
        $schema = [];
        $allTableColumns = $table->getColumns();
        $exportedColumns = $columns ?: $allTableColumns->getNames();

        foreach ($exportedColumns as $column) {
            $column = $allTableColumns->getByName($column);
            $columnMetadataKeyValueArray = $this->serializer->serializeColumn($column);
            $columnMetadata = [];
            foreach ($columnMetadataKeyValueArray as $columnMetadataKeyValue) {
                $columnMetadata[$columnMetadataKeyValue['key']] = $columnMetadataKeyValue['value'];
            }
            $isPrimaryKey = $primaryKeys && in_array($column->getName(), $primaryKeys, true);
            $backend = $table->hasDatatypeBackend() ? $table->getDatatypeBackend() : null;
            $schema[] = $this->createSchemaEntryFromSerializedColumnMetadata($columnMetadata, $isPrimaryKey, $backend);
        }

        return $schema;
    }

    /**
     * @param array<string, mixed> $columnMetadata
     * @throws \Keboola\Component\Manifest\ManifestManager\Options\OptionsValidationException
     */
    private function createSchemaEntryFromSerializedColumnMetadata(
        array $columnMetadata,
        bool $isPrimaryKey,
        ?string $backend = null,
    ): ManifestOptionsSchema {
        $baseType = [
            'type' => $columnMetadata[Common::KBC_METADATA_KEY_BASETYPE],
            'length' => $columnMetadata[Common::KBC_METADATA_KEY_LENGTH] ?? null,
            'default' => $columnMetadata[Common::KBC_METADATA_KEY_DEFAULT] ?? null,
        ];
        $baseType = array_filter($baseType, fn($value) => $value !== null);

        $dataTypes = ['base' => $baseType];
        if ($backend) {
            if (isset($columnMetadata[Common::KBC_METADATA_KEY_TYPE])) {
                $baseType['type'] = $columnMetadata[Common::KBC_METADATA_KEY_TYPE];
            }
            $dataTypes[$backend] = $baseType;
        }

        $isNullable = $columnMetadata[Common::KBC_METADATA_KEY_NULLABLE] ?? true;

        unset(
            $columnMetadata[Common::KBC_METADATA_KEY_BASETYPE],
            $columnMetadata[Common::KBC_METADATA_KEY_LENGTH],
            $columnMetadata[Common::KBC_METADATA_KEY_DEFAULT],
            $columnMetadata[Common::KBC_METADATA_KEY_TYPE],
            $columnMetadata[Common::KBC_METADATA_KEY_NULLABLE],
            $columnMetadata['KBC.primaryKey'],
            $columnMetadata['KBC.description'],
        );

        return new ManifestOptionsSchema(
            $columnMetadata['KBC.sanitizedName'],
            $dataTypes,
            $isNullable,
            $isPrimaryKey,
            $columnMetadata['KBC.description'] ?? null,
            $columnMetadata,
        );
    }

    private function createSchemaEntryFromColumn(Column $column, bool $isPrimaryKey): ManifestOptionsSchema
    {
        $metadata = [
            'KBC.sourceName' => $column->getName(),
            'KBC.sanitizedName' => $column->getSanitizedName(),
            'KBC.uniqueKey' => $column->isUniqueKey(),
            'KBC.ordinalPosition' => $column->hasOrdinalPosition() ? $column->getOrdinalPosition() : null,
            'KBC.autoIncrement' => $column->isAutoIncrement() ?: null,
            'KBC.autoIncrementValue' => $column->hasAutoIncrementValue() ? $column->getAutoIncrementValue() : null,
        ];

        if ($column->hasForeignKey()) {
            $fk = $column->getForeignKey();
            $metadata['KBC.foreignKey'] = true;
            $metadata['KBC.foreignKeyName'] = $fk->hasName() ? $fk->getName() : null;
            $metadata['KBC.foreignKeyRefSchema'] = $fk->hasRefSchema() ? $fk->getRefSchema() : null;
            $metadata['KBC.foreignKeyRefTable'] = $fk->getRefTable();
            $metadata['KBC.foreignKeyRefColumn'] = $fk->getRefColumn();
        }

        foreach ($column->getConstraints() as $constraint) {
            $metadata['KBC.constraintName'] = $constraint;
        }

        $metadata = array_filter($metadata, fn($value) => $value !== null);
        $dataTypes = null;

        $dataTypeClass = sprintf('\\Keboola\\Datatype\\Definition\\%s', $this->extractorClass);
        if (class_exists($dataTypeClass)) {
            /** @var \Keboola\Datatype\Definition\DefinitionInterface $backendDataTypeDefinition */
            $backendDataTypeDefinition = new $dataTypeClass($column->getType());
            $baseType = [
                'type' => $backendDataTypeDefinition->getBasetype(),
                'length' => $column->hasLength() ? $column->getLength() : null,
                'default' => $column->hasDefault() ? (string) $column->getDefault() : null,
            ];
            $baseType = array_filter($baseType, fn($value) => $value !== null);

            $dataTypes['base'] = $baseType;
        }

        $backend = strtolower($this->extractorClass);
        if (in_array($backend, ManifestOptionsSchema::ALLOWED_DATA_TYPES_BACKEND, true)) {
            $backendDataType = [
                'type' => $column->getType(),
                'length' => $column->hasLength() ? $column->getLength() : null,
                'default' => $column->hasDefault() ? (string) $column->getDefault() : null,
            ];

            $backendDataType = array_filter($backendDataType, fn($value) => $value !== null);
            $dataTypes[$backend] = $backendDataType;
        }

        return new ManifestOptionsSchema(
            $column->getSanitizedName(),
            $dataTypes,
            !$column->hasNullable() || $column->isNullable(),
            $isPrimaryKey,
            $column->hasDescription() ? $column->getDescription() : null,
            $metadata,
        );
    }

    private function constructTableMetadata(Table $table): array
    {
        $values = [
            'KBC.name' => $table->getName(),
            'KBC.sanitizedName' => $table->getSanitizedName(),
            'KBC.schema' => $table->hasSchema() ? $table->getSchema() : null,
            'KBC.catalog' => $table->hasCatalog() ? $table->getCatalog() : null,
            'KBC.tablespaceName' => $table->hasTablespaceName() ? $table->getTablespaceName() : null,
            'KBC.owner' => $table->hasOwner() ? $table->getOwner() : null,
            'KBC.type' => $table->hasType() ? $table->getType() : null,
            'KBC.rowCount' => $table->hasRowCount() ? $table->getRowCount() : null,
            'KBC.datatype.backend' => $table->hasDatatypeBackend() ? $table->getDatatypeBackend() : null,
        ];

        return array_filter($values, fn($value) => $value !== null);
    }
}
