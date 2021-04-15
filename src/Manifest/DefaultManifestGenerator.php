<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Manifest;

use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\ManifestSerializer;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class DefaultManifestGenerator implements ManifestGenerator
{
    protected MetadataProvider $metadataProvider;

    protected ManifestSerializer $serializer;

    public function __construct(MetadataProvider $metadataProvider, ManifestSerializer $manifestSerializer)
    {
        $this->metadataProvider = $metadataProvider;
        $this->serializer = $manifestSerializer;
    }

    public function generate(ExportConfig $exportConfig, ExportResult $exportResult): array
    {
        $manifestData = [
            'destination' => $exportConfig->getOutputTable(),
            'incremental' => $exportConfig->isIncrementalLoading(),
        ];

        if ($exportConfig->hasPrimaryKey()) {
            $manifestData['primary_key'] = $exportConfig->getPrimaryKey();
        }

        // If the CSV has a header, then columns metadata is not generated.
        if (!$exportResult->hasCsvHeader()) {
            if ($exportConfig->hasQuery()) {
                // Custom query -> use QueryMetadata
                $this->generateColumnsFromQueryMetadata($manifestData, $exportResult->getQueryMetadata());
            } else {
                // No custom query -> no generated columns -> all metadata are present in table metadata
                $this->generateColumnsFromTableMetadata($manifestData, $exportConfig);
            }
        }

        return $manifestData;
    }

    protected function generateColumnsFromTableMetadata(array &$manifestData, ExportConfig $exportConfig): void
    {
        $table = $this->metadataProvider->getTable($exportConfig->getTable());
        $allTableColumns = $table->getColumns();
        $columnMetadata = [];
        $sanitizedPks = [];
        $exportedColumns = $exportConfig->hasColumns() ?
            $exportConfig->getColumns() : $allTableColumns->getNames();

        foreach ($exportedColumns as $index => $columnName) {
            $column = $allTableColumns->getByName($columnName);
            $columnMetadata[$column->getSanitizedName()] = $this->serializer->serializeColumn($column);

            // Sanitize PKs defined in the configuration
            if ($exportConfig->hasPrimaryKey() &&
                in_array($column->getName(), $exportConfig->getPrimaryKey(), true)
            ) {
                $sanitizedPks[] = $column->getSanitizedName();
            }
        }

        $manifestData['metadata'] = $this->serializer->serializeTable($table);
        $manifestData['column_metadata'] = $columnMetadata;
        $manifestData['columns'] = array_keys($columnMetadata);
        if (!empty($sanitizedPks)) {
            $manifestData['primary_key'] = $sanitizedPks;
        }
    }

    protected function generateColumnsFromQueryMetadata(array &$manifestData, QueryMetadata $queryMetadata): void
    {
        $manifestData['columns'] = $queryMetadata->getColumns()->getNames();
        // TODO: We could also write column types metadata if they are set correctly in QueryMetadata.
    }
}
