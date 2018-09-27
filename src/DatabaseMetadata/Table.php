<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\DatabaseMetadata;

class Table implements \JsonSerializable
{
    /** @var string */
    private $name;

    /** @var string|null */
    private $sanitizedName;

    /** @var string|null */
    private $schema;

    /** @var string|null */
    private $type;

    /** @var int|null */
    private $rowCount;

    /** @var int|null */
    private $autoIncrement;

    /** @var string|null */
    private $timestampUpdateColumn;

    /** @var Column[] */
    private $columns = [];

    public function __construct(
        string $name,
        ?string $sanitizedName,
        ?string $schema,
        ?string $type,
        ?int $rowCount
    ) {
        $this->name = $name;
        $this->sanitizedName = $sanitizedName;
        $this->schema = $schema;
        $this->type = $type;
        $this->rowCount = $rowCount;
    }

    public function jsonSerialize(): array
    {
        $result = [
            'name' => $this->name,
            'sanitizedName' => $this->sanitizedName,
            'schema' => $this->schema,
            'type' => $this->type,
            'rowCount' => $this->rowCount,
        ];

        if ($this->autoIncrement) {
            $result['autoIncrement'] = $this->autoIncrement;
        }

        if ($this->timestampUpdateColumn) {
            $result['timestampUpdateColumn'] = $this->timestampUpdateColumn;
        }

        $result['columns'] = $this->getColumns();
        return $result;
    }

    public function addColumn(int $position, Column $columnMetadata): void
    {
        $this->columns[$position] = $columnMetadata;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getSanitizedName(): ?string
    {
        return $this->sanitizedName;
    }

    public function getSchema(): ?string
    {
        return $this->schema;
    }

    public function getType(): ?string
    {
        return $this->type;
    }

    public function getRowCount(): ?int
    {
        return $this->rowCount;
    }

    public function getAutoIncrement(): ?int
    {
        return $this->autoIncrement;
    }

    public function getTimestampUpdateColumn(): ?string
    {
        return $this->timestampUpdateColumn;
    }

    /**
     * @return Column[]
     */
    public function getColumns(): array
    {
        ksort($this->columns);
        return $this->columns;
    }

    public function setAutoIncrement(int $autoIncrement): void
    {
        $this->autoIncrement = $autoIncrement;
    }

    public function setTimestampUpdateColumn(string $timestampUpdateColumn): void
    {
        $this->timestampUpdateColumn = $timestampUpdateColumn;
    }
}
