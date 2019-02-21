<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\DatabaseMetadata;

class Table implements \JsonSerializable, ToArrayInterface
{
    /** @var string */
    private $name;

    /** @var string */
    private $sanitizedName;

    /** @var string */
    private $schema;

    /** @var string */
    private $type;

    /** @var Column[] */
    private $columns = [];

    public function __construct(
        string $name,
        string $schema,
        string $type
    ) {
        $this->name = $name;
        $this->sanitizedName = \Keboola\Utils\sanitizeColumnName($name);
        $this->schema = $schema;
        $this->type = $type;
    }

    public function jsonSerialize(): array
    {
        return $this->toArray();
    }

    public function toArray(): array
    {
        return [
            'name' => $this->name,
            'sanitizedName' => $this->sanitizedName,
            'schema' => $this->schema,
            'type' => $this->type,
            'columns' => $this->getColumns(),
        ];
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

    /**
     * @return Column[]
     */
    public function getColumns(): array
    {
        ksort($this->columns);
        return $this->columns;
    }
}
