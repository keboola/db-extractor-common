<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata\Builder;

use Keboola\DbExtractor\Metadata\ValueObject\TableCollection;

class MetadataBuilder implements Builder
{
    /** @var array|string[] */
    private array $tableRequiredProperties;

    /** @var array|string[] */
    private array $columnRequiredProperties;

    /** @var array|TableBuilder[] */
    private array $tables;

    public static function create(array $tableRequiredProperties = [], array $columnRequiredProperties = []): self
    {
        return new self($tableRequiredProperties, $columnRequiredProperties);
    }

    protected function __construct(array $tableRequiredProperties = [], array $columnRequiredProperties = [])
    {
        $this->tableRequiredProperties = $tableRequiredProperties;
        $this->columnRequiredProperties = $columnRequiredProperties;
    }

    public function build(): TableCollection
    {
        return new TableCollection(
            array_map(fn(TableBuilder $table) => $table->build(), $this->tables)
        );
    }

    public function addTable(): TableBuilder
    {
        $table = TableBuilder::create($this->tableRequiredProperties, $this->columnRequiredProperties);
        $this->tables[] = $table;
        return $table;
    }
}
