<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Tests\Old;

use Keboola\DbExtractorCommon\DatabaseMetadata\Column;

class CommonExtractorColumnMetadata extends Column
{
    /** @var string|null */
    private $constraintName;

    /** @var bool */
    private $isForeignKey = false;

    /** @var string|null */
    private $foreignKeyRefSchema;

    /** @var string|null */
    private $foreignKeyRefTable;

    /** @var string|null */
    private $foreignKeyRefColumn;

    /** @var string|null */
    private $extra;

    /** @var int|null */
    private $autoIncrement;

    /** @var string|null */
    private $timestampUpdateColumn;

    public function toArray(): array
    {
        $result = parent::toArray();

        if ($this->constraintName) {
            $result['constraintName'] = $this->constraintName;
        }
        if ($this->isForeignKey) {
            $result['foreignKeyRefSchema'] = $this->foreignKeyRefSchema;
            $result['foreignKeyRefTable'] = $this->foreignKeyRefTable;
            $result['foreignKeyRefColumn'] = $this->foreignKeyRefColumn;
        }
        if ($this->extra) {
            $result['extra'] = $this->extra;
        }
        if ($this->autoIncrement) {
            $result['autoIncrement'] = $this->autoIncrement;
        }
        if ($this->timestampUpdateColumn) {
            $result['timestampUpdateColumn'] = $this->timestampUpdateColumn;
        }
        return $result;
    }

    public function getConstraintName(): ?string
    {
        return $this->constraintName;
    }

    public function isForeignKey(): bool
    {
        return $this->isForeignKey;
    }

    public function getForeignKeyRefSchema(): ?string
    {
        return $this->foreignKeyRefSchema;
    }

    public function getForeignKeyRefTable(): ?string
    {
        return $this->foreignKeyRefTable;
    }

    public function getForeignKeyRefColumn(): ?string
    {
        return $this->foreignKeyRefColumn;
    }

    public function getExtra(): ?string
    {
        return $this->extra;
    }

    public function getAutoIncrement(): ?int
    {
        return $this->autoIncrement;
    }

    public function getTimestampUpdateColumn(): ?string
    {
        return $this->timestampUpdateColumn;
    }

    public function setForeignKey(string $schema, string $table, string $column): void
    {
        $this->foreignKeyRefSchema = $schema;
        $this->foreignKeyRefTable = $table;
        $this->foreignKeyRefColumn = $column;
        $this->isForeignKey = true;
    }

    public function setAutoIncrement(int $autoIncrement): void
    {
        $this->autoIncrement = $autoIncrement;
    }

    public function setConstraintName(string $constraintName): void
    {
        $this->constraintName = $constraintName;
    }

    public function setExtra(string $extra): void
    {
        $this->extra = $extra;
    }

    public function setTimestampUpdateColumn(string $timestampUpdateColumn): void
    {
        $this->timestampUpdateColumn = $timestampUpdateColumn;
    }
}