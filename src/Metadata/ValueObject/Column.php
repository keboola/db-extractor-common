<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata\ValueObject;

use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\PropertyNotSetException;
use Keboola\DbExtractor\ValueObject;

class Column implements ValueObject
{
    private string $name;

    private string $sanitizedName;

    private ?string $description;

    private ?int $ordinalPosition;

    private string $type;

    private ?bool $nullable;

    private ?string $length;

    private bool $primaryKey;

    private bool $uniqueKey;

    private bool $autoIncrement;

    private ?int $autoIncrementValue;

    /**
     * "null" is regular default value, so we must use bool hasDefaultValue to determine if column has default value
     */
    private bool $hasDefaultValue;

    private ?string $default;

    private ?ForeignKey $foreignKey;

    private array $constraints;

    public function __construct(
        string $name,
        string $sanitizedName,
        ?string $description,
        ?int $ordinalPosition,
        string $type,
        ?bool $nullable,
        ?string $length,
        bool $primaryKey,
        bool $uniqueKey,
        bool $autoIncrement,
        ?int $autoIncrementValue,
        bool $hasDefaultValue,
        ?string $default,
        ?ForeignKey $foreignKey,
        array $constraints
    ) {
        if (empty($name)) {
            throw new InvalidArgumentException('Column\'s name cannot be empty.');
        }

        if (empty($sanitizedName)) {
            throw new InvalidArgumentException('Column\'s sanitized name cannot be empty.');
        }

        if ($description !== null && empty($description)) {
            throw new InvalidArgumentException('Column\'s description cannot be empty string, use null.');
        }

        if (empty($type)) {
            throw new InvalidArgumentException('Column\'s type cannot be empty.');
        }

        if ($length !== null && empty($length)) {
            throw new InvalidArgumentException('Column\'s length cannot be empty string, use null.');
        }

        if (!$autoIncrement && $autoIncrementValue !== null) {
            throw new InvalidArgumentException('Auto increment value set, but column is not auto increment.');
        }

        if (!$hasDefaultValue && $default !== null) {
            throw new InvalidArgumentException('Default value is set, but hasDefaultValue is false.');
        }

        $this->name = $name;
        $this->sanitizedName = $sanitizedName;
        $this->description = $description;
        $this->ordinalPosition = $ordinalPosition;
        $this->type = $type;
        $this->nullable = $nullable;
        $this->length = $length;
        $this->primaryKey = $primaryKey;
        $this->uniqueKey = $uniqueKey;
        $this->autoIncrement = $autoIncrement;
        $this->autoIncrementValue = $autoIncrementValue;
        $this->hasDefaultValue = $hasDefaultValue;
        $this->default = $default;
        $this->foreignKey = $foreignKey;
        $this->constraints = $constraints;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getSanitizedName(): string
    {
        return $this->sanitizedName;
    }

    public function hasDescription(): bool
    {
        return $this->description !== null;
    }

    public function getDescription(): string
    {
        if ($this->description === null) {
            throw new PropertyNotSetException('Description is not set.');
        }
        return $this->description;
    }

    public function hasOrdinalPosition(): bool
    {
        return $this->ordinalPosition !== null;
    }

    public function getOrdinalPosition(): int
    {
        if ($this->ordinalPosition === null) {
            throw new PropertyNotSetException('Ordinal position is not set.');
        }
        return $this->ordinalPosition;
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function hasNullable(): bool
    {
        return $this->nullable !== null;
    }


    public function isNullable(): bool
    {
        if ($this->nullable === null) {
            throw new PropertyNotSetException('Nullable is not set.');
        }
        return $this->nullable;
    }

    public function hasLength(): bool
    {
        return $this->length !== null;
    }

    public function getLength(): string
    {
        if ($this->length === null) {
            throw new PropertyNotSetException('Length is not set.');
        }
        return $this->length;
    }

    public function isPrimaryKey(): bool
    {
        return $this->primaryKey;
    }

    public function isUniqueKey(): bool
    {
        return $this->uniqueKey;
    }

    public function isAutoIncrement(): bool
    {
        return $this->autoIncrement;
    }

    public function hasAutoIncrementValue(): bool
    {
        if (!$this->isAutoIncrement()) {
            return false;
        }

        return $this->autoIncrementValue !== null;
    }

    public function getAutoIncrementValue(): int
    {
        if (!$this->isAutoIncrement()) {
            throw new PropertyNotSetException('Column is not auto increment.');
        }

        if ($this->autoIncrementValue === null) {
            throw new PropertyNotSetException('Auto increment value is not set.');
        }

        return $this->autoIncrementValue;
    }


    public function hasDefault(): bool
    {
        return $this->hasDefaultValue;
    }

    public function getDefault(): ?string
    {
        if ($this->hasDefaultValue === false) {
            throw new PropertyNotSetException('Column hasn\'t default value.');
        }

        // Default value can be "null" value
        return $this->default;
    }

    public function hasForeignKey(): bool
    {
        return $this->foreignKey !== null;
    }

    public function getForeignKey(): ForeignKey
    {
        if ($this->foreignKey === null) {
            throw new PropertyNotSetException('Column has no foreign key.');
        }

        return $this->foreignKey;
    }

    public function getConstraints(): array
    {
        return $this->constraints;
    }
}
