<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata\Builder;

use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\InvalidStateException;
use Keboola\DbExtractor\Metadata\ValueObject\Column;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

class ColumnBuilder implements Builder
{
    use RequiredPropertiesTrait;

    /**
     * List of always required properties
     */
    public const ALWAYS_REQUIRED_PROPERTIES = ['name', 'sanitizedName', 'type'];

    /**
     * List of properties that can be marked as required through the constructor.
     */
    public const OPTIONAL_REQUIRED_PROPERTIES = ['ordinalPosition', 'nullable'];

    private ?string $name = null;

    private ?string $sanitizedName = null;

    private ?string $description = null;

    private ?int $ordinalPosition = null; // 0, 1, 2 ...

    private ?string $type = null;

    private ?bool $nullable = null;

    private ?string $length = null;

    private bool $primaryKey = false;

    private bool $uniqueKey = false;

    private bool $autoIncrement = false;

    private ?int $autoIncrementValue = null;

    private bool $hasDefaultValue = false;

    private ?string $default = null;

    private ?ForeignKeyBuilder $foreignKeyBuilder = null;

    private array $constraints = [];

    /**
     * @param array|string[] $requiredProperties
     */
    public static function create(array $requiredProperties = []): self
    {
        return new self($requiredProperties);
    }

    protected function __construct(array $requiredProperties)
    {
        $this->setRequiredProperties(
            $requiredProperties,
            self::ALWAYS_REQUIRED_PROPERTIES,
            self::OPTIONAL_REQUIRED_PROPERTIES
        );
    }

    public function build(): Column
    {
        $this->checkRequiredProperties();
        return new Column(
            $this->name,
            $this->sanitizedName,
            $this->description,
            $this->ordinalPosition,
            $this->type,
            $this->nullable,
            $this->length,
            $this->primaryKey,
            $this->uniqueKey,
            $this->autoIncrement,
            $this->autoIncrementValue,
            $this->hasDefaultValue,
            $this->default,
            $this->foreignKeyBuilder ? $this->foreignKeyBuilder->build() : null,
            $this->constraints
        );
    }

    public function setName(string $name): self
    {
        if (empty($name)) {
            throw new InvalidArgumentException('Table\'s name cannot be empty.');
        }

        $this->name = $name;
        $this->sanitizedName = ColumnNameSanitizer::sanitize($name);
        return $this;
    }

    public function setDescription(?string $description): self
    {
        // Normalize, empty string is not allowed
        $this->description = empty($description) ? null : $description;
        return $this;
    }

    public function setOrdinalPosition(int $ordinalPosition): self
    {
        $this->ordinalPosition = $ordinalPosition;
        return $this;
    }

    public function setType(string $type): self
    {
        $this->type = $type;
        return $this;
    }

    public function setNullable(bool $nullable): self
    {
        $this->nullable = $nullable;
        return $this;
    }

    public function setLength(?string $length): self
    {
        // Normalize, empty string is not allowed
        $this->length = empty($length) ? null : $length;
        return $this;
    }

    public function setPrimaryKey(bool $primaryKey): self
    {
        $this->primaryKey = $primaryKey;
        return $this;
    }

    public function setUniqueKey(bool $uniqueKey): self
    {
        $this->uniqueKey = $uniqueKey;
        return $this;
    }

    public function setAutoIncrement(bool $autoIncrement): self
    {
        $this->autoIncrement = $autoIncrement;
        return $this;
    }

    public function setAutoIncrementValue(int $autoIncrementValue): self
    {
        $this->setAutoIncrement(true);
        $this->autoIncrementValue = $autoIncrementValue;
        return $this;
    }

    public function setDefault(?string $default): self
    {
        $this->hasDefaultValue = true;
        $this->default = $default;
        return $this;
    }

    public function addForeignKey(): ForeignKeyBuilder
    {
        if ($this->foreignKeyBuilder) {
            throw new InvalidStateException('Foreign key is already set.');
        }

        $this->foreignKeyBuilder = ForeignKeyBuilder::create();
        return $this->foreignKeyBuilder;
    }

    public function addConstraint(string $constraint): self
    {
        $this->constraints[] = $constraint;
        return $this;
    }
}
