<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\DatabaseMetadata;

class Column implements \JsonSerializable, ToArrayInterface
{
    /** @var string */
    private $name;

    /** @var string */
    private $sanitizedName;

    /** @var string */
    private $type;

    /** @var bool */
    private $primaryKey;

    /** @var string|null */
    private $length;

    /** @var bool */
    private $nullable;

    /** @var string|null */
    private $default;

    /** @var int */
    private $ordinalPosition;

    public function __construct(
        string $name,
        string $type,
        bool $primaryKey,
        ?string $length,
        bool $nullable,
        ?string $default,
        int $ordinalPosition
    ) {
        $this->name = $name;
        $this->sanitizedName = \Keboola\Utils\sanitizeColumnName($name);
        $this->type = $type;
        $this->primaryKey = $primaryKey;
        $this->length = $length;
        $this->nullable = $nullable;
        $this->default = $default;
        $this->ordinalPosition = $ordinalPosition;
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
            'type' => $this->type,
            'primaryKey' => $this->primaryKey,
            'length' => $this->length,
            'nullable' => $this->nullable,
            'default' => $this->default,
            'ordinalPosition' => $this->ordinalPosition,
        ];
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getSanitizedName(): string
    {
        return $this->sanitizedName;
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function isPrimaryKey(): bool
    {
        return $this->primaryKey;
    }

    public function getLength(): ?string
    {
        return $this->length;
    }

    public function isNullable(): bool
    {
        return $this->nullable;
    }

    public function getDefault(): ?string
    {
        return $this->default;
    }

    public function getOrdinalPosition(): int
    {
        return $this->ordinalPosition;
    }
}
