<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\DatabaseMetadata;

class Column implements \JsonSerializable
{
    /** @var string */
    private $name;

    /** @var string|null */
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
        $result = [
            'name' => $this->name,
            'sanitizedName' => $this->sanitizedName,
            'type' => $this->type,
            'primaryKey' => $this->primaryKey,
            'length' => $this->length,
            'nullable' => $this->nullable,
            'default' => $this->default,
            'ordinalPosition' => $this->ordinalPosition,
        ];

        return $result;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getSanitizedName(): ?string
    {
        return $this->sanitizedName;
    }
}
