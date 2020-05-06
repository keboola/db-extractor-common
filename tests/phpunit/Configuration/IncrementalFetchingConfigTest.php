<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Configuration;

use Keboola\DbExtractor\Configuration\IncrementalFetchingConfig;
use Keboola\DbExtractor\ValueObject;
use Keboola\DbExtractor\Tests\ValueObject\BaseValueObjectTest;

class IncrementalFetchingConfigTest extends BaseValueObjectTest
{
    public function createValueObjectFromArray(array $properties): ValueObject
    {
        $name = $properties['column'];
        $schema = $properties['limit'];
        return new IncrementalFetchingConfig($name, $schema);
    }

    public function getAllProperties(): array
    {
        return [
            'column',
            'limit',
        ];
    }

    public function getNullableProperties(): array
    {
        return [
            'limit' => self::NULL_MEANS_NOT_SET,
        ];
    }

    public function getEmptyStringNotAllowedProperties(): array
    {
        return [
            'column',
        ];
    }

    public function getHasCallbacks(): array
    {
        return [
            'limit' => function (IncrementalFetchingConfig $config) {
                return $config->hasLimit();
            },
        ];
    }

    public function getGetCallbacks(): array
    {
        return [
            'column' => function (IncrementalFetchingConfig $config) {
                return $config->getColumn();
            },
            'limit' => function (IncrementalFetchingConfig $config) {
                return $config->getLimit();
            },
        ];
    }

    public function getValidInputs(): array
    {
        return [
            [
                'column' => 'column1',
                'limit' => 100,
            ],
            [
                'column' => 'column2',
                'limit' => null,
            ],
        ];
    }
}
