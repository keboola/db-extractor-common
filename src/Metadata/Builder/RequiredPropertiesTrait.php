<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata\Builder;

use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\PropertyNotFoundException;
use Keboola\DbExtractor\Exception\PropertyNotSetException;

trait RequiredPropertiesTrait
{
    /** @var array|string[] */
    protected array $requiredProperties;

    protected function setRequiredProperties(array $props, array $alwaysRequired, array $optionalRequired): void
    {
        // Check given required properties
        $invalidReqProps = array_diff($props, $optionalRequired);
        if ($invalidReqProps) {
            throw new InvalidArgumentException(sprintf(
                'Properties "%s" cannot be set as required, ' .
                'they are not present in OPTIONAL_REQUIRED_PROPERTIES constant',
                implode('", "', $invalidReqProps)
            ));
        }

        // Merge always required properties and required properties set by constructor
        $this->requiredProperties = array_unique(
            array_merge($alwaysRequired, $props)
        );
    }

    protected function setPropertyAsOptional(string $property): void
    {
        $key = array_search($property, $this->requiredProperties);
        if ($key === false) {
            throw new InvalidArgumentException(
                sprintf('Property "%s" is not required.', $property)
            );
        }

        unset($this->requiredProperties[$key]);
    }

    protected function checkRequiredProperties(): void
    {
        $vars = get_object_vars($this);
        foreach ($this->requiredProperties as $property) {
            if (!array_key_exists($property, $vars)) {
                throw new PropertyNotFoundException(
                    sprintf('Required property "%s" is not defined in %s.', $property, get_class($this))
                );
            }

            if ($vars[$property] === null) {
                throw new PropertyNotSetException(
                    sprintf('Required property "%s" is not set.', $property)
                );
            }
        }
    }
}
