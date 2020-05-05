<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Metadata\Builder;

use RuntimeException;
use LogicException;
use TypeError;
use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\PropertyNotSetException;
use PHPUnit\Framework\Assert;
use Keboola\DbExtractor\Metadata\Builder\Builder;
use Keboola\DbExtractor\ValueObject;
use PHPUnit\Framework\TestCase;

/**
 * This class tests that values set by a Builder set* methods
 * match values returned by a ValueObject has* and get* methods.
 */
abstract class BaseBuilderTest extends TestCase
{
    public const NULL_MEANS_NOT_SET = 'null_means_not_set_default_not_set';
    public const NULL_IS_REGULAR_DEFAULT_NULL = 'null_is_regular_default_null';
    public const NULL_IS_REGULAR_DEFAULT_NOT_SET = 'null_is_regular_default_not_set';

    abstract public function createBuilder(array $additionalRequiredProperties = []): Builder;

    /**
     * Get all builder / value object properties that will be tested
     * @return array|string[]
     */
    abstract public function getAllProperties(): array;

    /**
     * Get properties that are always required = must be set by setter
     * @return array|string[]
     */
    abstract public function getAlwaysRequiredProperties(): array;

    /**
     * Get properties that can be marked as required through the constructor.
     * @return array|string[]
     */
    abstract public function getOptionalRequiredProperties(): array;

    /**
     * Key => property name
     * Value => ENUM(
     *     self::NULL_MEANS_NOT_SET,
     *     self::NULL_IS_REGULAR_DEFAULT_NULL,
     *     self::NULL_IS_REGULAR_DEFAULT_NOT_SET
     * )
     * @return array|string[]
     */
    abstract public function getNullableProperties(): array;

    /**
     * Get properties for which an attempt to set an empty string will result in an error.
     * @return array|string[]
     */
    abstract public function getEmptyStringNotAllowedProperties(): array;

    /**
     * Get properties for which an empty string is converted to null.
     * @return array|string[]
     */
    abstract public function getEmptyStringConvertToNullProperties(): array;

    /**
     * Key => property name,
     * Value => property default value
     * @return array|mixed[]
     */
    abstract public function getDefaultValues(): array;

    /**
     * Get "set" callbacks called on Builder
     * @return array|callable<Builder, mixed>[]
     */
    abstract public function getSetCallbacks(): array;

    /**
     * Get "has" callbacks called on ValueObject
     * @return array|callable<ValueObject>[]
     */
    abstract public function getHasCallbacks(): array;

    /**
     * Get "get" callbacks called on ValueObject
     * @return array|callable<ValueObject>[]
     */
    abstract public function getGetCallbacks(): array;

    /**
     * Get valid inputs for testing.
     * Invalid inputs are automatically generated from these valid by changing a property.
     * @return array|mixed[][]
     */
    abstract public function getValidInputs(): array;

    /**
     * @dataProvider getValidDataProvider
     */
    public function testValid(array $propertiesToSet): void
    {
        $valueObject = $this->buildFromArray($propertiesToSet);

        foreach ($this->getAllProperties() as $property) {
            $nullableDef = $this->getNullableProperties()[$property] ?? null;
            $hasDefaultValue = $this->hasDefaultValue($property);

            // Expectations
            if (array_key_exists($property, $propertiesToSet)) {
                // If - property is set by this test (see buildFromArray method)
                $expectedValue = $propertiesToSet[$property];
                // Property is taken as "is set", when value is not null OR null value is allowed
                $expectedIsSet =
                    $expectedValue !== null ||
                    in_array($nullableDef, [
                        self::NULL_IS_REGULAR_DEFAULT_NULL,
                        self::NULL_IS_REGULAR_DEFAULT_NOT_SET,
                    ], true);
            } else {
                // Else - property is not set by test, ... default value is applied
                $expectedValue = $hasDefaultValue ? $this->getDefaultValue($property) : null;
                // Property is taken as "is set", when value is not null OR null value is default
                $expectedIsSet = $expectedValue !== null || $nullableDef === self::NULL_IS_REGULAR_DEFAULT_NULL;
            }

            // Test has* method
            // Property must be defined if it was set in this test or has default value
            $isSet = $this->callHas($valueObject, $property);
            if ($isSet !== null) { // null => ValueObject hasn't "has" method
                Assert::assertSame(
                    $expectedIsSet,
                    $isSet,
                    sprintf(
                        '"%s" expected as "%s" - property "has" method result, but given %s.',
                        $expectedIsSet ? 'true' : 'false',
                        $property,
                        $isSet ? 'true' : 'false',
                    )
                );
            }

            // Test get* method
            if ($expectedIsSet) {
                // It is expected that property has defined value
                // Compare value from ValueObject::get* with value set to Builder::set* (or with default value)
                $value = $this->callGet($valueObject, $property);
                Assert::assertSame($expectedValue, $value);
            } else {
                // It is expected that property is NOT defined
                // Get method must thrown PropertyNotSetException
                try {
                    $value = $this->callGet($valueObject, $property);
                    Assert::fail(sprintf(
                        'Expected %s not thrown when calling get callback of "%s". Returned %s.',
                        'PropertyNotSetException',
                        $property,
                        is_object($value) ? get_class($value) : getType($value) . ': ' . json_encode($value)
                    ));
                } catch (PropertyNotSetException $e) {
                    // ok, exception was thrown
                }
            }
        }
    }

    /**
     * @dataProvider getInvalidDataProvider
     * @psalm-param class-string<\Throwable> $exceptionClass
     */
    public function testInvalid(string $exceptionClass, string $exceptionMsg, array $propertiesToSet): void
    {
        $this->expectException($exceptionClass);
        $this->expectExceptionMessageMatches($exceptionMsg);
        $this->buildFromArray($propertiesToSet);
    }

    /**
     * @dataProvider getEmptyStringNotAllowedDataProvider
     */
    public function testEmptyStringNotAllowed(array $propertiesToSet): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('~cannot be empty~');
        $this->buildFromArray($propertiesToSet);
    }

    /**
     * @dataProvider getEmptyStringConvertToNullDataProvider
     */
    public function testEmptyStringConvertToNull(string $property, array $propertiesToSet): void
    {
        // Set empty string to property, it should be converted to null
        Assert::assertSame('', $propertiesToSet[$property]);
        $valueObject = $this->buildFromArray($propertiesToSet);
        $nullableDef = $this->getNullableProperties()[$property] ?? null;

        // Test has* method
        $isSet = $this->callHas($valueObject, $property);
        if ($isSet !== null) { // null => value object hasn't "has" method for property
            if ($nullableDef === self::NULL_IS_REGULAR_DEFAULT_NULL ||
                $nullableDef === self::NULL_IS_REGULAR_DEFAULT_NOT_SET
            ) {
                // Null is regular value, "has" method should return true
                Assert::assertTrue($isSet);
            } elseif ($nullableDef === self::NULL_MEANS_NOT_SET) {
                // Null means not set, "has" method should return false
                Assert::assertFalse($isSet);
            } else {
                throw new LogicException();
            }
        }

        // Test get* method
        if ($nullableDef === self::NULL_IS_REGULAR_DEFAULT_NULL ||
            $nullableDef === self::NULL_IS_REGULAR_DEFAULT_NOT_SET
        ) {
            // Null is regular value, getter should return it
            Assert::assertNull($this->callGet($valueObject, $property));
        } elseif ($nullableDef === self::NULL_MEANS_NOT_SET) {
            // Null means not set, getter method must thrown PropertyNotSetException
            try {
                $value = $this->callGet($valueObject, $property);
                Assert::fail(sprintf(
                    'Expected %s not thrown when calling get callback of "%s". Returned %s.',
                    'PropertyNotSetException',
                    $property,
                    is_object($value) ? get_class($value) : getType($value) . ': ' . json_encode($value)
                ));
            } catch (PropertyNotSetException $e) {
                // ok, exception was thrown
            }
        } else {
            throw new LogicException();
        }
    }

    /**
     * @dataProvider getValidOptionalRequiredPropertiesDataProvider
     */
    public function testValidOptionalRequiredProperties(array $propertiesToSet, array $additionalReqProperties): void
    {
        // Additional items may be marked as required through the Builder constructor
        // Expect valid data => no error
        $this->buildFromArray($propertiesToSet, $additionalReqProperties);
        $this->expectNotToPerformAssertions();
    }

    /**
     * @dataProvider getInvalidOptionalRequiredPropertiesDataProvider
     */
    public function testInvalidOptionalRequiredProperties(array $propertiesToSet, array $additionalReqProperties): void
    {
        // Additional items may be marked as required through the Builder constructor
        // Expect missing property
        $this->expectException(PropertyNotSetException::class);
        $this->expectExceptionMessageMatches('~Required property ".*" is not set.~');
        $this->buildFromArray($propertiesToSet, $additionalReqProperties);
    }

    public function getValidDataProvider(): iterable
    {
        foreach ($this->getValidInputs() as $index => $allProperties) {
            // All properties
            yield "$index-all-properties" => [$allProperties];

            // Only required properties
            $requiredProperties = array_filter(
                $allProperties,
                fn(string $key) => in_array($key, $this->getAlwaysRequiredProperties(), true),
                ARRAY_FILTER_USE_KEY
            );
            yield "$index-only-required-properties" => [$requiredProperties];

            // Required properties + one PROPERTY
            foreach ($this->getAllProperties() as $property) {
                if (array_key_exists($property, $requiredProperties)) {
                    // Skip, already defined in required properties
                    continue;
                }

                if (!array_key_exists($property, $allProperties)) {
                    // Skip, not set in this test case
                    continue;
                }

                $requiredPlusOne = $requiredProperties;
                $requiredPlusOne[$property] = $allProperties[$property];
                yield "$index-property-$property" => [$requiredPlusOne];
            }

            // Required properties + one nullable PROPERTY
            foreach (array_keys($this->getNullableProperties()) as $property) {
                $requiredPlusNullable = $requiredProperties;
                $requiredPlusNullable[$property] = null;
                yield "$index-nullable-$property" => [$requiredPlusNullable];
            }
        }
    }

    public function getInvalidDataProvider(): iterable
    {
        foreach ($this->getValidInputs() as $index => $allProperties) {
            // Try set not-nullable property to null
            $nullableProperties = array_keys($this->getNullableProperties());
            $notNullableProps = array_diff($this->getAllProperties(), $nullableProperties);
            foreach ($notNullableProps as $notNullableProperty) {
                $invalidProperties = $allProperties;
                $invalidProperties[$notNullableProperty] = null;
                yield "$index-not-nullable-$notNullableProperty" => [
                    TypeError::class,
                    '~null given~',
                    $invalidProperties,
                ];
            }

            // Try unset one of required properties
            $requiredProperties = array_filter(
                $allProperties,
                fn(string $key) => in_array($key, $this->getAlwaysRequiredProperties(), true),
                ARRAY_FILTER_USE_KEY
            );
            foreach (array_keys($requiredProperties) as $missingProperty) {
                $invalidProperties = $requiredProperties;
                unset($invalidProperties[$missingProperty]);
                yield "$index-missing-required-$missingProperty" => [
                    PropertyNotSetException::class,
                    '~Required property ".*" is not set.~',
                    $invalidProperties,
                ];
            }
        }
    }

    public function getEmptyStringNotAllowedDataProvider(): iterable
    {
        foreach ($this->getValidInputs() as $index => $allProperties) {
            foreach ($this->getEmptyStringNotAllowedProperties() as $notEmptyProp) {
                $invalidProperties = $allProperties;
                $invalidProperties[$notEmptyProp] = '';
                yield "$index-empty-str-not-allowed-$notEmptyProp" => [
                    $invalidProperties,
                ];
            }
        }
    }

    public function getEmptyStringConvertToNullDataProvider(): iterable
    {
        foreach ($this->getValidInputs() as $index => $allProperties) {
            foreach ($this->getEmptyStringConvertToNullProperties() as $prop) {
                $properties = $allProperties;
                $properties[$prop] = '';
                yield "$index-empty-str-to-null-$prop" => [
                    $prop,
                    $properties,
                ];
            }
        }
    }

    public function getValidOptionalRequiredPropertiesDataProvider(): iterable
    {
        foreach ($this->getValidInputs() as $index => $allProperties) {
            foreach ($this->getOptionalRequiredProperties() as $prop) {
                if (empty($allProperties[$prop])) {
                    // Property is empty in test dataset, skip
                    continue;
                }

                $optionalReqProps = [$prop];
                yield "$index-optional-required-$prop" => [
                    $allProperties,
                    $optionalReqProps,
                ];
            }
        }
    }

    public function getInvalidOptionalRequiredPropertiesDataProvider(): iterable
    {
        foreach ($this->getValidInputs() as $index => $allProperties) {
            foreach ($this->getOptionalRequiredProperties() as $prop) {
                // Mark property as required, and unset it (setter is not called)
                $optionalReqProps = [$prop];
                $invalidProperties = $allProperties;
                unset($invalidProperties[$prop]);
                yield "$index-optional-required-$prop" => [
                    $invalidProperties,
                    $optionalReqProps,
                ];
            }
        }
    }

    protected function hasDefaultValue(string $property): bool
    {
        $defaults = $this->getDefaultValues();
        return array_key_exists($property, $defaults);
    }

    /**
     * @param string $property
     * @return mixed
     */
    public function getDefaultValue(string $property)
    {
        $defaults = $this->getDefaultValues();
        return $defaults[$property];
    }

    /**
     * @param Builder $builder
     * @param string $property
     * @param mixed $value
     */
    protected function callSet(Builder $builder, string $property, $value): void
    {
        $setCallbacks = $this->getSetCallbacks();
        if (!array_key_exists($property, $setCallbacks)) {
            throw new RuntimeException(sprintf(
                'Set callback for property "%s" is not defined in test.',
                $property
            ));
        }

        $callback = $setCallbacks[$property];
        $result = $callback($builder, $value);
        if (!$result instanceof Builder) {
            throw new RuntimeException(sprintf(
                'Set callback for property "%s" must return Builder, given "%s".',
                $property,
                is_object($result) ? get_class($result) : gettype($result)
            ));
        }
    }

    /**
     * @param ValueObject $valueObject
     * @param string $property
     * @return mixed
     */
    protected function callGet(ValueObject $valueObject, string $property)
    {
        $getCallbacks = $this->getGetCallbacks();
        if (!array_key_exists($property, $getCallbacks)) {
            throw new RuntimeException(sprintf(
                'Get callback for property "%s" is not defined in test.',
                $property
            ));
        }

        $callback = $getCallbacks[$property];
        return $callback($valueObject);
    }

    protected function callHas(ValueObject $valueObject, string $property): ?bool
    {
        $hasCallbacks = $this->getHasCallbacks();
        if (!array_key_exists($property, $hasCallbacks)) {
            return null;
        }

        $callback = $hasCallbacks[$property];
        $result = $callback($valueObject);
        if (!is_bool($result)) {
            throw new RuntimeException(sprintf(
                'Has callback for property "%s" must return bool, given "%s".',
                $property,
                is_object($result) ? get_class($result) : gettype($result)
            ));
        }

        return $result;
    }

    protected function modifyBuilder(Builder $builder): void
    {
    }

    protected function buildFromArray(array $properties, array $additionalRequiredProperties = []): ValueObject
    {
        $builder = $this->createBuilder($additionalRequiredProperties);
        foreach ($properties as $property => $value) {
            $this->callSet($builder, $property, $value);
        }

        $this->modifyBuilder($builder);

        return $builder->build();
    }
}
