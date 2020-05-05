<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\ValueObject;

use TypeError;
use RuntimeException;
use Keboola\DbExtractor\Exception\PropertyNotSetException;
use PHPUnit\Framework\Assert;
use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\ValueObject;
use PHPUnit\Framework\TestCase;

/**
 * This class tests a ValueObject constructor, has* and get* methods.
 */
abstract class BaseValueObjectTest extends TestCase
{
    public const NULL_MEANS_NOT_SET = 'null_means_not_set_default_not_set';
    public const NULL_IS_REGULAR_DEFAULT_NULL = 'null_is_regular_default_null';
    public const NULL_IS_REGULAR_DEFAULT_NOT_SET = 'null_is_regular_default_not_set';

    abstract public function createValueObjectFromArray(array $properties): ValueObject;

    /**
     * Get ValueObject's properties that will be tested
     * @return array|string[]
     */
    abstract public function getAllProperties(): array;


    /**
     * Get properties with allowed null value
     * @return array|string[]
     */
    abstract public function getNullableProperties(): array;

    /**
     * Get properties for which an attempt to set an empty string will result in an error.
     * @return array|string[]
     */
    abstract public function getEmptyStringNotAllowedProperties(): array;

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
    public function testValid(array $properties): void
    {
        $valueObject = $this->createValueObjectFromArray($properties);

        foreach ($this->getAllProperties() as $property) {
            $nullableDef = $this->getNullableProperties()[$property] ?? null;

            // Expectations
            if (array_key_exists($property, $properties)) {
                // If - property is set by this test (see buildFromArray method)
                $expectedValue = $properties[$property];
                // Property is taken as "is set", when value is not null OR null value is allowed
                $expectedIsSet =
                    $expectedValue !== null ||
                    in_array($nullableDef, [
                        self::NULL_IS_REGULAR_DEFAULT_NULL,
                        self::NULL_IS_REGULAR_DEFAULT_NOT_SET,
                    ], true);
            } else {
                // Else - property is not set by test
                $expectedValue = null;
                // Property is taken as "is set", when value is not null OR null value is default
                $expectedIsSet = $nullableDef === self::NULL_IS_REGULAR_DEFAULT_NULL;
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
    public function testInvalid(string $exceptionClass, string $exceptionMsg, array $properties): void
    {
        $this->expectException($exceptionClass);
        $this->expectExceptionMessageMatches($exceptionMsg);
        $this->createValueObjectFromArray($properties);
    }

    public function getValidDataProvider(): iterable
    {
        foreach ($this->getValidInputs() as $index => $allProperties) {
            // Test valid inputs
            yield "$index-valid-input" => [$allProperties];

            // Test nullable properties
            foreach (array_keys($this->getNullableProperties()) as $prop) {
                $properties = $allProperties;
                $properties[$prop] = null;
                yield "$index-nullable-$prop" => [$properties];
            }
        }
    }

    public function getInvalidDataProvider(): iterable
    {
        foreach ($this->getValidInputs() as $index => $allProperties) {
            // Test null not allowed
            $notNullProperties = array_diff($this->getAllProperties(), array_keys($this->getNullableProperties()));
            foreach ($notNullProperties as $prop) {
                $properties = $allProperties;
                $properties[$prop] = null;
                yield "$index-not-nullable-$prop" => [
                    TypeError::class,
                    '~null given~',
                    $properties,
                ];
            }

            // Test empty string not allowed
            foreach ($this->getEmptyStringNotAllowedProperties() as $prop) {
                $properties = $allProperties;
                $properties[$prop] = '';
                yield "$index-empty-str-not-allowed-$prop" => [
                    InvalidArgumentException::class,
                    '~cannot be empty~',
                    $properties,
                ];
            }
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
}
