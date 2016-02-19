#!/usr/bin/env bash

composer install -n

php ./vendor/bin/phpunit
