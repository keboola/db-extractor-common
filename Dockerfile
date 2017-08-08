#VERSION 1.0.0
FROM php:7.1-fpm
MAINTAINER Miro Cillik <miro@keboola.com>

# Install dependencies
RUN apt-get update -q \
  && apt-get install mysql-client ssh git zip wget curl make git patch unzip bzip2 time libzip-dev -y --no-install-recommends

RUN docker-php-ext-install pdo_mysql

RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

ADD . /code
WORKDIR /code

# Main
RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini
RUN echo "date.timezone = \"Europe/Prague\"" >> /usr/local/etc/php/php.ini

#install
RUN composer install --no-interaction
