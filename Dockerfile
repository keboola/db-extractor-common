#VERSION 1.0.0
FROM php:7.1
MAINTAINER Miro Cillik <miro@keboola.com>

ENV DEBIAN_FRONTEND noninteractive

# Install dependencies
RUN apt-get update -q \
  && apt-get install mysql-client ssh git zip -y --no-install-recommends

RUN docker-php-ext-install pdo_mysql

ADD . /code
WORKDIR /code
RUN echo "memory_limit = -1" >> /etc/php.ini

RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

RUN composer install --no-interaction

CMD php ./vendor/bin/phpunit
