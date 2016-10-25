#VERSION 1.0.0
FROM ubuntu:14.04
MAINTAINER Erik Zigo <erik@keboola.com>

# Install dependencies
RUN apt-get update && \
    apt-get install -y \
      curl \
      git \
      php5 \
      php5-cli \
      php5-json \
      php5-mysqlnd

ADD . /code
WORKDIR /code
RUN echo "memory_limit = -1" >> /etc/php.ini
RUN echo "date.timezone=UTC" >> /etc/php.ini
RUN echo "mysql.allow_local_infile = On" >> /etc/php.ini

RUN curl -sS https://getcomposer.org/installer | php && \
	mv composer.phar /usr/local/bin/composer

RUN composer install --no-interaction


ENTRYPOINT php ./vendor/bin/phpunit
