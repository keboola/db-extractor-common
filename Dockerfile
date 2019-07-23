#VERSION 1.0.0
FROM db-ex-common-sshproxy AS sshproxy
FROM php:7.1-fpm

ENV DEBIAN_FRONTEND noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"

# Install dependencies
RUN apt-get update -q \
  && apt-get install default-mysql-client ssh git zip wget curl make patch unzip bzip2 time libzip-dev -y --no-install-recommends

RUN docker-php-ext-install pdo_mysql

# use recommended php settings
COPY docker/php-prod.ini /usr/local/etc/php/php.ini

# add debugger
RUN pecl channel-update pecl.php.net \
    && pecl config-set php_ini /usr/local/etc/php.ini \
    && pecl install xdebug \
    && docker-php-ext-enable xdebug

# install composer
COPY docker/composer-install.sh /tmp/composer-install.sh
RUN chmod +x /tmp/composer-install.sh
RUN /tmp/composer-install.sh

WORKDIR /code

## deps always cached unless changed
# First copy only composer files
COPY composer.* /code/
# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY . /code/
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

COPY --from=sshproxy /root/.ssh /root/.ssh
