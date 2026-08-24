FROM php:8.2-cli-trixie

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER=1
ENV COMPOSER_PROCESS_TIMEOUT=3600

WORKDIR /code/

COPY patches /code/patches
COPY docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    locales \
    unzip \
    ssh \
    wget \
    curl \
    unzip \
    libzip-dev \
    libicu-dev \
    && rm -r /var/lib/apt/lists/* \
    && sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
    && locale-gen \
    && chmod +x /tmp/composer-install.sh \
    && /tmp/composer-install.sh

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# INTL
RUN docker-php-ext-configure intl \
    && docker-php-ext-install intl

# PDO mysql
RUN docker-php-ext-install pdo_mysql

# Add debugger
RUN pecl channel-update pecl.php.net \
    && pecl config-set php_ini /usr/local/etc/php.ini \
    && pecl install xdebug \
    && docker-php-ext-enable xdebug


## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/

# The only advisory-affected package is guzzle 6, pulled solely by the dev/test dep
# ihsw/toxiproxy-php-client ^2.0 (its v3 needs PHP 8.3; we are on 8.2). It never ships in the runtime.
# Composer 2.9+ excludes advisory-affected versions from the resolution pool, breaking this lockless
# install; the opt-out is scoped to the build-time installs only (the resolve happens here and writes the
# lock), so the runtime `composer ci` below keeps full security blocking.
#
# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN COMPOSER_NO_SECURITY_BLOCKING=1 composer install $COMPOSER_FLAGS --no-scripts --no-autoloader

# Copy rest of the app
COPY . /code/

# Run normal composer - all deps are cached already
RUN COMPOSER_NO_SECURITY_BLOCKING=1 composer install $COMPOSER_FLAGS
