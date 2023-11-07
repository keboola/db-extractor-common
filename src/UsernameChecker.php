<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\Component\Config\BaseConfig;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\BadUsernameException;
use Psr\Log\LoggerInterface;

class UsernameChecker
{
    private LoggerInterface $logger;

    private array $config;

    private array $featureConfig;

    public function __construct(LoggerInterface $logger, BaseConfig $config)
    {
        $this->logger = $logger;
        $this->config = $config->getData();
        $this->featureConfig = $this->config['image_parameters']['checkUsername'] ?? [];
    }

    public function checkUsername(): void
    {
        if (!$this->isEnabled()) {
            return;
        }

        $realUsername = $this->getRealUsername();
        $dbUsername = $this->getDbUsername();

        // The check is not performed for service accounts
        if ($this->isServiceAccount($dbUsername)) {
            $this->logger->info(sprintf(
                'Database username "%s" is service account, username check skipped.',
                $dbUsername,
            ));
            return;
        }

        // Compare real username (user running config) and database username
        if ($realUsername !== $dbUsername) {
            throw new BadUsernameException(
                sprintf(
                    'Your username "%s" does not have permission to ' .
                    'run configuration with the database username "%s"',
                    $realUsername,
                    $dbUsername,
                ),
            );
        }

        $this->logger->info(sprintf(
            'Your username "%s" and database username are same. Running allowed.',
            $realUsername,
        ));
    }

    protected function isEnabled(): bool
    {
        $enabled = $this->featureConfig['enabled'] ?? false;
        if (!is_bool($enabled)) {
            throw new ApplicationException(
                'Value "image_parameters.check_username.enabled" must be boolean.',
            );
        }

        return $enabled;
    }

    protected function getDbUsername(): string
    {
        return $this->config['parameters']['db']['user'];
    }

    protected function getRealUsername(): string
    {
        return (string) getenv('KBC_REALUSER');
    }

    protected function isServiceAccount(string $username): bool
    {
        // Service account can be defined by "serviceAccountRegexp" or inverted "userAccountRegexp".
        $serviceAccRegexp = $this->featureConfig['serviceAccountRegexp'] ?? null;
        $userAccRegexp = $this->featureConfig['userAccountRegexp'] ?? null;

        if (isset($serviceAccRegexp) && isset($userAccRegexp)) {
            throw new ApplicationException(
                'Only one of "image_parameters.check_username.serviceAccountRegexp" ' .
                'or "image_parameters.check_username.userAccountRegexp" must be set.',
            );
        }

        if (isset($serviceAccRegexp)) {
            return preg_match($serviceAccRegexp, $username) === 1;
        }

        if (isset($userAccRegexp)) {
            return preg_match($userAccRegexp, $username) !== 1;
        }

        return false;
    }
}
