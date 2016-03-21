<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 17/02/16
 * Time: 16:03
 */

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Process\Process;

class SSH
{
    public function __construct()
    {

    }

    public function generateKeyPair()
    {
        $process = new Process("ssh-keygen -b 2048 -t rsa -f ./ssh.key -N '' -q");
        $process->run();

        // return public key
        return [
            'private' => file_get_contents('ssh.key'),
            'public' => file_get_contents('ssh.key.pub')
        ];
    }

    public function openTunnel($user, $sshHost, $localPort, $remoteHost, $remotePort, $privateKey, $sshPort = '22')
    {
        $cmd = sprintf(
            'ssh -p %s %s@%s -L %s:%s:%s -i %s -fN -o ExitOnForwardFailure=yes -o StrictHostKeyChecking=no',
            $sshPort,
            $user,
            $sshHost,
            $localPort,
            $remoteHost,
            $remotePort,
            $this->writeKeyToFile($privateKey)
        );

        $process = new Process($cmd);
        $process->setTimeout(60);
        $process->start();

        while ($process->isRunning()) {
            sleep(1);
        }

        if ($process->getExitCode() !== 0) {
            throw new UserException(sprintf(
                "Unable to create ssh tunnel. Output: %s ErrorOutput: %s",
                $process->getOutput(),
                $process->getErrorOutput()
            ));
        }

        return $process;
    }

    private function writeKeyToFile($stringKey)
    {
        $fileName = 'ssh.' . microtime(true) . '.key';
        file_put_contents(ROOT_PATH . '/' . $fileName, $stringKey);
        chmod($fileName, 0600);
        return realpath($fileName);
    }

}
