<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 17/02/16
 * Time: 16:03
 */

namespace Keboola\DbExtractor;

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

    public function openTunnel($user, $sshHost, $localPort, $remoteHost, $remotePort, $privateKey)
    {
        $cmd = sprintf(
            'ssh %s@%s -L %s:%s:%s -i %s -fN -o ExitOnForwardFailure=yes -o StrictHostKeyChecking=no',
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
