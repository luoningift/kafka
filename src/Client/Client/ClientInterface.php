<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/21
 * Time: 上午9:11
 */
namespace HKY\Kafka\Client\Client;

use HKY\Kafka\Client\Exception\ConnectionException;
use HKY\Kafka\Client\Exception\Exception;

interface ClientInterface
{
    /**
     * @return bool
     * @throws ConnectionException
     * @throws Exception
     */
    public function connect(): bool;

    /**
     * @return bool
     */
    public function isConnected(): bool;

    /**
     * @param null|string $data
     * @param int         $tries
     * @return mixed
     * @throws ConnectionException
     * @throws Exception
     */
    public function send(?string $data = null, $tries = 2);

    /**
     * @param null|string $data
     * @param int         $tries
     * @return mixed
     * @throws ConnectionException
     * @throws Exception
     */
    public function sendWithNoResponse(?string $data = null, $tries = 2);

    /**
     * @param float $timeout
     * @return string
     * @throws ConnectionException
     * @throws Exception
     */
    public function recv(float $timeout = -1): string;

    public function close();

    /**
     * @return bool
     * @throws ConnectionException
     * @throws Exception
     */
    public function reconnect();
}
