<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/21
 * Time: 上午9:11
 */
namespace HKY\Kafka\Client;

use HKY\Kafka\Client\Client\ClientInterface;
use HKY\Kafka\Client\Client\CoClient;
use HKY\Kafka\Client\Client\NormalClient;
use HKY\Kafka\Client\Config\Config;
use HKY\Kafka\Client\Exception\ConnectionException;
use HKY\Kafka\Client\Exception\Exception;

class Client implements ClientInterface
{

    /**
     * @var ClientInterface | null
     */
    private $client;

    /**
     * CommonClient constructor.
     * @param string             $host
     * @param int                $port
     * @param Config|null        $config
     * @param SaslMechanism|null $saslProvider
     * @throws Exception
     */
    public function __construct(string $host, int $port, ?Config $config = null, ?SaslMechanism $saslProvider = null)
    {
        if (class_exists('\Swoole\Coroutine\Client')) {
            $this->client = new CoClient($host, $port, $config, $saslProvider);
        } else {
            $this->client = new NormalClient($host, $port, $config, $saslProvider);
        }
    }

    /**
     * @return bool
     * @throws ConnectionException
     * @throws Exception
     */
    public function connect(): bool
    {
        return (bool)$this->client->connect();
    }

    /**
     * @return bool
     */
    public function isConnected(): bool
    {
        return $this->client->isConnected();
    }

    /**
     * @param null|string $data
     * @param int         $tries
     * @return mixed
     * @throws ConnectionException
     * @throws Exception
     */
    public function send(?string $data = null, $tries = 2)
    {
        return $this->client->send($data, $tries);
    }

    /**
     * @param null|string $data
     * @param int         $tries
     * @return mixed
     * @throws ConnectionException
     * @throws Exception
     */
    public function sendWithNoResponse(?string $data = null, $tries = 2)
    {
        return $this->client->sendWithNoResponse($data, $tries);
    }

    /**
     * @param float $timeout
     * @return string
     * @throws ConnectionException
     * @throws Exception
     */
    public function recv(float $timeout = -1): string
    {
        return $this->client->recv($timeout);
    }

    public function close()
    {
        $this->client->close();
    }

    /**
     * @return bool
     * @throws ConnectionException
     * @throws Exception
     */
    public function reconnect()
    {
        return $this->client->reconnect();
    }
}
