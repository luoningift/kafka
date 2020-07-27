<?php
namespace HKY\Kafka\Client;

use HKY\Kafka\Client\Config\Config;
use HKY\Kafka\Client\Exception\ConnectionException;
use HKY\Kafka\Client\Exception\Exception;
use Hyperf\Contract\StdoutLoggerInterface;
use Swoole;
use Swoole\Coroutine\Client as CoroutineClient;

class ClientConnection
{
    /**
     * @var Client
     */
    private $client;

    /**
     * CommonClient constructor.
     * @param string $host
     * @param int $port
     * @param Config|null $config
     * @param SaslMechanism|null $saslProvider
     */
    public function __construct(string $host, int $port, ?Config $config = null, ?SaslMechanism $saslProvider = null)
    {
        $this->client = new Client($host, $port, $config, $saslProvider);
    }

    public function __call($name, $arguments)
    {
        try {
            $result = $this->client->{$name}(...$arguments);
        } catch (\Throwable $exception) {
            $result = $this->retry($name, $arguments, $exception);
        }
        return $result;
    }

    /**
     * 失败重试
     * @param $name
     * @param $arguments
     * @param \Throwable $exception
     * @return mixed
     * @throws \Throwable
     */
    protected function retry($name, $arguments, \Throwable $exception)
    {
        try {
            $this->client->reconnect();
            $result = $this->client->{$name}(...$arguments);
        } catch (\Throwable $exception) {
            throw new ConnectionException($exception->getMessage());
        }
        return $result;
    }
}