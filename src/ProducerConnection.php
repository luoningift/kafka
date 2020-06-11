<?php
declare(strict_types=1);

namespace HKY\Kafka;

use HKY\Kafka\Client\Config\ProducerConfig;
use Hyperf\Contract\ConnectionInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Pool\Connection as BaseConnection;
use Hyperf\Pool\Exception\ConnectionException;
use Hyperf\Pool\Pool;
use Psr\Container\ContainerInterface;

/**
 * Class ProducerConnection
 * @package HKY\Kafka
 *
 * @method send($data)
 */
class ProducerConnection extends BaseConnection implements ConnectionInterface
{
    /**
     * @var array
     */
    protected $config = [
        'broker_list' => '127.0.0.1:9092,127.0.0.1:9093',
        'ack' => 1,
        'version' => '0.9.0',
        'poolName' => '',
        'pool' => [
            'min_connections' => 1,
            'max_connections' => 10,
            'connect_timeout' => 1.0,
            'wait_timeout' => 3.0,
            'heartbeat' => -1,
            'max_idle_time' => 60,
        ],
    ];

    /**
     * @var \HKY\Kafka\Client\Producer
     */
    protected $producer = null;

    /**
     * @var
     */
    protected $channel = null;

    public function __construct(ContainerInterface $container, Pool $pool, array $config)
    {
        parent::__construct($container, $pool);
        $this->config = array_replace($this->config, $config);
    }

    public function __call($name, $arguments)
    {
        try {
            $result = $this->producer->{$name}(...$arguments);
        } catch (\Throwable $exception) {
            $result = $this->retry($name, $arguments, $exception);
        }
        return $result;
    }

    /**
     * 重新建立链接
     * @return bool
     * @throws
     */
    public function reconnect(): bool
    {
        $config = new ProducerConfig();
        $config->setMetadataBrokerList($this->config['broker_list']);
        $config->setBrokerVersion($this->config['version']);
        $config->setRequiredAck($this->config['ack']);
        $this->producer = new \HKY\Kafka\Client\Producer($config);
        $this->lastUseTime = microtime(true);
        return true;
    }

    /**
     * 关闭链接
     * @return bool
     */
    public function close(): bool
    {
        $this->producer->close();
        $this->producer = null;
        return true;
    }

    /**
     * 获取活跃链接
     * @return $this
     * @throws ConnectionException
     */
    public function getActiveConnection()
    {
        if ($this->check()) {
            return $this;
        }
        if (!$this->reconnect()) {
            throw new ConnectionException('Connection reconnect failed.');
        }
        return $this;
    }

    /**
     * 归还链接到
     */
    public function release(): void
    {
        parent::release();
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
        $logger = $this->container->get(StdoutLoggerInterface::class);
        $logger->warning(sprintf('kafka::__call failed, because ' . $exception->getMessage()));
        try {
            $this->reconnect();
            $result = $this->producer->{$name}(...$arguments);
        } catch (\Throwable $exception) {
            $this->lastUseTime = 0.0;
            throw $exception;
        }
        return $result;
    }
}
