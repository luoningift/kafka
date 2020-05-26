<?php
declare(strict_types=1);

namespace HKY\Kafka;

use HKY\Kafka\Client\Config\ProducerConfig;
use Hyperf\Contract\ConnectionInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Pool\Connection as BaseConnection;
use Hyperf\Pool\Exception\ConnectionException;
use Hyperf\Pool\Pool;
use Hyperf\Utils\ApplicationContext;
use Psr\Container\ContainerInterface;
use Swoole\Coroutine\Channel as CoChannel;
use Swoole\Timer;

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

    protected $initLength = 100;

    protected $initMaxLength = 50;


    /**
     * @var
     */
    protected $channel = null;

    protected $timer = -1;

    public function __construct(ContainerInterface $container, Pool $pool, array $config)
    {
        parent::__construct($container, $pool);
        $this->config = array_replace($this->config, $config);
        $this->channel = new CoChannel($this->initLength);
        $this->addTimer();
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
     * 带缓存buffer
     * @param $data
     * @return boolean
     */
    public function lazySend($data) {

        if ($this->channel->length() >= $this->initLength) {
            $this->batchSend();
        }
        foreach($data as $one) {
            $this->channel->push($one, 1.5);
        }
        return true;
    }

    /**
     * 批量发送
     */
    protected function batchSend() {

        $queueCount = $this->channel->length();
        while($queueCount > 0) {
            $popCount = $queueCount >= 30 ? 30 : $queueCount;
            $queueCount = $queueCount - $popCount;
            go(function() use ($popCount) {
                $send = [];
                for($i = 0; $i < $popCount; $i++) {
                    $popData = $this->channel->pop(0.05);
                    if (is_array($popData) && $popData) {
                        $send[] = $popData;
                    } else {
                        break;
                    }
                }
                if ($send) {
                    //获取连接池消费当前消息
                    ApplicationContext::getContainer()->get(ProducerFactory::class)->get($this->config['poolName'])->send($send);
                }
            });
        }
    }

    /**
     * 定时300ms批量发送数据
     */
    protected function addTimer() {

        $this->timer = Timer::tick(300, function() {
            $this->batchSend();
        });
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
        $this->batchSend();
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

    /**
     * 清理定时器
     */
    public function __destruct()
    {
        try {
            Timer::clear($this->timer);
        } catch (\Exception $e) {}
    }
}
