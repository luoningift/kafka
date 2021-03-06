<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午11:00
 */
namespace HKY\Kafka\Client;

use HKY\Kafka\Client\Config\ConsumerConfig;
use HKY\Kafka\Client\Consumer\Process;
use HKY\Kafka\Message\ConsumerMessageInterface;

class Consumer
{
    /**
     * @var Process|null
     */
    private $process;

    /**
     * Consumer constructor.
     * @param ConsumerConfig $config
     * @throws Exception\Exception
     */
    public function __construct(ConsumerConfig $config)
    {
        $this->process = new Process($config);
    }


    /**
     * @param ConsumerMessageInterface $consumerMessage
     * @param float         $breakTime
     * @param int           $maxCurrency
     * @throws \Throwable
     */
    public function subscribe(ConsumerMessageInterface $consumerMessage, $breakTime = 0.01, $maxCurrency = 128)
    {
        if (!class_exists('\Swoole\Coroutine\Client') || \Swoole\Coroutine::getCid() < 0) {
            throw new \RuntimeException('only support swoole environment and in coroutine');
        }
        $this->process->subscribe($consumerMessage, $breakTime, $maxCurrency);
    }

    public function stop()
    {
        $this->process->stop();
    }

    /**
     * 释放链接
     */
    public function close() {
        $this->process->close();
    }
}
