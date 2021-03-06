<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://doc.hyperf.io
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */

namespace HKY\Kafka\Message;

use HKY\Kafka\Atomic\VariableAtomic;
use HKY\Kafka\Client\Consumer\Process;
use HKY\Kafka\Frequency\FrequencyInterface;
use Psr\Container\ContainerInterface;

abstract class ConsumerMessage implements ConsumerMessageInterface
{
    /**
     * @var ContainerInterface
     */
    public $container;

    /**
     * @var bool
     */
    protected $enable = true;

    /**
     * @var int
     */
    protected $maxConsumption = 0;

    protected $poolName = '';

    protected $consumerNums = 1;

    protected $topicName = '';

    protected $group = '';

    /**
     * @var VariableAtomic
     */
    protected $atomic;

    protected $isSingalExit = false;

    protected $maxBytes = 65535;

    protected $maxPollRecord = 5;

    //控制是否消费消息
    protected $isConsume = true;

    protected $bufferNumber = 10;

    public function setMaxBytes(int $maxBytes)
    {
        $this->maxBytes = $maxBytes;
        return $this;
    }

    public function getMaxBytes(): int
    {
        return intval($this->maxBytes);
    }

    public function setSingalExit()
    {
        $this->isSingalExit = true;
        return $this;
    }

    public function getSingalExit()
    {
        return $this->isSingalExit;
    }

    public function initAtomic()
    {
        if (!$this->atomic) {
            $this->atomic = new VariableAtomic();
        }
        $this->atomic->set(0);
    }

    public function atomicMessage(Process $process, $topic, $partition, $message)
    {
        $this->atomic->add(1);
        $this->consume($topic, $partition, $message);
        if ($this->checkAtomic()) {
            $process->stop();
        }
    }

    public function checkAtomic()
    {
        if ($this->getMaxConsumption() == -1) {
            return false;
        }
        return $this->atomic->get() >= $this->getMaxConsumption();
    }

    public function setTopic(string $queue)
    {
        $this->topicName = $queue;
        return $this;
    }

    public function getTopic(): string
    {
        return $this->topicName;
    }

    public function setPoolName(string $poolName)
    {
        $this->poolName = $poolName;
        return $this;
    }

    public function getPoolName(): string
    {
        return $this->poolName;
    }

    public function setConsumerNums(int $consumerNums)
    {
        $this->consumerNums = $consumerNums;
        return $this;
    }

    public function getConsumerNums(): int
    {
        return $this->consumerNums;
    }

    public function isEnable(): bool
    {
        return $this->enable;
    }

    public function setEnable(bool $enable): self
    {
        $this->enable = $enable;
        return $this;
    }

    public function getMaxConsumption(): int
    {
        return $this->maxConsumption;
    }

    public function setMaxConsumption(int $maxConsumption)
    {
        $this->maxConsumption = $maxConsumption;
        return $this;
    }

    public function setGroup(string $group)
    {
        $this->group = $group;
        return $this;
    }

    public function getGroup(): string
    {
        return $this->group;
    }

    public function getMaxPollRecord(): int
    {

        return $this->maxPollRecord;
    }

    public function setMaxPollRecord(int $maxPollRecord)
    {
        $this->maxPollRecord = $maxPollRecord;
        return $this;
    }

    public function setOffConsume()
    {
        $this->isConsume = false;
        return $this;
    }

    public function setOnConsume()
    {
        $this->isConsume = true;
        return $this;
    }

    public function getConsumeControl() {

        return $this->isConsume;
    }

    public function init()
    {
    }
}
