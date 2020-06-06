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

use HKY\Kafka\Client\Consumer\Process;

interface ConsumerMessageInterface
{

    public function initAtomic();

    public function atomicMessage(Process $process, $topic, $partition, $message);

    public function checkAtomic();

    public function consume($topic, $partition, $message): string;

    public function setTopic(string $queue);

    public function getTopic(): string;

    public function setGroup(string $queue);

    public function getGroup(): string;

    public function setConsumerNums(int $consumerNums);

    public function getConsumerNums() : int;

    public function setPoolName(string $poolName);

    public function getPoolName() : string;

    public function isEnable(): bool;

    public function setEnable(bool $enable);

    public function getMaxConsumption(): int;

    public function setMaxConsumption(int $maxConsumption);
}
