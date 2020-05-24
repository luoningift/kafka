<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午8:56
 */
namespace HKY\Kafka\Client;

use HKY\Kafka\Client\Config\ConsumerConfig;
use HKY\Kafka\Client\Consumer\Assignment;
use HKY\Kafka\Client\Fetch\Process;

class Fetch
{
    private $process;

    /**
     * Fetch constructor.
     * @param ConsumerConfig $config
     * @param Assignment     $assignment
     * @param Broker         $broker
     * @throws Exception\Exception
     */
    public function __construct(ConsumerConfig $config, Assignment $assignment, Broker $broker)
    {
        $this->process = new Process($config, $assignment, $broker);
    }

    /**
     * @return array|null
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function fetch(): ?array
    {
        return $this->process->fetch();
    }
}
