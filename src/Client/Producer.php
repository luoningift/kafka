<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午11:00
 */
namespace HKY\Kafka\Client;

use HKY\Kafka\Client\Config\ProducerConfig;
use HKY\Kafka\Client\Producer\Process;

class Producer
{
    private $process;

    /**
     * Producer constructor.
     * @param ProducerConfig $config
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function __construct(ProducerConfig $config)
    {
        $this->process = new Process($config);
    }

    /**
     * @param array $data
     * @return array|null
     * @throws Exception\Exception
     * @throws Exception\InvalidRecordInSet
     */
    public function send(array $data): ?array
    {
        return $this->process->send($data);
    }
}
