<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 下午1:49
 */
namespace HKY\Kafka\Client\Heartbeat;

use HKY\Kafka\Client\BaseProcess;
use HKY\Kafka\Client\Broker;
use HKY\Kafka\Client\Config\ConsumerConfig;
use HKY\Kafka\Client\Consumer\Assignment;
use HKY\Kafka\Client\Exception\ConnectionException;
use HKY\Kafka\Client\Protocol;

class Process extends BaseProcess
{
    public function __construct(ConsumerConfig $config, Assignment $assignment, Broker $broker)
    {
        parent::__construct($config);

        $this->setAssignment($assignment);
        $this->setBroker($broker);
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \HKY\Kafka\Client\Exception\Config
     * @throws \HKY\Kafka\Client\Exception\Exception
     */
    public function heartbeat(): array
    {
        $connect = $this->getBroker()->getMetaConnectByBrokerId($this->getBroker()->getGroupBrokerId());
        if ($connect === null) {
            throw new ConnectionException();
        }

        $params = [
            'group_id'      => $this->getConfig()->getGroupId(),
            'generation_id' => $this->getAssignment()->getGenerationId(),
            'member_id'     => $this->getAssignment()->getMemberId(),
        ];

        $requestData = Protocol::encode(Protocol::HEART_BEAT_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::HEART_BEAT_REQUEST, substr($data, 8));
        return $ret;
    }
}
