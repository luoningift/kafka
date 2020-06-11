<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午5:47
 */
namespace HKY\Kafka\Client\SaslHandShake;

use HKY\Kafka\Client\BaseProcess;
use HKY\Kafka\Client\Broker;
use HKY\Kafka\Client\Config\Config;
use HKY\Kafka\Client\Exception\ConnectionException;
use HKY\Kafka\Client\Protocol;

class Process extends BaseProcess
{
    public function __construct(Config $config, Broker $broker)
    {
        parent::__construct($config);
        $this->setBroker($broker);
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \HKY\Kafka\Client\Exception\Exception
     */
    public function handShake()
    {
        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

        $params = [
            $this->config->getSaslMechanism()
        ];

        $requestData = Protocol::encode(Protocol::SASL_HAND_SHAKE_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::SASL_HAND_SHAKE_REQUEST, substr($data, 8));

        return $ret;
    }
}
