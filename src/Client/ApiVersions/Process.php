<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/20
 * Time: 上午10:17
 */
namespace HKY\Kafka\Client\ApiVersions;

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
    public function apiVersions()
    {
        $connect = $this->getBroker()->getMetaConnectByBrokerId($this->getBroker()->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

        $params     = [];

        $requestData = Protocol::encode(Protocol::API_VERSIONS_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::API_VERSIONS_REQUEST, substr($data, 8));

        return $ret;
    }
}
