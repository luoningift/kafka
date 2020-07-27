<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/10/14
 * Time: 下午5:40
 */
namespace HKY\Kafka\Client\SyncMeta;

use HKY\Kafka\Client\BaseProcess;
use HKY\Kafka\Client\Exception\ConnectionException;
use HKY\Kafka\Client\Exception\Exception;
use HKY\Kafka\Client\Protocol;

class Process extends BaseProcess
{
    /**
     * @return \HKY\Kafka\Client\Broker
     * @throws ConnectionException
     * @throws Exception
     * @throws \HKY\Kafka\Client\Exception\ErrorCodeException
     */
    public function syncMeta()
    {
        $brokerList = $this->config->getMetadataBrokerList();
        $brokerHost = [];
        foreach (explode(',', $brokerList) as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = trim($val);
            }
        }
        if (count($brokerHost) === 0) {
            throw new Exception('No valid broker configured');
        }

        shuffle($brokerHost);
        $broker = $this->getBroker();
        foreach ($brokerHost as $host) {
            $client = $broker->getMetaConnectByBrokerId($host);
            if (! $client->isConnected()) {
                continue;
            }
            $params = [];

            $requestData = Protocol::encode(Protocol::METADATA_REQUEST, $params);
            $data = $client->send($requestData);
            $dataLen = Protocol\Protocol::unpack(Protocol\Protocol::BIT_B32, substr($data, 0, 4));
            $correlationId = Protocol\Protocol::unpack(Protocol\Protocol::BIT_B32, substr($data, 4, 4));
            // 0-4字节是包头长度
            // 4-8字节是correlationId
            $result = Protocol::decode(Protocol::METADATA_REQUEST, substr($data, 8));
            if (! isset($result['brokers'], $result['topics'])) {
                throw new Exception("Get metadata is fail, brokers or topics is null.");
            }

            // 更新 topics和brokers
            if (empty($result['brokers'])) {
                continue;
            }
            $broker->setData($result['topics'], $result['brokers']);
            $broker->setGroupBrokerId($broker->getGroupCoordinatorByGroupId($this->config->getGroupId()));
            break;
        }
        return $broker;
    }
}
