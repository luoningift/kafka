<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午8:48
 */
namespace HKY\Kafka\Client\Fetch;

use HKY\Kafka\Client\BaseProcess;
use HKY\Kafka\Client\Broker;
use HKY\Kafka\Client\Config\ConsumerConfig;
use HKY\Kafka\Client\Consumer\Assignment;
use HKY\Kafka\Client\Exception;
use HKY\Kafka\Client\Exception\ConnectionException;
use HKY\Kafka\Client\Protocol;

class Process extends BaseProcess
{
    /**
     * Process constructor.
     * @param ConsumerConfig $config
     * @param Assignment     $assignment
     * @param Broker         $broker
     * @throws Exception\Exception
     */
    public function __construct(ConsumerConfig $config, Assignment $assignment, Broker $broker)
    {
        parent::__construct($config);

        $this->setAssignment($assignment);
        $this->setBroker($broker);
    }

    /**
     * @param array $offsets
     * @param int $maxPollRecord
     * @return array
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function fetch(array $offsets = [], $maxPollRecord = 1): array
    {
        if (empty($offsets)) {
            return [];
        }
        $shuffleTopics = $this->getAssignment()->getTopics();
        uksort($shuffleTopics, function ($a, $b) {return mt_rand(-1000, 1000);});
        $res = [];
        $hasPollRecord = 0;
        foreach ($shuffleTopics as $nodeId => $topics) {
            $data = [];
            foreach ($topics as $topicName => $partitions) {
                $item = [
                    'topic_name' => $topicName,
                    'partitions' => [],
                ];
                foreach ($offsets[$topicName] as $partId => $offset) {
                    if (in_array($partId, $partitions['partitions'])) {
                        $item['partitions'][] = [
                            'partition_id' => $partId,
                            'offset' => $offset > 0 ? $offset : 0,
                            'max_bytes' => $this->getConfig()->getMaxBytes(),
                        ];
                    }
                }
                $data[] = $item;
            }
            $params = array(
                'max_wait_time'     => $this->getConfig()->getMaxWaitTime(),
                'min_bytes'         => $this->getConfig()->getMinBytes(),
                'replica_id'        => -1,
                'data'              => $data,
            );
            $connect = $this->getBroker()->getFetchConnectByBrokerId($nodeId);
            if ($connect === null) {
                throw new ConnectionException();
            }
            $requestData = Protocol::encode(Protocol::FETCH_REQUEST, $params);
            $data = $connect->send($requestData);
            $valueRet = Protocol::decode(Protocol::FETCH_REQUEST, substr($data, 8));
            $allTopicName = [];
            $throttleTime = [];
            foreach ($valueRet['topics'] as $keyTopics => $valueTopics) {
                $allTopicName['topics'][$valueTopics['topicName']]['topicName'] = $valueTopics['topicName'];
                foreach ($valueTopics['partitions'] as $keyPartitions => $valuePartitions) {
                    if (!isset($throttleTime[$valueTopics['topicName']])) {
                        $throttleTime[$valueTopics['topicName']] = $valueRet['throttleTime'];
                    }
                    $allTopicName['topics'][$valueTopics['topicName']]['partitions'][] = $valuePartitions;
                }
            }
            foreach ($allTopicName['topics'] as $keyRes => $valueRes) {
                if (!isset($res['throttleTime'])) {
                    $res['throttleTime'] = $throttleTime[$keyRes];
                }
                $res['topics'][] = $valueRes;
                foreach($valueRes['partitions'] as $part) {
                    $hasPollRecord += count($part['messages']??[]);
                    if ($hasPollRecord >= $maxPollRecord) {
                        return $res;
                    }
                }
            }
        }
        return $res;
    }
}
