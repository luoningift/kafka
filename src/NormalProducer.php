<?php
namespace HKY\Kafka;

use HKY\Kafka\Client\Config\ProducerConfig;
use HKY\Kafka\Client\Producer;

/**
 * use not hyperf environment
 * Class NormalProducer
 * @package HKY\Kafka
 */
class NormalProducer {

    private static $instance = [];

    private function __construct()
    {
    }

    /**
     * @param string $metaBrokerList
     * @param int $ack
     * @param string $version
     * @return Producer
     * @throws Client\Exception\Config
     * @throws Client\Exception\ConnectionException
     * @throws Client\Exception\Exception
     */
    public static function getInstance($metaBrokerList, $ack = 1, $version = '0.9.0') {

        $key = sha1($metaBrokerList. $ack . $version);
        if (isset(static::$instance[$key])) {
            return static::$instance[$key];
        }

        $config = new ProducerConfig();
        $config->setMetadataBrokerList($metaBrokerList);
        $config->setBrokerVersion($version);
        $config->setRequiredAck($ack);

        return static::$instance[$key] = new Producer($config);
    }
}