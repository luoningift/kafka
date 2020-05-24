<?php

namespace HKY\Kafka;

use HKY\Kafka\Client\Config\ProducerConfig;
use HKY\Kafka\Client\Producer;

/**
 * use not hyperf environment
 * Class NormalProducer
 * @package HKY\Kafka
 */
class NormalProducer
{

    private static $instance = [];

    /**
     * @var Producer
     */
    private $producer;

    private $brokerList = '';

    private $ack = '';

    private $version = '';

    private function __construct()
    {
    }

    private static function getKey($metaBrokerList, $ack = 1, $version = '0.9.0')
    {

        return sha1($metaBrokerList . $ack . $version);
    }

    /**
     * @param $data
     * [
     *    'topic' => 'test',
     *    'value' => 'message--',
     *    'key'   => 'key--',
     *  ],
     * @param int $count
     * @return mixed
     * @throws
     */
    public function send($data, $count = 0)
    {

        if ($this->producer == null) {
            throw new \Exception('must call getInstance method');
        }
        try {
            $this->producer->send($data);
            return true;
        } catch (\Exception $e) {
            if (!$count) {
                $this->close();
                $key = static::getKey($this->brokerList, $this->ack, $this->version);
                unset(static::$instance[$key]);
                $self = static::getInstance($this->brokerList, $this->ack, $this->version);
                $self->send($data, $count + 1);
            }
        }
        return false;
    }

    private function close() {
        $this->producer->close();;
    }

    /**
     * @param string $metaBrokerList
     * @param int $ack
     * @param string $version
     * @return self
     * @throws Client\Exception\Config
     * @throws Client\Exception\ConnectionException
     * @throws Client\Exception\Exception
     */
    public static function getInstance($metaBrokerList, $ack = 1, $version = '0.9.0')
    {

        $key = static::getKey($metaBrokerList, $ack, $version);
        if (isset(static::$instance[$key])) {
            return static::$instance[$key];
        }

        $config = new ProducerConfig();
        $config->setMetadataBrokerList($metaBrokerList);
        $config->setBrokerVersion($version);
        $config->setRequiredAck($ack);

        $producer = new self();
        $producer->producer = new Producer($config);
        $producer->ack = $ack;
        $producer->brokerList = $metaBrokerList;
        $producer->version = $version;

        return static::$instance[$key] = $producer;
    }
}