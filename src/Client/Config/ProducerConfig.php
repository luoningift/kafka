<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午11:06
 */
namespace HKY\Kafka\Client\Config;

use HKY\Kafka\Client\Protocol\Protocol;
use HKY\Kafka\Client\Protocol\Produce;
use HKY\Kafka\Client\Exception;

/**
 * Class ProducerConfig
 * @package HKY\Kafka\Client
 * @method int getRequestTimeout()
 * @method int getProduceInterval()
 * @method int getTimeout()
 * @method int getRequiredAck()
 * @method int getCompression()
 */
class ProducerConfig extends Config
{
    private const COMPRESSION_OPTIONS = [
        Produce::COMPRESSION_NONE,
        Produce::COMPRESSION_GZIP,
        Produce::COMPRESSION_SNAPPY,
    ];

    protected $otherDefault = [
        'requiredAck'       => 1,// -1,阻塞等待服务端所有副本同步后发送response，0 服务端不发送response，1 服务端写入日志后发送response
        'timeout'           => 5000,
        'requestTimeout'    => 6000,//todo
        'produceInterval'   => 100,//todo
        'compression'       => Protocol::COMPRESSION_NONE,
    ];

    /**
     * @param int $requestTimeout
     * @throws Exception\Config
     */
    public function setRequestTimeout(int $requestTimeout): void
    {
        if ($requestTimeout < 1 || $requestTimeout > 900000) {
            throw new Exception\Config("Set Request timeout value is invalid, must set it 1 .. 900000.");
        }

        $this->options['requestTimeout'] = $requestTimeout;
    }

    /**
     * @param int $produceInterval
     * @throws Exception\Config
     */
    public function setProduceInterval(int $produceInterval): void
    {
        if ($produceInterval < 1 || $produceInterval > 900000) {
            throw new Exception\Config("Set produce interval timeout value is invalid, must set it 1.. 900000.");
        }

        $this->options['produceInterval'] = $produceInterval;
    }

    /**
     * @param int $timeout
     * @throws Exception\Config
     */
    public function setTimeout(int $timeout): void
    {
        if ($timeout < 1 || $timeout > 900000) {
            throw new Exception\Config("Set timeout is invalid, mudt set it 1 .. 900000.");
        }

        $this->options['timeout'] = $timeout;
    }

    /**
     * @param int $requiredAck
     * @throws Exception\Config
     */
    public function setRequiredAck(int $requiredAck): void
    {
        if ($requiredAck < -1 || $requiredAck > 900000) {
            throw new Exception\Config("Set required ack value is invalid, must set it -1 .. 10000.");
        }

        $this->options['requiredAck'] = $requiredAck;
    }

    /**
     * @param int $compression
     * @throws Exception\Config
     */
    public function setCompression(int $compression): void
    {
        if (! in_array($compression, self::COMPRESSION_OPTIONS, true)) {
            throw new Exception\Config(
                "Compression must be one the HKY\Kafka\Client\Protocol\Produce::COMPRESSION_OPTIONS_* constants."
            );
        }

        $this->options['compression'] = $compression;
    }

}