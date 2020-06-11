<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/20
 * Time: 上午10:17
 */
namespace HKY\Kafka\Client;

use HKY\Kafka\Client\ApiVersions\Process;
use HKY\Kafka\Client\Config\Config;

class ApiVersions
{
    private $process;

    /**
     * ApiVersions constructor.
     * @param Config $config
     * @param Broker $broker
     * @throws Exception\Exception
     */
    public function __construct(Config $config, Broker $broker)
    {
        $this->process = new Process($config, $broker);
    }

    /**
     * @return array|null
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function getVersions(): ?array
    {
        return $this->process->apiVersions();
    }
}
