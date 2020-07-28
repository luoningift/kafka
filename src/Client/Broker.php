<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午10:59
 */

namespace HKY\Kafka\Client;

use HKY\Kafka\Client\Config\Config;
use HKY\Kafka\Client\Sasl\Plain;
use HKY\Kafka\Client\Exception;
use Roave\BetterReflection\Util\Autoload\ClassPrinter\ClassPrinterInterface;

class Broker
{
    /**
     * @var int
     */
    private $groupBrokerId = 0;

    /**
     * @var array
     */
    private $topics = [];

    /**
     * @var array
     */
    private $brokers = [];

    /**
     * @var Config
     */
    private $config;

    private $socketClients = [];


    /**
     * @return mixed
     */
    public function getGroupBrokerId()
    {
        return $this->groupBrokerId;
    }

    /**
     * @param mixed $groupBrokerId
     */
    public function setGroupBrokerId($groupBrokerId): void
    {
        $this->groupBrokerId = $groupBrokerId;
    }

    /**
     * @return array
     */
    public function getTopics(): array
    {
        return $this->topics;
    }

    /**
     * @return mixed
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * @param mixed $config
     */
    public function setConfig($config): void
    {
        $this->config = $config;
    }

    /**
     * @param array $topics
     * @param array $brokersResult
     * @return bool
     * @throws Exception\ErrorCodeException
     */
    public function setData(array $topics, array $brokersResult): bool
    {
        
        $brokers = [];
        foreach ($brokersResult as $value) {
            $brokers[$value['nodeId']] = $value['host'] . ':' . $value['port'];
        }
        $changed = false;
        // brokers发送前后是否改变，并存最新的brokers
        if (serialize($this->brokers) !== serialize($brokers)) {
            $this->brokers = $brokers;
            $changed = true;
        }
        $newTopics = [];
        foreach ($topics as $topic) {
            if ((int)$topic['errorCode'] !== Protocol::NO_ERROR) {
                throw new Exception\ErrorCodeException();
                continue;
            }
            $item = [];
            foreach ($topic['partitions'] as $part) {
                $item[$part['partitionId']] = $part['leader'];
            }
            $newTopics[$topic['topicName']] = $item;
        }
        // topics 发送前后是否改变，并存最新的topics
        if (serialize($this->topics) !== serialize($newTopics)) {
            $this->topics = $newTopics;
            $changed = true;
        }
        return $changed;
    }

    /**
     * @param string $brokerId
     * @return ClientConnection|null
     * @throws Exception\Exception
     */
    public function getMetaConnectByBrokerId(string $brokerId): ?ClientConnection
    {
        return $this->getConnect($brokerId, 'connect');
    }


    /**
     * @param string $key
     * @return ClientConnection|null
     * @throws Exception\Exception
     */
    public function getFetchConnectByBrokerId(string $key): ?ClientConnection
    {
        return $this->getConnect($key, 'fetchConnect');
    }


    /**
     * @param string $key
     * @param string $type
     * @return ClientConnection|null
     * @throws Exception\Exception
     */
    public function getConnect(string $key, string $type): ?ClientConnection
    {
        if (isset($this->brokers[$key])) {
            $hostPort = $this->brokers[$key];
        } else {
            $hostPort = $key;
        }
        if (isset($this->socketClients[$type][$hostPort])) {
            return $this->socketClients[$type][$hostPort];
        }
        if (strpos($hostPort, ':') === false) {
            throw new Exception\ConnectionException($hostPort . ' is not connect failed');
        }
        [$host, $port] = explode(':', $hostPort);
        if (!$host || !$port) {
            throw new Exception\ConnectionException('host or port is not correct');
        }
        try {
            $client = $this->getClient((string)$host, (int)$port);
            if ($client->connect()) {
                $this->socketClients[$type][$hostPort] = $client;
                return $client;
            }
        } catch (\Throwable $exception) {
            throw new Exception\ConnectionException($exception->getMessage());
        }
        throw new Exception\ConnectionException($hostPort . ' is not connect failed');
    }

    /**
     * @param string $host
     * @param int $port
     * @return ClientConnection
     * @throws Exception\Config
     * @throws Exception\Exception
     */
    private function getClient(string $host, int $port): ?ClientConnection
    {
        $saslProvider = $this->judgeConnectionConfig();
        return new ClientConnection($host, $port, $this->config);
    }

    /**
     * @return ClientConnection|null
     * @throws Exception\Exception
     */
    public function getRandConnect(): ?ClientConnection
    {
        $nodeIds = array_keys($this->brokers);
        shuffle($nodeIds);
        if (!isset($nodeIds[0])) {
            return null;
        }
        return $this->getMetaConnectByBrokerId((string)$nodeIds[0]);
    }

    /**
     * @return SaslMechanism|null
     * @throws Exception\Config
     * @throws Exception\Exception
     */
    private function judgeConnectionConfig(): ?SaslMechanism
    {
        if ($this->config === null) {
            return null;
        }

        $plainConnections = [
            Config::SECURITY_PROTOCOL_PLAINTEXT,
            Config::SECURITY_PROTOCOL_SASL_PLAINTEXT,
        ];

        $saslConnections = [
            Config::SECURITY_PROTOCOL_SASL_SSL,
            Config::SECURITY_PROTOCOL_SASL_PLAINTEXT,
        ];

        $securityProtocol = $this->config->getSecurityProtocol();

        $this->config->setSslEnable(!in_array($securityProtocol, $plainConnections, true));

        if (in_array($securityProtocol, $saslConnections, true)) {
            return $this->getSaslMechanismProvider($this->config);
        }

        return null;
    }

    /**
     * @param Config $config
     * @return SaslMechanism
     * @throws Exception\Exception
     */
    private function getSaslMechanismProvider(Config $config): SaslMechanism
    {
        $mechanism = $config->getSaslMechanism();
        $username = $config->getSaslUsername();
        $password = $config->getSaslPassword();

        switch ($mechanism) {
            case Config::SASL_MECHANISMS_PLAIN:
                return new Plain($username, $password);
                break;
            case Config::SASL_MECHANISMS_SCRAM_SHA_512:
            case Config::SASL_MECHANISMS_GSSAPI:
                break;
        }
        throw new Exception\Exception(sprintf('"%s" is an invalid SASL mechnism', $mechanism));
    }

    public function close()
    {
        foreach($this->socketClients as $type => $hostPortsClient) {
            foreach($hostPortsClient as $client) {
                if ($client instanceof ClientConnection) {
                    try {
                        $client->close();
                    } catch (\Exception $e) {}
                }
            }
        }
        $this->socketClients = [];
    }
}
