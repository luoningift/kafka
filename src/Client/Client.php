<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/21
 * Time: 上午9:11
 */

namespace HKY\Kafka\Client;

use HKY\Kafka\Client\Config\Config;
use HKY\Kafka\Client\Exception\ConnectionException;
use HKY\Kafka\Client\Exception\Exception;
use Swoole;
use Swoole\Coroutine\Client as CoroutineClient;

class Client
{
    /*
     * @var string
     */
    protected $host;

    /**
     * @var int
     */
    protected $port = -1;

    /**
     * @var Config
     */
    protected $config;

    /**
     * @var CoroutineClient
     */
    protected $client;
    /**
     * @var SaslMechanism
     */
    private $saslProvider;


    /**
     * CommonClient constructor.
     * @param string $host
     * @param int $port
     * @param Config|null $config
     * @param SaslMechanism|null $saslProvider
     */
    public function __construct(string $host, int $port, ?Config $config = null, ?SaslMechanism $saslProvider = null)
    {
        $this->host = $host;
        $this->port = $port;
        $this->config = $config;
        $this->saslProvider = $saslProvider;
    }

    /**
     * @return bool
     * @throws ConnectionException
     */
    public function connect(): bool
    {
        if (trim($this->host) === '') {
            throw new ConnectionException("Cannot open null host.");
        }

        if ($this->port <= 0) {
            throw new ConnectionException("Cannot open without port.");
        }

        if (!filter_var($this->host, FILTER_VALIDATE_IP)) {
            $ip = Swoole\Coroutine\System::gethostbyname($this->host);
            if ($ip == $this->host || $ip === false) {
                throw new ConnectionException(sprintf(
                    'couldn\'t get host info for %s',
                    $this->host
                ));
            }
            $this->host = $ip;
        }
        $settings = [
            'open_length_check' => 1,
            'package_length_type' => 'N',
            'package_length_offset' => 0,
            'package_body_offset' => 4,
            'package_max_length' => 1024 * 1024 * 3,
        ];
        if (!$this->client instanceof Swoole\Coroutine\Client) {
            $this->client = new Swoole\Coroutine\Client(SWOOLE_TCP);
            $this->client->set($settings);
        }
        // ssl connection
        if ($this->config !== null && $this->config->getSslEnable()) {
            array_push($settings, [
                'ssl_verify_peer' => $this->config->getSslVerifyPeer(),
                'ssl_cafile' => $this->config->getSslCafile(),
            ]);
        }
        if (!$this->client->isConnected()) {
            $connected = $this->client->connect($this->host, $this->port, 100);
            if (!$connected) {
                $connectStr = "tcp://{$this->host}:{$this->port}";
                throw new ConnectionException("Connect to Kafka server {$connectStr} failed: {$this->client->errMsg}");
            }
        }
        // todo 未实现接口
        if ($this->saslProvider !== null) {
            $this->saslProvider->autheticate($this);
        }

        return (bool)$this->client->isConnected();
    }

    /**
     * @return bool
     */
    public function isConnected(): bool
    {
        return $this->client->isConnected();
    }

    /**
     * @param null|string $data
     * @return mixed
     * @throws ConnectionException
     * @throws Exception
     */
    public function send(?string $data = null)
    {
        if ($this->isConnected()) {
            $send = $this->client->send($data);
            if ($send) {
                $receive = $this->client->recv(100);
                //空字符串表示服务器主动关闭 false发生错误
                if ($receive !== '' && $receive !== false) {
                    return $receive;
                }
            }
        }
        $connectStr = "tcp://{$this->host}:{$this->port}";
        throw new ConnectionException("Connect to Kafka server {$connectStr} failed: {$this->client->errMsg}");
    }

    /**
     * @param null|string $data
     * @return mixed
     * @throws ConnectionException
     * @throws Exception
     */
    public function sendWithNoResponse(?string $data = null)
    {
        if ($this->isConnected()) {
            return $this->client->send($data);
        }
        $connectStr = "tcp://{$this->host}:{$this->port}";
        throw new ConnectionException("Connect to Kafka server {$connectStr} failed: {$this->client->errMsg}");
    }

    public function close()
    {
        $this->client->close();
    }

    /**
     * @return bool
     * @throws ConnectionException
     * @throws Exception
     */
    public function reconnect()
    {
        if ($this->client instanceof Swoole\Coroutine\Client) {
            $this->client->close();
        }
        return $this->connect();
    }
}
