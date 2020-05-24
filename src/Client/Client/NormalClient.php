<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/21
 * Time: 上午9:11
 */

namespace HKY\Kafka\Client\Client;

use HKY\Kafka\Client\Config\Config;
use HKY\Kafka\Client\Exception\ConnectionException;
use HKY\Kafka\Client\Exception\Exception;
use HKY\Kafka\Client\Protocol\Protocol;
use HKY\Kafka\Client\SaslMechanism;
use Swoole\Coroutine\Client as CoroutineClient;

class NormalClient implements ClientInterface
{

    private const MAX_WRITE_BUFFER = 2048;
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
     * @var
     */
    protected $client;
    /**
     * @var SaslMechanism
     */
    private $saslProvider;

    /**
     * @var array
     */
    protected $clientConfig;

    /**
     * CommonClient constructor.
     * @param string $host
     * @param int $port
     * @param Config|null $config
     * @param SaslMechanism|null $saslProvider
     * @throws Exception
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
     * @throws Exception
     */
    public function connect(): bool
    {
        if (trim($this->host) === '') {
            throw new Exception("Cannot open null host.");
        }

        if ($this->port <= 0) {
            throw new Exception("Cannot open without port.");
        }

        if (!is_resource($this->client)) {
            $remoteSocket = sprintf('tcp://%s:%s', $this->host, $this->port);
            $context = stream_context_create([]);
            if ($this->config !== null && $this->config->getSslEnable()) { // ssl connection
                $remoteSocket = sprintf('ssl://%s:%s', $this->host, $this->port);
                $context = stream_context_create(
                    [
                        'ssl' => [
                            'local_cert' => $this->config->getSslLocalCert(),
                            'local_pk' => $this->config->getSslLocalPk(),
                            'verify_peer' => $this->config->getSslVerifyPeer(),
                            'passphrase' => $this->config->getSslPassphrase(),
                            'cafile' => $this->config->getSslCafile(),
                            'peer_name' => $this->config->getSslPeerName(),
                        ],
                    ]
                );
            }
            $this->client = stream_socket_client(
                $remoteSocket,
                $errno,
                $errstr,
                2,
                STREAM_CLIENT_CONNECT,
                $context
            );
            stream_set_blocking($this->client, true);
            stream_set_read_buffer($this->client, 0);
            stream_set_timeout($this->client, 30);
        }

        if (!$this->isConnected()) {
            $connectStr = "tcp://{$this->host}:{$this->port}";
            throw new ConnectionException("Connect to Kafka server {$connectStr} failed: {$this->client->errMsg}");
        }

        // todo 未实现接口
        if ($this->saslProvider !== null) {
            $this->saslProvider->autheticate($this);
        }

        return (bool)$this->isConnected();
    }

    /**
     * @return bool
     */
    public function isConnected(): bool
    {
        return is_resource($this->client) && (!stream_get_meta_data($this->client)['timed_out']);
    }

    /**
     * @param null|string $data
     * @param int $tries
     * @return mixed
     * @throws ConnectionException
     * @throws Exception
     */
    public function send(?string $data = null, $tries = 2)
    {
        if (!is_string($data)) {
            return '';
        }

        for ($try = 0; $try <= $tries; $try++) {
            $bytesToWrite = strlen($data);
            $bytesWritten = 0;
            try {
                while ($bytesWritten < $bytesToWrite) {
                    if (!$this->isConnected()) {
                        throw new \Exception('timeout');
                    }
                    if ($bytesToWrite - $bytesWritten > self::MAX_WRITE_BUFFER) {
                        $wrote = fwrite($this->client, substr($data, $bytesWritten, self::MAX_WRITE_BUFFER));
                    } else {
                        $wrote = fwrite($this->client, substr($data, $bytesWritten));
                    }
                    if ($wrote === -1 || $wrote === false) {
                        throw new ConnectionException('Could not write ' . strlen($data) . ' bytes to stream, completed writing only ' . $bytesWritten . ' bytes');
                    }
                    $bytesWritten += $wrote;
                }
            } catch (\Exception $e) {
                $this->reconnect();
                continue;
            }
            return $this->recv();
        }
        $connectStr = "tcp://{$this->host}:{$this->port}";
        throw new ConnectionException("Connect to Kafka server {$connectStr} failed: {$this->client->errMsg}");
    }

    /**
     * @param null|string $data
     * @param int $tries
     * @return mixed
     * @throws ConnectionException
     * @throws Exception
     */
    public function sendWithNoResponse(?string $data = null, $tries = 2)
    {
        for ($try = 0; $try <= $tries; $try++) {
            $bytesToWrite = strlen($data);
            $bytesWritten = 0;
            try {
                while ($bytesWritten < $bytesToWrite) {
                    if (!$this->isConnected()) {
                        throw new \Exception('timeout');
                    }
                    if ($bytesToWrite - $bytesWritten > self::MAX_WRITE_BUFFER) {
                        $wrote = fwrite($this->client, substr($data, $bytesWritten, self::MAX_WRITE_BUFFER));
                    } else {
                        $wrote = fwrite($this->client, substr($data, $bytesWritten));
                    }
                    if ($wrote === -1 || $wrote === false) {
                        throw new ConnectionException('Could not write ' . strlen($data) . ' bytes to stream, completed writing only ' . $bytesWritten . ' bytes');
                    }
                    $bytesWritten += $wrote;
                }
            } catch (\Exception $e) {
                $this->reconnect();
                continue;
            }
        }
        if ($bytesWritten < $bytesToWrite) {
            $connectStr = "tcp://{$this->host}:{$this->port}";
            throw new ConnectionException("Connect to Kafka server {$connectStr} failed: {$this->client->errMsg}");
        }
    }

    /**
     * @param float $timeout
     * @return string
     * @throws ConnectionException
     * @throws Exception
     */
    public function recv(float $timeout = -1): string
    {

        $data = substr(fread($this->client, 4), 0, 4);
        if (!$data) {
            throw new \RuntimeException('read failed');
        }
        $len = Protocol::unpack(Protocol::BIT_B32, $data);
        $maxRead = 65535;
        $count = intval($len / $maxRead);
        $surplus = $len % $maxRead;
        $buffer = $data;
        for($i = 0; $i < $count; $i++) {
            $readBuffer = fread($this->client, $maxRead);
            if ($readBuffer === false) {
                throw new \RuntimeException('read failed');
            }
            if ($readBuffer === '') {
                throw new \RuntimeException('read failed');
            }
            $buffer .= $readBuffer;
        }
        if ($surplus > 0) {
            $readBuffer = fread($this->client, $surplus);
            if ($readBuffer === false) {
                throw new \RuntimeException('read failed');
            }
            if ($readBuffer === '') {
                throw new \RuntimeException('read failed');
            }
            $buffer .= $readBuffer;
        }
        return $buffer;
    }

    public function close()
    {
        if (is_resource($this->client)) {
            fclose($this->client);
            $this->client = null;
        }
    }

    /**
     * @return bool
     * @throws ConnectionException
     * @throws Exception
     */
    public function reconnect()
    {
        if ($this->client) {
            $this->close();
        }
        return $this->connect();
    }
}
