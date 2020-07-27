<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/21
 * Time: 上午9:27
 */
namespace HKY\Kafka\Client\Sasl;

use HKY\Kafka\Client\Client;
use HKY\Kafka\Client\ClientConnection;
use HKY\Kafka\Client\Protocol;

class Plain extends Mechanism
{
    private const MECHANISM_NAME = "PLAIN";

    /**
     * @var string
     */
    private $username;

    /**
     * @var string
     */
    private $password;

    public function __construct(string $username, string $password)
    {
        $this->username = trim($username);
        $this->password = trim($password);
    }

    /**
     * @param Client $client
     * @throws \HKY\Kafka\Client\Exception\NotSupported
     */
    protected function performAuthentication(ClientConnection $client): void
    {
        $split = Protocol\Protocol::pack(Protocol\Protocol::BIT_B8, '0');

        $data = Protocol\Protocol::encodeString(
            $split . $this->username . $split . $this->password,
            Protocol\Protocol::PACK_INT32
        );

    }

    public function getName(): string
    {
        return self::MECHANISM_NAME;
    }

}