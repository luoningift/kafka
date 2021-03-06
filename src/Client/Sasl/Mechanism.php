<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/21
 * Time: 上午9:28
 */
namespace HKY\Kafka\Client\Sasl;

use HKY\Kafka\Client\ClientConnection;
use HKY\Kafka\Client\SaslMechanism;

abstract class Mechanism implements SaslMechanism
{
    public function autheticate (ClientConnection $client): void
    {
        $this->handShake($client, $this->getName());
        $this->performAuthentication($client);
    }

    protected function handShake(ClientConnection $client, string $mechanism): void
    {

    }

    abstract protected function performAuthentication(ClientConnection $client): void;

    abstract public function getName(): string;

}