<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/21
 * Time: 上午9:28
 */
namespace HKY\Kafka\Client\Sasl;

use HKY\Kafka\Client\Client\ClientInterface;
use HKY\Kafka\Client\SaslMechanism;

abstract class Mechanism implements SaslMechanism
{
    public function autheticate (ClientInterface $client): void
    {
        $this->handShake($client, $this->getName());
        $this->performAuthentication($client);
    }

    protected function handShake(ClientInterface $client, string $mechanism): void
    {

    }

    abstract protected function performAuthentication(ClientInterface $client): void;

    abstract public function getName(): string;

}