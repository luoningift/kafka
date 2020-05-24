<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/21
 * Time: 上午9:17
 */
namespace HKY\Kafka\Client;

use HKY\Kafka\Client\Client\ClientInterface;

interface SaslMechanism
{
    public function autheticate(ClientInterface $client): void;
}
