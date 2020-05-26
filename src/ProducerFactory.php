<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://doc.hyperf.io
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */
namespace HKY\Kafka;

use Hyperf\Contract\ConfigInterface;

class ProducerFactory
{
    /**
     * @var ProducerProxy[]
     */
    protected $proxies;

    public function __construct(ConfigInterface $config)
    {
        $mongoConfig = $config->get('hky_kafka');

        foreach ($mongoConfig as $poolName => $item) {
            $this->proxies[$poolName] = make(ProducerProxy::class, ['pool' => $poolName]);
        }
    }

    /**
     * @return ProducerProxy
     */
    public function get(string $poolName)
    {
        $proxy = $this->proxies[$poolName] ?? null;
        if (! $proxy instanceof ProducerProxy) {
            throw new \RuntimeException('Invalid Kafka proxy.');
        }
        return $proxy;
    }
}
