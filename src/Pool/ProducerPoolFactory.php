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
namespace HKY\Kafka\Pool;

use Hyperf\Contract\ConfigInterface;
use Hyperf\Di\Container;
use Psr\Container\ContainerInterface;

class ProducerPoolFactory
{
    /**
     * @var ContainerInterface
     */
    protected $container;

    /**
     * @var ProducerPool[]
     */
    protected $pools = [];

    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    public function getPool(string $name): ProducerPool
    {
        if (isset($this->pools[$name])) {
            return $this->pools[$name];
        }
        $producerConf = $this->container->get(ConfigInterface::class)->get('hky_kafka.producer');
        $currentPoolName = $producerConf[$name]['pool_name'];
        if ($this->container instanceof Container) {
            $pool = $this->container->make(ProducerPool::class, ['name' => $currentPoolName]);
        } else {
            $pool = new ProducerPool($this->container, $currentPoolName);
        }
        foreach($producerConf as $tPoolName => $tProducerConf) {
            if ($tProducerConf['pool_name'] == $currentPoolName && $tPoolName != $name) {
                $this->pools[$tPoolName] = $pool;
            }
        }
        return $this->pools[$name] = $pool;
    }
}
