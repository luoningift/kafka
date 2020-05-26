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

        if ($this->container instanceof Container) {
            $pool = $this->container->make(ProducerPool::class, ['name' => $name]);
        } else {
            $pool = new ProducerPool($this->container, $name);
        }
        return $this->pools[$name] = $pool;
    }
}
