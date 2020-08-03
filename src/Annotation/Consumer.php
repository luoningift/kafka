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
namespace HKY\Kafka\Annotation;

use Hyperf\Di\Annotation\AbstractAnnotation;

/**
 * @Annotation
 * @Target({"CLASS"})
 */
class Consumer extends AbstractAnnotation
{
    /**
     * @var string
     */
    public $configName;

    /**
     * @var string
     */
    public $poolName;

    /**
     * @var string
     */
    public $topic;

    /**
     * @var int
     */
    public $consumerNums;

    /**
     * @var string
     */
    public $group;

    /**
     * @var string
     */
    public $name;

    /**
     * @var null|bool
     */
    public $enable;

    /**
     * @var int
     */
    public $maxConsumption;

    /**
     * @var int
     */
    public $maxByte = 65535;
}
