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
    public $poolName = '';

    /**
     * @var string
     */
    public $topic = '';

    /**
     * @var int
     */
    public $consumerNums = 3;

    /**
     * @var string
     */
    public $group = 'kafka_group';

    /**
     * @var string
     */
    public $name = 'KafkaConsumer';

    /**
     * @var int
     */
    public $processNums = 1;

    /**
     * @var null|bool
     */
    public $enable;

    /**
     * @var int
     */
    public $maxConsumption = 0;
}
