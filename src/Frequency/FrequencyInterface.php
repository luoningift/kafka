<?php
declare(strict_types=1);
namespace HKY\Kafka\Frequency;

/**
 * 消费频率控制类
 * Class FrequencyInterface
 * @package HKY\Kafka\Frequency
 */
interface FrequencyInterface
{
    /**
     * 实现返回频率信息
     * @return array
     * @example [1,1] 表示每次拉取一个消息，消费完后休眠1毫秒
     * @example [1,0] 表示每次拉取一个消息，消费完后休眠0毫秒
     * @example [0,0] 表示不拉取消息
     *                休眠时间需小于等于1000 否则不生效
     */
    public function get();
}
