<?php
declare(strict_types=1);

namespace HKY\Kafka\Frequency;

/**
 * 消费频率控制类
 * Class FrequencyInterface
 * @package HKY\Kafka\Frequency
 */
class TimeFrequency implements FrequencyInterface
{

    private $timeConfig = [];

    /**
     * @param array $maxTimePollRecord
     * @return object
     * 小时秒 ['000000'=> [3, 100], '010000' => [10, 300], '230000' => [20, 500]]
     * 0时0分0秒到1时0分0秒 每次poll3条记录,一次消费完后休眠100毫秒
     * 1时0分0秒到23时0分0秒 每次poll10条记录,一次消费完后休眠300毫秒
     * 23时0分0秒到23时59分0秒 每次poll10条记录,一次消费完后休眠500毫秒
     */
    public function set(array $maxTimePollRecord)
    {
        $maxTimePollRecord = array_reverse($maxTimePollRecord, true);
        $getConfig = function ($time) use ($maxTimePollRecord) {
            foreach ($maxTimePollRecord as $configTime => $config) {
                if (intval($time) >= intval($configTime)) {
                    return $config;
                }
            }
            return [5, 0];
        };
        for ($hour = 0; $hour < 24; $hour++) {
            for ($minute = 0; $minute < 60; $minute++) {
                for ($second = 0; $second < 60; $second++) {
                    $key = str_pad(strval($hour), 2, "0", STR_PAD_LEFT) . str_pad(strval($minute), 2, "0", STR_PAD_LEFT) . str_pad(strval($second), 2, "0", STR_PAD_LEFT);
                    $this->timeConfig[$key] = $getConfig($key);
                }
            }
        }
        return $this;
    }

    /**
     * 返回频率
     * @return array
     * @example [1,1] 表示每次拉取一个消息，消费完后休眠1毫秒
     * @example [1,0] 表示每次拉取一个消息，消费完后休眠0毫秒
     * @example [0,0] 表示不拉取消息
     */
    public function get()
    {
        $time = date('His');
        return $this->timeConfig[$time] ?? [];
    }
}
