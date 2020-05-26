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
return [
    'producer' => [
        'default' => [
            'broker_list' => '127.0.0.1:9092,127.0.0.1:9093',
            'ack' => 1,
            'version' => '0.9.0',
            'pool' => [
                'min_connections' => 1,
                'max_connections' => 10,
                'connect_timeout' => 1.0,
                'wait_timeout' => 3.0,
                'heartbeat' => -1,
                'max_idle_time' => 60,
            ],
        ]
    ],
    'consumer' => [
        'default' => [

        ],
    ],
];
