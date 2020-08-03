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
    'pool' => [
        'default' => [
            'broker_list' => env('DEFAULT_BROKER_LIST', '192.168.10.1:9092,192.168.10.1:9093,192.168.10.1:9094'),
            'ack' => 1,
            'version' => '0.9.0',
            'pool' => [
                'min_connections' => 1,
                'max_connections' => 100,
                'connect_timeout' => 1.0,
                'wait_timeout' => 3.0,
                'heartbeat' => -1,
                'max_idle_time' => 60,
            ],
        ],
        'pool_other' => [
            'broker_list' => env('OTHER_BROKER_LIST', '192.168.10.1:9092,192.168.10.1:9093,192.168.10.1:9094'),
            'ack' => 1,
            'version' => '0.9.0',
            'pool' => [
                'min_connections' => 1,
                'max_connections' => 100,
                'connect_timeout' => 1.0,
                'wait_timeout' => 3.0,
                'heartbeat' => -1,
                'max_idle_time' => 60,
            ],
        ]
    ],
    'producer' => [
        'default' => [
            'pool_name' => 'default',
        ],
        'other' => [
            'pool_name' => 'default',
        ],
        'second' => [
            'pool_name' => 'pool_other',
        ],
    ],
    'consumer' => [
        'default' => [
            'pool_name' => 'default',
            'enable' => true,
            'max_byte' => 10240,
            'topic' => 'test1',
            'consumer_nums' => 10,
            'name' => '进程名称',
            'group' => 'test_group',
        ],
        'other' => [
            'pool_name' => 'pool_other',
            'enable' => true,
            'max_byte' => 10240,
            'topic' => 'test2',
            'consumer_nums' => 10,
            'name' => '进程名称',
            'group' => 'test_group',
        ],
    ],
];
