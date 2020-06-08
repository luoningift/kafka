#### hky-kafka

##### 1.安装
在项目中 `composer.json` 的 `repositories` 项中增加
```
{
    ....
    "repositories":{
        "hky/kafka":{
            "type":"vcs",
            "url":"git@192.168.100.11:base/hky-hyperf-kafka-client.git"
        }
        ....
    }
}
```
修改完成后执行
```bash
$ composer require hky/kafka
$ php bin/hyperf.php vendor:publish hky/kafka
```
##### 2.配置文件说明config/autoload/hky_kafka.php
```php
<?php
return [
    'producer' => [
        'default' => [
            'broker_list' => '192.168.10.1:9092,192.168.10.1:9093,192.168.10.1:9094', //连接地址 多个broker以英文逗号隔开，ip和端口以英文冒号隔开
            'ack' => 1,     //kafka 确认几个副本写入成功才返回成功
            'version' => '0.9.0',  //客户端版本号 一般不用动
            'pool' => [
                'min_connections' => 1,      // 最少的客户端连接数   
                'max_connections' => 10,     // 最多的客户端连接数
                'connect_timeout' => 1.0,    
                'wait_timeout' => 3.0,
                'heartbeat' => -1,
                'max_idle_time' => 60,
            ],
        ]
    ],
    'consumer' => [
        'default' => [
            'broker_list' => '192.168.10.1:9092,192.168.10.1:9093,192.168.10.1:9094', //连接地址 多个broker以英文逗号隔开，ip和端口以英文冒号隔开
            'version' => '0.9.0',
        ],
    ],
];
//producer 为 kafka 生产者配置 
//consumer 为 kafka 消费者配置
```
##### 3.生产者发送消息 PHP-FPM环境
```php
<?php
\HKY\Kafka\ProducerNormal::getInstance('192.168.10.1:9092,192.168.10.1:9093,192.168.10.1:9094')->send([
    ['topic' => 'test1', 'value' => 'hello world', 'key' => 'xxx'], //key 设置key后会根据key将消息发送到固定的partition
    ['topic' => 'test1', 'value' => 'hello world', 'key' => 'xxx'],
    ['topic' => 'test1', 'value' => 'hello world', 'key' => 'xxx'],
]);
```
##### 3.生产者发送消息 hyperf环境
```php
<?php
use HKY\Kafka\Producer;
$this->container->get(Producer::class)->send([
    ['topic' => 'test1', 'value' => 'hello world', 'key' => 'xxx'], //key 设置key后会根据key将消息发送到固定的partition
    ['topic' => 'test1', 'value' => 'hello world', 'key' => 'xxx'],
    ['topic' => 'test1', 'value' => 'hello world', 'key' => 'xxx'],
]);
```
##### 4.消费者消费消息 hyper环境
```php
<?php
declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://doc.hyperf.io
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf-cloud/hyperf/blob/master/LICENSE
 */

namespace App\Process;

use HKY\Kafka\Message\ConsumerMessage;
use Hyperf\Event\Annotation\Listener;
use HKY\Kafka\Annotation\Consumer;
use Hyperf\Utils\Coroutine;

/**
 * enable 是否启动进程 
 * poolName 对应hky_kafka配置文件consumer的key
 * topic 消费的topic
 * consumerNums 启动协程消费者数量
 * processNums 启动的进程数量  消费者数量 = consumerNums * processNums
 * name 启动时设置的进程名称
 * group 消费者组
 * maxConsumption 消费多少消息后消费进程重启
 * @Consumer(enable=true, poolName="default", topic="test1", consumerNums=5, maxConsumption=10000, processNums=2, name="study_progress", group="luoningtest")
 */
class StudyProgressNormalProcess extends ConsumerMessage
{
    public function consume($topic, $partition, $message): string
    {
        echo 'partition:' . $partition . 'message:' . $message['message']['value'] . PHP_EOL;
        echo '总共消费了 ' . $this->atomic->get() . ' 条, 进程ID是 '.posix_getpid().' 协程id是 ' . Coroutine::id() . PHP_EOL;
        return 'success';
    }
}
//consume方法结果请返回string
//$this->atomic->get() 获取已经消费的消息数量
```
##### 4.其他注意事项
```$xslt
进程异常重启后, 部分消息会重复消费，原因还未来得及提交offset
```
### 版本改动:
```$xslt
v1.0.0   kafka协程版本
```
