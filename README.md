#### hky-kafka

##### 1.安装
在项目中 `composer.json` 的 `repositories` 项中增加
```
{
    ....
    "repositories":{
        "hky/hyperf-kafka-client":{
            "type":"vcs",
            "url":"http://icode.kaikeba.com/base/hky-packages-hyperf-kafka-client.git"
        }
        ....
    }
}
```
修改完成后执行
```bash
$ composer require hky/hyperf-kafka-client
$ php bin/hyperf.php vendor:publish hky/hyperf-kafka-client
```
如果遇到错误信息为:
`Your configuration does not allow connections to http://icode.kaikeba.com/base/hky-packages-hyperf-http-client.git. See https://getcomposer.org/doc/06-config.md#secure-http for details` 
执行以下命令
```bash
$ composer config secure-http false
```

##### 2.配置文件说明config/autoload/hky_kafka.php
```php
<?php
return [
    //连接配置
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
        //对应生产者的key
        'default' => [
            //对应pool里面的key
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
        /**
        * 消费组配置文件 注解方式优先级大于配置文件方式  配置文件方式主要解决和环境变量相关配置
        * pool_name 必填项 对应pool里面的key
        * name 必填项 进程名称
        * topic 必填项 消费的kafka主题 由于测试环境 开发环境用的是同一个kafka 注意区分为不同的名字 建议名字增加前缀或者后缀 
        * group 必填项 消费者组 由于测试环境 开发环境用的是同一个kafka 注意区分为不同的名字 建议名字增加前缀或者后缀
        * enable 非必填项 不填默认true 取值范围 true 和 false  false不启动该进程  true 表示启动该进程
        * max_byte 非必填项 不填默认65535 每次拉取的消息的最大byte 比如一个消息是1byte 设置maxByte为1024 每次会拉回1024条消息
        * consumer_nums 非必填项 不填默认1个
        * process_nums 非必填项 不填默认1个
        * 消费者数量 = consumerNums * processNums
        * max_consumption 非必填项 消费多少消息后消费进程重启 不重启 写-1 默认不重启
        * max_poll_record 非必填项 每次最多拉取多少条进行消费 默认5条
        */
        //对应consumer消费者的key
        'default' => [
            //对应pool里面的key
            'pool_name' => 'default',
            'enable' => true,
            'max_byte' => 10240,
            'topic' => 'test1',
            'consumer_nums' => 10,
            'process_nums' => 1,
            'name' => '进程名称',
            'group' => 'test_group',
        ],
        'other' => [
            'pool_name' => 'pool_other',
            'enable' => true,
            'max_byte' => 10240,
            'topic' => 'test2',
            'consumer_nums' => 10,
            'process_nums' => 1,
            'name' => '进程名称',
            'group' => 'test_group',
        ],
    ],
];
//producer 为 kafka 生产者配置 
//consumer 为 kafka 消费者配置
```
##### 3.生产者发送消息 hyperf环境
```php
<?php
use HKY\Kafka\Producer;
//默认使用default pool连接
$this->container->get(Producer::class)->send([
    ['topic' => 'test1', 'value' => 'hello world', 'key' => 'xxx'], //key 设置key后会根据key将消息发送到固定的partition
    ['topic' => 'test1', 'value' => 'hello world', 'key' => 'xxx'],
    ['topic' => 'test1', 'value' => 'hello world', 'key' => 'xxx'],
]);
//使用其他 pool
$this->container->get(\HKY\Kafka\ProducerFactory::class)->get('other')->send();
```
##### 5.消费者消费消息 
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
use HKY\Kafka\Annotation\Consumer;
use Hyperf\Utils\Coroutine;

/**
 * 消费组配置文件 注解方式优先级大于配置文件方式  配置文件方式主要解决和环境变量相关配置
 * configName 必填项 对应hky_kafka.consumer里面的key
 * name 必填项 进程名称
 * topic 必填项 消费的kafka主题 由于测试环境 开发环境用的是同一个kafka 注意区分为不同的名字 建议名字增加前缀或者后缀 
 * group 必填项 消费者组 由于测试环境 开发环境用的是同一个kafka 注意区分为不同的名字 建议名字增加前缀或者后缀
 * enable 非必填项 不填默认true 取值范围 true 和 false  false不启动该进程  true 表示启动该进程
 * maxByte 非必填项 不填默认65535 每次拉取的消息的最大byte 比如一个消息是1byte 设置maxByte为1024 每次会拉回1024条消息
 * consumerNums 非必填项 不填默认1个
 * processNums 非必填项 不填默认1个
 * 消费者数量 = consumerNums * processNums
 * maxConsumption 非必填项 消费多少消息后消费进程重启 不重启 写-1 默认不重启
 * maxPollRecord 非必填项 每次最多拉取多少条进行消费 默认5条
 * @Consumer(enable=true, configName="default", maxByte=65535, maxPollRecord=5, topic="test1", consumerNums=5, maxConsumption=10000, processNums=2, name="study_progress", group="luoningtest")
 */
class StudyProgressNormalProcess extends ConsumerMessage
{
    public function init() {
          //启动时设置不消费消息
          $this->setOffConsume();
          //12秒后开始消费消息
          Swoole\Timer::after(12000, function() {
              echo "beginConsumer" . PHP_EOL;
              $this->setOnConsume();
          });
          //18秒后设置不消费消息
          Swoole\Timer::after(18000, function() {
              echo "endConsumer" . PHP_EOL;
              $this->setOffConsume();
          });
          //26秒后开始消费消息
          Swoole\Timer::after(26000, function() {
              echo "beginConsumer" . PHP_EOL;
              $this->setOnConsume();
          });
    }

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
##### 6.其他注意事项
```$xslt
1、进程异常重启后, 部分消息会重复消费，原因还未来得及提交offset
```
### 版本改动:
```$xslt
v1.0.7   修改版本号到0.10.1.0
v1.0.6   randConnect bug modify
v1.0.5   bug fixed,删除无用代码
v1.0.4   解决消费时间过长 提交偏移量失败的问题
v1.0.3   增加控制消费频率，控制队列是否消费开关，增加consumer注解最大拉取条数
v1.0.2   文档说明修改，逻辑修改
v1.0.1   逻辑修改
v1.0.0   kafka协程版本
```
