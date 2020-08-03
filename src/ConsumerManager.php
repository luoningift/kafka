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

namespace HKY\Kafka;

use HKY\Kafka\Annotation\Consumer as ConsumerAnnotation;
use HKY\Kafka\Client\Config\ConsumerConfig;
use HKY\Kafka\Message\ConsumerMessageInterface;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Di\Annotation\AnnotationCollector;
use Hyperf\Process\AbstractProcess;
use Hyperf\Process\ProcessManager;
use Hyperf\Utils\Coroutine;
use Hyperf\Utils\Coroutine\Concurrent;
use Psr\Container\ContainerInterface;
use Swoole\Process;

class ConsumerManager
{
    /**
     * @var ContainerInterface
     */
    private $container;

    private $propertyMap = [
        'pool_name' => 'poolName',
        'enable' => 'enable',
        'max_byte' => 'maxByte',
        'topic' => 'topic',
        'consumer_nums' => 'consumerNums',
        'name' => 'name',
        'group' => 'group',
        'max_consumption' => 'maxConsumption',
    ];

    private $propertyDefault = [
        'enable' => true,
        'maxByte' => 65535,
        'consumerNums' => 1,
        'maxConsumption' => -1,
    ];

    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    public function run()
    {
        $classes = AnnotationCollector::getClassByAnnotation(ConsumerAnnotation::class);

        $connectConfig = $this->container->get(ConfigInterface::class)->get('hky_kafka.pool');
        $consumerConfig = $this->container->get(ConfigInterface::class)->get('hky_kafka.consumer');
        if ($classes && ($consumerConfig === null || $connectConfig === null)) {
            $this->container->get(StdoutLoggerInterface::class)->error('未找到hky_kafka.consumer或者hky_kafka.pool配置文件');
            exit;
        }

        /**
         * @var string
         * @var ConsumerAnnotation $annotation
         */
        foreach($classes as $class => $annotation) {

            if ($annotation->configName === null) {
                $this->container->get(StdoutLoggerInterface::class)->error('kafka消费端' . $class . '未配置configName');
                exit;
            }
            if (!isset($consumerConfig[$annotation->configName])) {
                $this->container->get(StdoutLoggerInterface::class)->error('在hky_kafka.consumer配置文件中未找到 ' . $annotation->configName . ' 配置项');
                exit;
            }
            if (!isset($consumerConfig[$annotation->configName]['pool_name'])) {
                $this->container->get(StdoutLoggerInterface::class)->error('在hky_kafka.consumer配置文件中未找到 pool_name 配置项');
            }
            $connectPool = $consumerConfig[$annotation->configName]['pool_name'];
            if (!isset($connectConfig[$connectPool])) {
                $this->container->get(StdoutLoggerInterface::class)->error('在hky_kafka.pool配置文件中未找到 ' . $connectPool . ' 配置项');
                exit;
            }
            $oneConsumerConfig = $consumerConfig[$annotation->configName];
            foreach($this->propertyMap as $configKey => $annotationKey) {
                if ($annotation->$annotationKey === null && isset($oneConsumerConfig[$configKey])) {
                    $annotation->$annotationKey = $oneConsumerConfig[$configKey];
                }
                if ($annotation->$annotationKey === null && isset($this->propertyDefault[$annotationKey])) {
                    $annotation->$annotationKey = $this->propertyDefault[$annotationKey];
                }
                if ($annotation->$annotationKey === null) {
                    $this->container->get(StdoutLoggerInterface::class)->error('配置项' . $annotationKey . '不能为空, 请在配置文件中配置 ' . $configKey . ' 或者在注解中配置 ' . $annotationKey);
                    exit;
                }
            }
        }
        /**
         * @var string
         * @var ConsumerAnnotation $annotation
         */
        foreach ($classes as $class => $annotation) {
            $instance = make($class);
            if (!$instance instanceof ConsumerMessageInterface) {
                continue;
            }
            $annotation->consumerNums && $instance->setConsumerNums($annotation->consumerNums);
            $annotation->poolName && $instance->setPoolName($annotation->poolName);
            $annotation->maxByte && $instance->setMaxBytes(intval($annotation->maxByte));
            $annotation->topic && $instance->setTopic($annotation->topic);
            $annotation->group && $instance->setGroup($annotation->group);
            !is_null($annotation->enable) && $instance->setEnable($annotation->enable);
            property_exists($instance, 'container') && $instance->container = $this->container;
            $annotation->maxConsumption && $instance->setMaxConsumption($annotation->maxConsumption);
            $nums = $instance->getConsumerNums();
            $process = $this->createProcess($instance);
            $process->nums = (int)$nums;
            $process->name = $annotation->name . '-' . $instance->getTopic();
            ProcessManager::register($process);
        }
    }

    private function createProcess(ConsumerMessageInterface $consumerMessage): AbstractProcess
    {
        return new class($this->container, $consumerMessage) extends AbstractProcess {

            /**
             * @var ConsumerMessageInterface
             */
            private $consumerMessage;

            public function __construct(ContainerInterface $container, ConsumerMessageInterface $consumerMessage)
            {
                parent::__construct($container);
                $this->consumerMessage = $consumerMessage;
            }

            public function handle(): void
            {

                $consumerMessage = $this->consumerMessage;
                $consumerMessage->init();
                $consumerMessage->initAtomic();
                
                Process::signal(SIGINT, function () use ($consumerMessage) {
                    $consumerMessage->setSingalExit();
                });
                
                Process::signal(SIGTERM, function () use ($consumerMessage) {
                    $consumerMessage->setSingalExit();
                });
                
                $kafka = null;
                $config = null;
                try {
                    $kafkaConfig = $this->container->get(ConfigInterface::class)->get('hky_kafka.pool.' . $consumerMessage->getPoolName());
                    $config = new ConsumerConfig();
                    $config->setRefreshIntervalMs(1000);
                    $config->setMetadataBrokerList($kafkaConfig['broker_list'] ?? '127.0.0.1:9092,127.0.0.1:9093');
                    $config->setBrokerVersion('0.10.1.0');
                    $config->setGroupId($consumerMessage->getGroup());
                    $config->setTopics([$consumerMessage->getTopic()]);
                    $config->setMaxBytes($consumerMessage->getMaxBytes());
                    $config->setOffsetReset('earliest');
                    $config->setConsumeMode(ConsumerConfig::CONSUME_AFTER_COMMIT_OFFSET);
                    $kafka = new Client\Consumer($config);
                    $kafka->subscribe($consumerMessage);
                } catch (\Exception $e) {
                    if ($kafka) {
                        $kafka->close();
                        unset($kafka);
                    }
                }
            }

            public function isEnable(): bool
            {
                return $this->consumerMessage->isEnable();
            }
        };
    }
}
