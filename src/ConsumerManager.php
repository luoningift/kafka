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
namespace HKY\Kafka;;

use HKY\Kafka\Annotation\Consumer as ConsumerAnnotation;
use HKY\Kafka\Client\Config\ConsumerConfig;
use HKY\Kafka\Message\ConsumerMessageInterface;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Di\Annotation\AnnotationCollector;
use Hyperf\Process\AbstractProcess;
use Hyperf\Process\ProcessManager;
use Hyperf\Utils\Coroutine;
use Hyperf\Utils\Coroutine\Concurrent;
use Hyperf\Utils\Exception\ParallelExecutionException;
use Hyperf\Utils\Parallel;
use Psr\Container\ContainerInterface;
use Swoole\Process;

class ConsumerManager
{
    /**
     * @var ContainerInterface
     */
    private $container;

    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    public function run()
    {
        $classes = AnnotationCollector::getClassByAnnotation(ConsumerAnnotation::class);
        /**
         * @var string
         * @var ConsumerAnnotation $annotation
         */
        foreach ($classes as $class => $annotation) {
            $instance = make($class);
            if (! $instance instanceof ConsumerMessageInterface) {
                continue;
            }
            $annotation->consumerNums && $instance->setConsumerNums($annotation->consumerNums);
            $annotation->poolName && $instance->setPoolName($annotation->poolName);
            $annotation->maxByte && $instance->setMaxBytes(intval($annotation->maxByte));
            $annotation->topic && $instance->setTopic($annotation->topic);
            $annotation->group && $instance->setGroup($annotation->group);
            ! is_null($annotation->enable) && $instance->setEnable($annotation->enable);
            property_exists($instance, 'container') && $instance->container = $this->container;
            $annotation->maxConsumption && $instance->setMaxConsumption($annotation->maxConsumption);
            $nums = $annotation->processNums;
            $process = $this->createProcess($instance);
            $process->nums = (int) $nums;
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
                $consumerMessage->initAtomic();

                Process::signal(SIGINT, function () use ($consumerMessage) {
                    $consumerMessage->setSingalExit();
                });

                Process::signal(SIGTERM, function () use ($consumerMessage) {
                    $consumerMessage->setSingalExit();
                });

                $concurrent = new Concurrent($this->consumerMessage->getConsumerNums());
                while(true) {
                    if ($consumerMessage->checkAtomic()) {
                        $this->process->exit(0);
                    }
                    if ($consumerMessage->getSingalExit()) {
                        $this->process->exit(0);
                    }
                    $concurrent->create(function () use ($consumerMessage) {
                        $kafka = null;
                        $config = null;
                        try {
                            $kafkaConfig = $this->container->get(ConfigInterface::class)->get('hky_kafka.consumer.' . $consumerMessage->getPoolName());
                            $config = new ConsumerConfig();
                            $config->setRefreshIntervalMs(1000);
                            $config->setMetadataBrokerList($kafkaConfig['broker_list'] ?? '127.0.0.1:9092,127.0.0.1:9093');
                            $config->setBrokerVersion($kafkaConfig['version'] ?? '0.9.0');
                            $config->setGroupId($consumerMessage->getGroup());
                            $config->setTopics([$consumerMessage->getTopic()]);
                            $config->setMaxBytes($consumerMessage->getMaxBytes());
                            $config->setOffsetReset('earliest');
                            $config->setConsumeMode(ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET);
                            $kafka = new Client\Consumer($config);
                            $kafka->subscribe($consumerMessage);
                        } catch (\Exception $e) {
                            if ($kafka) {
                                $kafka->close();
                                unset($kafka);
                            }
                            if ($config) {
                                unset($config);
                            }
                        }
                        return Coroutine::id();
                    });
                }
            }

            public function isEnable(): bool
            {
                return $this->consumerMessage->isEnable();
            }
        };
    }
}
