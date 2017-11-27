<?php
/**
 * Created by PhpStorm.
 * User: hunter
 * Date: 2017/8/31
 * Time: 10:08
 */
namespace hisensetv\components\kafka;

use Yii;

class KafkaConsumer
{
    #kafkaz主要配置
    private $_config;
    #kafka获取的所有数据
    private $kafka_data = [];
    #kafka topic配置
    private $topic_config;

    #初始化配置
    public function __construct($serverName)
    {
        $this->_config = Yii::$app->params['hisensetv_kafka'][$serverName];

        $this->topic_config = $this->topicConfig();
    }

    #初始化topic
    public function topicConfig()
    {
        $topicConf = new \RdKafka\TopicConf();
        #配置没有设置偏移初始量时的消费消息点 smallest表示从最开始消费
        $topicConf->set('auto.offset.reset', 'smallest');
        #offset存储方式
//        $topicConf->set('offset.store.method', 'file');
        $topicConf->set('offset.store.method', 'broker');
        #offset存储路径
//        $topicConf->set('offset.store.path', sys_get_temp_dir());
        #自动提交offset时间间隔 0代表禁用
        $topicConf->set("auto.commit.interval.ms", 100);

        return $topicConf;
    }

    //根据配置信息获取所有数据
    public function main()
    {
        $config = $this->_config;
        $conf = new \RdKafka\Conf;
        #定义消费组
        $conf->set('group.id', $config['groupId']);
        $rk = new \RdKafka\Consumer($conf);
        #设置日志级别
        $rk->setLogLevel(LOG_DEBUG);
        #集群地址
        $rk->addBrokers($config["broker"]);
        #跟踪话题
        $topic = $rk->newTopic($config["topic"], $this->topic_config);
        #第一个参数 要消耗的分区 ， 第二个参数 开始消耗的偏移量  开始使用0分区
        $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);

        while (true) {
            #第一个参数 要消耗的分区 ， 第二个参数 超时时间
            $message = $topic->consume(0, 120*10000);

            switch ($message->err) {
                #消息传递成功
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $notice = json_decode($message->payload,true);
                    $notice['topic_name'] = $message->topic_name;
                    array_push($this->kafka_data,$notice);
                    break;
                #消费者到达分区结尾
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
//                    $topic->consumeStop(0);
                    return $this->kafka_data;
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    Yii::trace($message->err.$message->errstr());
                    break;
            }
        }

    }
}

