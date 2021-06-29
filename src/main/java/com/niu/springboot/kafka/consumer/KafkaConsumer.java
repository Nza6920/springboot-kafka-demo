package com.niu.springboot.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 消费者
 *
 * @author [nza]
 * @version 1.0 [2021/06/28 15:24]
 * @createTime [2021/06/28 15:24]
 */
@Component
@Slf4j
public class KafkaConsumer {

    /**
     * 监听 topic1 所有消息
     * errorHandler: 异常处理器
     *
     * @param record 消息
     */
    @KafkaListener(topics = {"topic1"}, groupId = "consumer-group-1", id = "consumer1", errorHandler = "consumerAwareErrorHandler")
    public void onMessage1(ConsumerRecord<String, String> record) {
//        throw new RuntimeException("消费异常");
        log.info("kafka 消费消息, topic: {}, partition: {}, value: {}", record.topic(), record.partition(), record.value());
    }

    /**
     * 消费指定 topic partitions
     */
//    @KafkaListener(id = "consumer2", groupId = "consumer-group-1",topicPartitions = {
//            @TopicPartition(topic = "topic1", partitions = { "0" }),
//            @TopicPartition(topic = "topic2", partitions = "0", partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "8"))
//    })
//    public void onMessage2(ConsumerRecord<?, ?> record) {
//        System.out.println("topic:"+record.topic()+"|partition:"+record.partition()+"|offset:"+record.offset()+"|value:"+record.value());
//    }

    /**
     * 批量消费
     *
     * @param records
     */
//    @KafkaListener(id = "consumer2",groupId = "felix-group", topics = "topic1")
    public void onMessage3(List<ConsumerRecord<?, ?>> records) {
        log.info("批量消费一次, records.size(): {}", records.size());
        for (ConsumerRecord<?, ?> record : records) {
            log.info("value: {}", record.value());
        }
    }

    /**
     * 监听 topic1 所有消息
     * errorHandler: 异常处理器
     *
     * @param record 消息
     */
    @KafkaListener(topics = {"topic2"},
            groupId = "consumer-group-1",
            id = "consumer4",
            errorHandler = "consumerAwareErrorHandler",
            containerFactory = "filterContainerFactory")
    public void onMessage4(ConsumerRecord<String, String> record) {
        log.info("kafka 消费消息, topic: {}, partition: {}, value: {}", record.topic(), record.partition(), record.value());
    }


    /**
     * 转发消息
     *
     * @param record 消息体
     * @return {@link String} 转发的消息
     */
    @KafkaListener(topics = {"topic3"},
            groupId = "consumer-group-1",
            id = "consumer5",
            errorHandler = "consumerAwareErrorHandler")
    @SendTo("topic2")
    public String onMessage7(ConsumerRecord<?, ?> record) {
        log.info("消息被转发至 topic2: {}", record.value());
        return String.valueOf(record.value());
    }
}
