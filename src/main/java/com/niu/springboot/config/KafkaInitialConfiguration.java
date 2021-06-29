package com.niu.springboot.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;

/**
 * kafka 配置类
 *
 * @author [nza]
 * @version 1.0 [2021/06/28 15:19]
 * @createTime [2021/06/28 15:19]
 */
@Configuration
@Slf4j
public class KafkaInitialConfiguration {

    @Autowired
    private ConsumerFactory consumerFactory;

    /**
     * 创建一个名为 testtopic 的 Topic 并设置分区数为 8，分区副本数为2
     *
     * @return {@link NewTopic}
     */
    @Bean
    public NewTopic initialTopic() {
        return new NewTopic("testtopic", 8, (short) 1);
    }

    /**
     * 如果要修改分区数，只需修改配置值重启项目即可
     * 修改分区数并不会导致数据的丢失，但是分区数只能增大不能减小
     *
     * @return {@link NewTopic}
     */
    @Bean
    public NewTopic updateTopic() {
        return new NewTopic("testtopic", 10, (short) 1);
    }


    /**
     * 异常处理器
     *
     * @return {@link ConsumerAwareListenerErrorHandler}
     */
    @Bean
    public ConsumerAwareListenerErrorHandler consumerAwareErrorHandler() {
        return (message, exception, consumer) -> {
            log.error("消费异常, message: {}, consumer: {}, exception: ", message.getPayload(), consumer.toString(), exception);
            return null;
        };
    }

    /**
     * 消息过滤器
     *
     * @return {@link ConcurrentKafkaListenerContainerFactory}
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory filterContainerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory);
        // 被过滤的消息将被丢弃
        factory.setAckDiscarded(true);
        // 消息过滤策略
        factory.setRecordFilterStrategy(consumerRecord -> {
            if (Integer.parseInt(consumerRecord.value().toString()) % 2 == 0) {
                return false;
            }
            //返回true消息则被过滤
            log.info("消息被过滤: {}", consumerRecord.value());
            return true;
        });
        return factory;
    }
}
