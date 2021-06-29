package com.niu.springboot.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 生产者控制器
 *
 * @author [nza]
 * @version 1.0 [2021/06/28 15:21]
 * @createTime [2021/06/28 15:21]
 */
@RestController
@RequestMapping("/kafka")
@Api("生产者")
@Slf4j
public class KafkaProducerController {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * 发送消息
     *
     * @param normalMessage 消息
     */
    @GetMapping("/normal/topic1/{message}")
    @ApiOperation("发送消息 topic1")
    public void sendMessage1(@PathVariable("message") @ApiParam("消息体") String normalMessage) {
        kafkaTemplate.send("topic1", normalMessage);
    }

    /**
     * 发送消息
     *
     * @param normalMessage 消息
     */
    @GetMapping("/normal/topic2/{message}")
    @ApiOperation("发送消息 topic2")
    public void sendMessage4(@PathVariable("message") @ApiParam("消息体") String normalMessage) {
        kafkaTemplate.send("topic2", normalMessage);
    }

    /**
     * 发送消息
     *
     * @param normalMessage 消息
     */
    @GetMapping("/normal/topic3/{message}")
    @ApiOperation("发送消息 topic3")
    public void sendMessage5(@PathVariable("message") @ApiParam("消息体") String normalMessage) {
        kafkaTemplate.send("topic3", normalMessage);
    }

    /**
     * 发送消息
     *
     * @param normalMessage 消息
     */
    @GetMapping("/normal/topic4/{message}")
    @ApiOperation("发送消息 topic4")
    public void sendMessage6(@PathVariable("message") @ApiParam("消息体") String normalMessage) {
        kafkaTemplate.send("topic4", normalMessage);
    }

    /**
     * 带有回调的消息发送
     *
     * @param callbackMessage 消息体
     */
    @GetMapping("/callbackTwo/{message}")
    @ApiOperation("带有回调的消息")
    public void sendMessage2(@PathVariable("message") @ApiParam("消息体") String callbackMessage) {

        kafkaTemplate.send("topic1", callbackMessage).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("发送消息失败: ", ex);
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                log.info("发送消息成功, topic: {}, partition: {}, offset: {} ", result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    /**
     * 事务提交
     *
     * @param transactionMessage 消息体
     */
    @GetMapping("transaction/{message}")
    @ApiOperation("事务提交")
    public void sendMessage3(@PathVariable("message") @ApiParam("消息体") String transactionMessage) {

        kafkaTemplate.executeInTransaction(operation -> {
            // 后面抛出了异常, 这条消息不会发出
            operation.send("topic1", transactionMessage);

            throw new RuntimeException("测试 kafka 事务提交");
        });
    }
}
