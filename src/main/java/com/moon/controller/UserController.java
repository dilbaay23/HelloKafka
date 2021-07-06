package com.moon.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

/**
 * Created by Moon on 5/07/2021
 */
@RestController
@RequestMapping("/kafka")
public class UserController {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC1= "coder_topic";
    private static final String TOPIC2= "hero_topic";


    @GetMapping("/publish")
    public String post(@RequestParam("message") final String message){
        kafkaTemplate.send(TOPIC1, message);
        return "Published successfully";
    }


    @GetMapping("/hero")
    public void sendMessage(@RequestParam("message") String message) {

        ListenableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(TOPIC2, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=["
                        + message + "] due to : " + ex.getMessage());
            }
        });
    }

    @KafkaListener(topics = "hero_topic", groupId = "hero")   //We can implement multiple listeners for a topic, each with a different group Id
   // @KafkaListener(topics = "topic1, topic2", groupId = "foo")  //one consumer can listen for messages from various topics:
    public void listenGroupFoo(String message) {
        System.out.println("Received Message in group hero: " + message);
    }

    @KafkaListener(topics = "coder_topic", groupId = "coder")  //Spring also supports retrieval of one or more message headers using the @Header annotation in the listener:
    public void listenWithHeaders(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println(
                "Received Message in group coder: " + message
                        + " from partition: " + partition);
    }

   @KafkaListener(
            topicPartitions = @TopicPartition(topic = "hero_topic",
                    partitionOffsets = {
                            @PartitionOffset(partition = "0", initialOffset = "0"),
                            @PartitionOffset(partition = "3", initialOffset = "0")}),
            containerFactory = "partitionsKafkaListenerContainerFactory")   //For a topic with multiple partitions, however, a @KafkaListener can explicitly subscribe to a particular partition of a topic with an initial offset:
    public void listenToPartition(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println(
                "Received Message for Listen To Partitions: " + message
                        + " from partition: " + partition);
    }
}
