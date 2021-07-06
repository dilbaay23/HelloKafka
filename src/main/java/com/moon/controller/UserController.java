package com.moon.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
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
}
