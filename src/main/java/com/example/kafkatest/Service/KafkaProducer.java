package com.example.kafkatest.Service;

import com.example.kafkatest.Model.Greeting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class KafkaProducer {
//  private static final String TOPIC = "test";

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private KafkaTemplate<String, Greeting> greetingKafkaTemplate;

  @Value(value = "${message.topic.name}")
  private String topicName;

  @Value(value = "${partitioned.topic.name")
  private String partitionedTopicName;

  @Value(value = "${filtered.topic.name")
  private String filteredTopicName;

  @Value(value = "${greeting.topic.name}")
  private String greetingTopicName;

  @Autowired
  public KafkaProducer(KafkaTemplate kafkaTemplate){
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendMessage(String message){
//    System.out.println(String.format("Produce message : %s", message));
//    this.kafkaTemplate.send(TOPIC, message);
    ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
    future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
      @Override
      public void onFailure(Throwable ex) {
        System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
      }

      @Override
      public void onSuccess(SendResult<String, String> result) {
        System.out.println("Send message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
      }
    });
  }

  public void sendMessageToPartition(String message, int partition) {
    kafkaTemplate.send(partitionedTopicName, partition, null, message);
  }

  public void sendMessageToFiltered(String message) {
    kafkaTemplate.send(filteredTopicName, message);
  }

  public void sendGreetingMessage(Greeting greeting) {
    greetingKafkaTemplate.send(greetingTopicName, greeting);
  }
}
