package com.example.kafkatest.Service;

import java.io.IOException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

  @KafkaListener(topics = "test", groupId = "consumerGroup")
  public void consume(String message) throws IOException {
    System.out.println(String.format("Consumed message : %s", message));
  }

}
