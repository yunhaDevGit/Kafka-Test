package com.example.kafkatest.Controller;

import com.example.kafkatest.Service.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {
  private final KafkaProducer kafkaProducer;

  @Autowired
  KafkaController(KafkaProducer kafkaProducer){
    this.kafkaProducer = kafkaProducer;
  }

  @PostMapping
  public String sendMessage(@RequestParam("message") String message){
    this.kafkaProducer.sendMessage(message);
    return "success";
  }

}
