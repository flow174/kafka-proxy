package com.beck.kafka.proxy.server.kafka;

import java.util.UUID;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

  private final KafkaTemplate<String, String> kafkaTemplate;

  public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  /**
   * send message, the error will be caught in the outer
   *
   * @param topic   topic name
   * @param message message body
   * @return message uuid
   */
  public String send(String topic, String message) {
    String key = UUID.randomUUID().toString();
    kafkaTemplate.send(topic, key, message);
    return key;
  }
}
