package com.beck.kafka.proxy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class KafkaProxyApplication {

  public static void main(String[] args) {
    SpringApplication.run(KafkaProxyApplication.class, args);
  }

}
