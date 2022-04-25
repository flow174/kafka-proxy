package com.beck.kafka.proxy.server.kafka;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "kafka.consumer")
public class KafkaConsumerProperties {

  @NonNull
  private String bootstrapServers;

  @NonNull
  private String username;

  @NonNull
  private String password;

  @NonNull
  private String securityProtocol;

  @NonNull
  private String groupId;
}
