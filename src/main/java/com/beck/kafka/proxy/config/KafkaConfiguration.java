package com.beck.kafka.proxy.config;

import com.beck.kafka.proxy.server.kafka.KafkaConsumerProperties;
import com.beck.kafka.proxy.server.kafka.KafkaProducerProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.StringUtils;

@Configuration
public class KafkaConfiguration {

  public static final String PLAIN_SASL_TEMPLATE = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";

  @Bean
  public KafkaTemplate<String, String> kafkaProducerTemplate(KafkaProducerProperties kafkaProperties) {
    Map<String, Object> props = producerConfigs(kafkaProperties);
    var kafkaFactory = new DefaultKafkaProducerFactory<String, String>(props);
    return new KafkaTemplate<>(kafkaFactory);
  }

  @Bean
  public KafkaConsumer<String, String> kafkaConsumer(KafkaConsumerProperties kafkaProperties) {
    Map<String, Object> props = consumerConfigs(kafkaProperties);
    return new KafkaConsumer<>(props);
  }

  private Map<String, Object> producerConfigs(KafkaProducerProperties kafkaProperties) {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put("security.protocol", kafkaProperties.getSecurityProtocol());
    props.put("sasl.jaas.config", getSaslConfig(kafkaProperties.getUsername(), kafkaProperties.getPassword()));
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put("sasl.mechanism", "PLAIN");
    return props;
  }

  private Map<String, Object> consumerConfigs(KafkaConsumerProperties kafkaConsumerProperties) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConsumerProperties.getBootstrapServers());
    props.put("security.protocol", kafkaConsumerProperties.getSecurityProtocol());
    props.put("sasl.jaas.config", getSaslConfig(kafkaConsumerProperties.getUsername(), kafkaConsumerProperties.getPassword()));
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("sasl.mechanism", "PLAIN");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerProperties.getGroupId());
    return props;
  }

  private String getSaslConfig(String username, String password) {
    // Here is for compatibility with azure eventhub
    if (StringUtils.isEmpty(username) || "ConnectionString".equals(username)) {
      username = "$ConnectionString";
    }
    return String.format(PLAIN_SASL_TEMPLATE, username, password);
  }
}
