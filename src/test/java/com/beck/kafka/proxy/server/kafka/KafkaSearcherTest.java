package com.beck.kafka.proxy.server.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class KafkaSearcherTest {

  KafkaConsumer<String, String> kafkaConsumer;

  KafkaSearcher kafkaSearcher;

  @Test
  @DisplayName("test-empty-partitions")
  void case1() {
    kafkaConsumer = mock(KafkaConsumer.class);
    kafkaSearcher = new KafkaSearcher(kafkaConsumer);

    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000 * 60 * 60;

    var result = kafkaSearcher.getSpanRecord("test", startTime, endTime);
    assertTrue(result.isEmpty());
  }

  @Test
  @DisplayName("test-empty-partitions")
  void case2() {
    kafkaConsumer = mock(KafkaConsumer.class);
    kafkaSearcher = new KafkaSearcher(kafkaConsumer);

    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000 * 60 * 60;

    PartitionInfo partitionInfo = new PartitionInfo("test", 0, null, null, null);
    List<PartitionInfo> partitionInfos = Arrays.asList(partitionInfo);
    when(kafkaConsumer.partitionsFor("test")).thenReturn(partitionInfos);

    TopicPartition topicPartition = new TopicPartition("test", 0);
    OffsetAndTimestamp offsetAndTimestamp = new OffsetAndTimestamp(1, endTime);
    Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = new HashMap<>();
    offsetAndTimestampMap.put(topicPartition, offsetAndTimestamp);
    when(kafkaConsumer.offsetsForTimes(any())).thenReturn(offsetAndTimestampMap);

    Map<TopicPartition, Long> topicPartitionLongMap = new HashMap<>();
    topicPartitionLongMap.put(topicPartition, 1L);
    when(kafkaConsumer.endOffsets(any())).thenReturn(topicPartitionLongMap);

    ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<String, String>("test", 0, 1, "key", "value");
    List<ConsumerRecord<String, String>> consumerRecordList = Arrays.asList(consumerRecord);
    Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
    records.put(topicPartition, consumerRecordList);
    ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(records);
    when(kafkaConsumer.poll(any())).thenReturn(consumerRecords);

    var result = kafkaSearcher.getSpanRecord("test", startTime, endTime);
    assertEquals(1, result.size());
  }
}