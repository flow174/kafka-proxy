package com.beck.kafka.proxy.server.kafka;

import static com.beck.kafka.proxy.common.Constants.DATE_TIME_FORMAT;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaSearcher {

  private final KafkaConsumer<String, String> kafkaConsumer;

  private static Duration ONE_SECOND_DURATION = Duration.ofSeconds(1);

  public KafkaSearcher(KafkaConsumer<String, String> kafkaConsumer) {
    this.kafkaConsumer = kafkaConsumer;
  }

  public List<Object> getSpanRecord(String topic, Long startTime, Long endTime) {
    DateFormat dateFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
    List<Object> results = new ArrayList<>();

    // get all partition information of topic
    List<PartitionInfo> partitionInfos = getAllPartitionInfo(topic);
    if (partitionInfos.isEmpty()) {
      return results;
    }

    Map<TopicPartition, OffsetAndTimestamp> startTimeOffsetMap = getOffsetMapWithTimestamp(topic, startTime, partitionInfos);

    // start set each partition initial offset
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : startTimeOffsetMap.entrySet()) {
      // if the set query offset time is greater than the maximum index record time, then the value is null
      if (entry.getValue() != null) {
        handleOnePartition(endTime, dateFormat, results, entry);
      }
    }

    return results;
  }

  private void handleOnePartition(Long endTime, DateFormat dateFormat, List<Object> results, Entry<TopicPartition, OffsetAndTimestamp> entry) {
    TopicPartition topicPartition = entry.getKey();
    OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
    String partitionDate = dateFormat.format(new Date(offsetAndTimestamp.timestamp()));
    log.info("partition : {} time : {} offset: {}", topicPartition.partition(), partitionDate, offsetAndTimestamp.offset());
    // assign one partition
    kafkaConsumer.assign(Arrays.asList(topicPartition));
    // set given offset
    kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());
    // get max offset
    long maxOffset = getMaxOffset(topicPartition);
    // start poll one partition data
    boolean isFinished = false;
    while (true) {
      ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(ONE_SECOND_DURATION);
      isFinished = polledAllData(endTime, results, maxOffset, consumerRecords);
      if (isFinished) {
        break;
      }
    }
  }

  private boolean polledAllData(Long endTime, List<Object> results, long maxOffset, ConsumerRecords<String, String> consumerRecords) {
    for (ConsumerRecord record : consumerRecords) {
      // is exceeded end time
      if (isExceededEndTime(endTime, record)) {
        return true;
      }
      // add message value
      results.add(record.value());
      // is last one
      if (isLast(maxOffset, record)) {
        return true;
      }
    }
    return false;
  }

  private boolean isExceededEndTime(Long endTime, ConsumerRecord record) {
    return record.timestamp() > endTime;
  }

  private boolean isLast(long maxOffset, ConsumerRecord record) {
    return record.offset() >= maxOffset;
  }

  private List<PartitionInfo> getAllPartitionInfo(String topic) {
    return kafkaConsumer.partitionsFor(topic);
  }

  private Map<TopicPartition, OffsetAndTimestamp> getOffsetMapWithTimestamp(String topic, Long timestamp, List<PartitionInfo> partitionInfos) {
    Map<TopicPartition, Long> endTimePartitionMap = new HashMap<>();
    for (PartitionInfo partitionInfo : partitionInfos) {
      endTimePartitionMap.put(new TopicPartition(topic, partitionInfo.partition()), timestamp);
    }

    // get each partition the offset for a given time
    return kafkaConsumer.offsetsForTimes(endTimePartitionMap);
  }

  private long getMaxOffset(TopicPartition topicPartition) {
    return kafkaConsumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition) - 1;
  }
}
