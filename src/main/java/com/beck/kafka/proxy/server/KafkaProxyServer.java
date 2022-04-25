package com.beck.kafka.proxy.server;

import static com.beck.kafka.proxy.common.Constants.DATE_TIME_FORMAT;

import com.beck.kafka.proxy.exception.CustomException;
import com.beck.kafka.proxy.server.kafka.KafkaProducer;
import com.beck.kafka.proxy.server.kafka.KafkaSearcher;
import com.beck.kafka.proxy.server.mongodb.MongodbOperator;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class KafkaProxyServer {


  final KafkaSearcher kafkaSearcher;

  final KafkaProducer kafkaProducer;

  final MongodbOperator mongodbOperator;

  public KafkaProxyServer(KafkaSearcher kafkaSearcher,
                          KafkaProducer kafkaProducer,
                          MongodbOperator mongodbOperator) {
    this.kafkaSearcher = kafkaSearcher;
    this.kafkaProducer = kafkaProducer;
    this.mongodbOperator = mongodbOperator;
  }

  /**
   * send message
   *
   * @param topic   topic name
   * @param message message body
   * @return message uuid
   */
  public String send(String topic, String message) {
    return kafkaProducer.send(topic, message);
  }

  /**
   * get data from mongodb with message uuid
   *
   * @param key message uuid
   * @return message data
   */
  public Object get(String key) {
    var result = mongodbOperator.find(key);
    if (result != null) {
      return result;
    }
    return "Cannot found a value in mongodb";
  }

  /**
   * get a stream data from kafka with start time and end time
   *
   * @param topic topic name
   * @param start start time
   * @param end   end time
   * @return a list of data
   * @throws ParseException parse timestamp issue
   */
  public List<Object> get(String topic, String start, String end) throws ParseException, CustomException {
    Long startTime = dateToStamp(start);
    Long endTime = dateToStamp(end);

    if (startTime > System.currentTimeMillis()) {
      throw new CustomException("start time must be less than current time");
    }

    if (endTime <= startTime) {
      throw new CustomException("end time must be greater than start time");
    }

    if (endTime > System.currentTimeMillis()) {
      endTime = System.currentTimeMillis();
    }

    return kafkaSearcher.getSpanRecord(topic, startTime, endTime);
  }

  private Long dateToStamp(String s) throws ParseException {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
    Date date = simpleDateFormat.parse(s);
    return date.getTime();
  }
}
