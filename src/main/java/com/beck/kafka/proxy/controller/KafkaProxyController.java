package com.beck.kafka.proxy.controller;

import com.alibaba.fastjson.JSON;
import com.beck.kafka.proxy.exception.CustomException;
import com.beck.kafka.proxy.server.KafkaProxyServer;
import java.text.ParseException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Slf4j
@Controller
@RequestMapping("/api/v1")
public class KafkaProxyController {

  final KafkaProxyServer kafkaProxyServer;

  public KafkaProxyController(KafkaProxyServer kafkaProxyServer) {
    this.kafkaProxyServer = kafkaProxyServer;
  }

  /**
   * Send a message to a specific topic. This interface
   *
   * @param topic   topic name
   * @param message message body
   * @return message uuid
   */
  @PutMapping("/send")
  @ResponseBody
  public String send(@RequestParam String topic, @RequestParam String message) {
    try {
      return kafkaProxyServer.send(topic, message);
    } catch (Exception ex) {
      log.error("send message error : {}", ex.getMessage(), ex);
      return "Send message failed, please try again.";
    }
  }

  /**
   * get message value from mongodb with message uuid
   *
   * @param key message uuid
   * @return kafka message which saved in mongodb, it will be return empty if not been saved
   */
  @GetMapping("/get")
  @ResponseBody
  public String get(@RequestParam String key) {
    try {
      return JSON.toJSONString(kafkaProxyServer.get(key));
    } catch (Exception ex) {
      log.error("get data error : {}", ex.getMessage(), ex);
      return "Get data failed, please try again.";
    }
  }

  /**
   * get a stream data from kafka from a start timestamp to an end timestamp. Timestamp format: yyyy-MM-dd HH:mm:ss
   *
   * @param topic     topic name
   * @param startTime start time, format: yyyy-MM-dd HH:mm:ss
   * @param endTime   end time, format: yyyy-MM-dd HH:mm:ss
   * @return kafka messages
   */
  @GetMapping("/gets")
  @ResponseBody
  public String get(String topic, String startTime, String endTime) {
    try {
      var result = kafkaProxyServer.get(topic, startTime, endTime);
      return JSON.toJSONString(result);
    } catch (CustomException ex) {
      return ex.getMessage();
    } catch (ParseException ex) {
      return "Please follow the timestamp format: yyyy-MM-dd HH:mm:ss";
    } catch (Exception ex) {
      log.error("get stream data error : {}", ex.getMessage(), ex);
      return "Get data failed, please try again";
    }
  }
}
