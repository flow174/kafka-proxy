package com.beck.kafka.proxy.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.beck.kafka.proxy.exception.CustomException;
import com.beck.kafka.proxy.server.kafka.KafkaProducer;
import com.beck.kafka.proxy.server.kafka.KafkaSearcher;
import com.beck.kafka.proxy.server.mongodb.MongodbOperator;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class KafkaProxyServerTest {

  KafkaSearcher kafkaSearcher;

  KafkaProducer kafkaProducer;

  MongodbOperator mongodbOperator;

  KafkaProxyServer kafkaProxyServer;

  @Test
  @DisplayName("test-get-return-document")
  void case1() {

    kafkaSearcher = mock(KafkaSearcher.class);
    kafkaProducer = mock(KafkaProducer.class);
    mongodbOperator = mock(MongodbOperator.class);

    kafkaProxyServer = new KafkaProxyServer(kafkaSearcher, kafkaProducer, mongodbOperator);

    Document document = new Document();
    when(mongodbOperator.find(anyString())).thenReturn(document);

    var result = kafkaProxyServer.get("test");
    assertEquals(result, document);
  }

  @Test
  @DisplayName("test-get-return-null")
  void case2() {

    kafkaSearcher = mock(KafkaSearcher.class);
    kafkaProducer = mock(KafkaProducer.class);
    mongodbOperator = mock(MongodbOperator.class);

    kafkaProxyServer = new KafkaProxyServer(kafkaSearcher, kafkaProducer, mongodbOperator);

    var result = kafkaProxyServer.get("test");
    assertEquals(result, "Cannot found a value in mongodb");
  }

  @Test
  @DisplayName("test-send-message-success")
  void case3() {

    kafkaSearcher = mock(KafkaSearcher.class);
    kafkaProducer = mock(KafkaProducer.class);
    mongodbOperator = mock(MongodbOperator.class);

    kafkaProxyServer = new KafkaProxyServer(kafkaSearcher, kafkaProducer, mongodbOperator);

    String uuid = UUID.randomUUID().toString();
    when(kafkaProducer.send(anyString(), anyString())).thenReturn(uuid);

    var result = kafkaProxyServer.send("test", "message");
    assertEquals(result, uuid);
  }

  @Test
  @DisplayName("test-get-stream-data-success")
  void case4() throws Exception {

    kafkaSearcher = mock(KafkaSearcher.class);
    kafkaProducer = mock(KafkaProducer.class);
    mongodbOperator = mock(MongodbOperator.class);

    kafkaProxyServer = new KafkaProxyServer(kafkaSearcher, kafkaProducer, mongodbOperator);

    List<Object> expectedResult = new ArrayList<>();
    when(kafkaSearcher.getSpanRecord(anyString(), anyLong(), anyLong())).thenReturn(expectedResult);

    var result = kafkaProxyServer.get("test", "2022-01-12 12:00:00", "2022-01-12 13:00:00");
    assertEquals(result, expectedResult);
  }

  @Test
  @DisplayName("test-get-stream-data-failed1")
  void case5() {

    kafkaSearcher = mock(KafkaSearcher.class);
    kafkaProducer = mock(KafkaProducer.class);
    mongodbOperator = mock(MongodbOperator.class);

    kafkaProxyServer = new KafkaProxyServer(kafkaSearcher, kafkaProducer, mongodbOperator);

    List<Object> expectedResult = new ArrayList<>();
    when(kafkaSearcher.getSpanRecord(anyString(), anyLong(), anyLong())).thenReturn(expectedResult);

    assertThrows(CustomException.class, () -> kafkaProxyServer.get("test", "2022-01-12 13:00:00", "2022-01-12 12:00:00"));
  }

  @Test
  @DisplayName("test-get-stream-data-failed2")
  void case6() {

    kafkaSearcher = mock(KafkaSearcher.class);
    kafkaProducer = mock(KafkaProducer.class);
    mongodbOperator = mock(MongodbOperator.class);

    kafkaProxyServer = new KafkaProxyServer(kafkaSearcher, kafkaProducer, mongodbOperator);

    List<Object> expectedResult = new ArrayList<>();
    when(kafkaSearcher.getSpanRecord(anyString(), anyLong(), anyLong())).thenReturn(expectedResult);

    assertThrows(ParseException.class, () -> kafkaProxyServer.get("test", "2022-012 11:00:00", "2022-01-12 12:00:00"));
  }
}