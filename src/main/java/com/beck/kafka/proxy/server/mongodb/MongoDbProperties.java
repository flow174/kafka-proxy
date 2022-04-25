package com.beck.kafka.proxy.server.mongodb;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;

@Component
@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "mongodb")
public class MongoDbProperties {

  private static final String BASE_URL = "mongodb://%s:%s@%s:%s/?ssl=%s&replicaSet=%s&maxIdleTimeMS=120000&keepAlive=true";

  @NonNull
  private String user;

  @NonNull
  private String password;

  @NonNull
  private String host;

  @NonNull
  private String port;

  @NonNull
  private String database;

  @NonNull
  private String collectionName;

  @NonNull
  private String replicaSet;

  @NonNull
  private String ssl;

  private String appName;

  public String getMongoDbUrl() {
    String mongoDBUrl = String.format(BASE_URL, user, password, host, port, ssl, replicaSet);
    if (!StringUtils.isEmpty(appName)) {
      //Here is for compatibility with cosmosdb
      mongoDBUrl += String.format("&appName=@%s@", appName);
    }
    return mongoDBUrl;
  }
}
