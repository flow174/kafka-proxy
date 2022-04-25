package com.beck.kafka.proxy.server.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MongoDbCollectionFactory {

  private MongoDatabase database;

  private MongoClient mongoClient;

  private final MongoDbProperties properties;

  public MongoDbCollectionFactory(MongoDbProperties properties) {
    String url = properties.getMongoDbUrl();
    String database = properties.getDatabase();
    log.info("MongoDb url: {}, database: {}", url, database);

    MongoClientURI uri = new MongoClientURI(url);
    this.properties = properties;
    this.mongoClient = new MongoClient(uri);
    this.database = mongoClient.getDatabase(database);
  }

  /**
   * get collection
   *
   * @param collectionName collection name
   * @return MongoCollection<Document>
   */
  public MongoCollection<Document> getCollection(String collectionName) {
    try {
      return database.getCollection(collectionName);
    } catch (Exception e) {
      log.error("get collection error", e);

      if (e instanceof IllegalArgumentException) {
        throw e;
      }

      // if not argument issue we can try again
      reConnect();
    }
    return database.getCollection(collectionName);
  }

  private void reConnect() {
    try {
      this.mongoClient.close();
    } catch (Exception e) {
      log.error("Close mongodb exception", e);
    }

    // if this connection failed, the server will be closed
    this.mongoClient = new MongoClient(this.properties.getMongoDbUrl());
    this.database = mongoClient.getDatabase(properties.getDatabase());
  }

  /**
   * check collection exist
   *
   * @param collectionName collection name
   * @return true: exist false: need to create collection
   */
  public boolean hasCollection(String collectionName) {
    MongoIterable<String> names = database.listCollectionNames();
    for (String name : names) {
      if (name.equals(collectionName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * create collection
   *
   * @param collectionName collection name
   */
  public void createCollection(String collectionName) {
    database.createCollection(collectionName);
  }
}
