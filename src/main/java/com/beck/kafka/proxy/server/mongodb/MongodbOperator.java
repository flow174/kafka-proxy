package com.beck.kafka.proxy.server.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MongodbOperator {

  protected MongoCollection<Document> collection;

  public static final String RECORD_KEY = "key";

  /**
   * Initialize mongodb collection operator
   *
   * @param collectionFactory MongoDbCollectionFactory
   * @param mongoDbProperties MongoDbProperties
   */
  public MongodbOperator(MongoDbCollectionFactory collectionFactory, MongoDbProperties mongoDbProperties) {
    String collectionName = mongoDbProperties.getCollectionName();
    if (!collectionFactory.hasCollection(collectionName)) {
      log.info("There is no {} collection. Create it.", collectionName);
      collectionFactory.createCollection(collectionName);
      collection = collectionFactory.getCollection(collectionName);
      createIndexes();
    } else {
      collection = collectionFactory.getCollection(collectionName);
    }
  }

  private void createIndexes() {
    IndexOptions indexOptions = new IndexOptions().unique(true).background(false).name("key-unique-index");
    Document doc = new Document();
    doc.append(RECORD_KEY, 1);
    collection.createIndex(doc, indexOptions);
  }

  /**
   * find a document in database
   *
   * @param uuid message uuid
   * @return Document
   */
  public Document find(String uuid) {
    Bson filter = Filters.eq(RECORD_KEY, uuid);
    return collection.find(filter).first();
  }
}
