package io.javabytes.batch.recon.bank;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

@Configuration
public class SourceConfiguration {

    @Bean("sourceTransactionsClient")
    public MongoClient mongoClient() {
        return MongoClients.create("mongodb://localhost:27017");
    }

    @Bean("sourceTransactionsTemplate")
    public MongoTemplate mongoTemplate(@Qualifier("sourceTransactionsClient") MongoClient mongoClient) {
        var mongoTemplate = new MongoTemplate(mongoClient, "job_history");
        var mappingMongoConverter = (MappingMongoConverter) mongoTemplate.getConverter();
        mappingMongoConverter.setMapKeyDotReplacement(".");

        mappingMongoConverter.afterPropertiesSet();
        return mongoTemplate;

    }
}
