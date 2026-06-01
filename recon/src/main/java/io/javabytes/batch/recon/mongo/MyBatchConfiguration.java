package io.javabytes.batch.recon.mongo;

import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MongoJobRepositoryFactoryBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoTransactionManager;


public class MyBatchConfiguration extends DefaultBatchConfiguration {

    private final MongoTransactionManager mongoTransactionManager;

    public MyBatchConfiguration(MongoTransactionManager mongoTransactionManager) {
        this.mongoTransactionManager = mongoTransactionManager;
    }

    // This forces Spring Batch to build a Mongo Job Repository instead of JDBC
    //@Override
    protected JobRepository createJobRepository() throws Exception {
        MongoJobRepositoryFactoryBean factory = new MongoJobRepositoryFactoryBean();
        factory.setTransactionManager(mongoTransactionManager);
        factory.afterPropertiesSet();
        return factory.getObject();
    }
}
