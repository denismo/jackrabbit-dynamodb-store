package org.apache.jackrabbit.oak.plugins.document.dynamodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * User: Denis Mikhalkin
 * Date: 25/11/2014
 * Time: 1:31 PM
 */
public class DynamoDBStoreBaseTest {

    public static ContentRepository createRepository(boolean reset) {
        if (isDynamoDBStore()) {
            AmazonDynamoDBClient dynamodb = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());
            dynamodb.setEndpoint("http://localhost:8000");
            performReset(reset, dynamodb);
            NodeStore store = new DocumentMK.Builder()
                    .setDynamoDB(dynamodb)
                    .open().getNodeStore();
            return new Oak(store)
                    .with(new InitialContent())
                    .with(new OpenSecurityProvider())
                    .createContentRepository();

        }
        return null;
    }

    public static void performReset(boolean reset, AmazonDynamoDBClient dynamodb) {
        if (reset) {
            try {
                dynamodb.deleteTable("nodes");
            } catch (AmazonClientException e) {
            }
            try {
                dynamodb.deleteTable("settings");
            } catch (AmazonClientException e) {
            }
            try {
                dynamodb.deleteTable("clusterNodes");
            } catch (AmazonClientException e) {
            }
        }
    }

    public static Oak createOak(boolean reset) {
        if (isDynamoDBStore()) {
            AmazonDynamoDBClient dynamodb = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());
            dynamodb.setEndpoint("http://localhost:8000");
            performReset(reset, dynamodb);
            NodeStore store = new DocumentMK.Builder()
                    .setDynamoDB(dynamodb)
                    .open().getNodeStore();
            return new Oak(store)
                    .with(new InitialContent())
                    .with(new OpenSecurityProvider());
        }
        return null;
    }


    public static boolean isDynamoDBStore() {
        return "true".equals(System.getenv("forceDynamoDBStore")) || "true".equals(System.getProperty("forceDynamoDBStore"));
    }

}
