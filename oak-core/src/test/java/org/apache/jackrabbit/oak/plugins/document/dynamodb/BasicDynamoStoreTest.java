package org.apache.jackrabbit.oak.plugins.document.dynamodb;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.query.Query;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.*;

/**
 * User: Denis Mikhalkin
 * Date: 20/11/2014
 * Time: 10:23 PM
 */
public class BasicDynamoStoreTest {

    private ContentRepository repository;

    @Before
    public void prepare() {
        AmazonDynamoDBClient dynamodb = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());
        dynamodb.setEndpoint("http://localhost:8000");
        NodeStore store = new DocumentMK.Builder()
                .setDynamoDB(dynamodb)
                .open().getNodeStore();
        repository = new Oak(store)
                .with(new OpenSecurityProvider())
                .createContentRepository();
    }

    @After
    public void teardown() {
    }

    @Test
    public void testGet() throws LoginException, NoSuchWorkspaceException, ParseException, CommitFailedException, IOException {
        ContentSession s = repository.login(null, null);
        try {
            Root r = s.getLatestRoot();
            Tree t = r.getTree("/").addChild("test");
            t.addChild("node1").setProperty("jcr:primaryType", "nt:base");
            t.addChild("node2").setProperty("jcr:primaryType", "nt:base");
            t.addChild("node3").setProperty("jcr:primaryType", "nt:base");
            r.commit();

            Tree testTree = r.getTree("/test");
            assertTrue(testTree.exists());
            Tree node2 = testTree.getChild("node2");
            assertTrue(node2.exists());

            ContentSession s2 = repository.login(null, null);
            Root r2 = s2.getLatestRoot();

            node2.remove();
            r.commit();

            Result result = r2.getQueryEngine().executeQuery(
                    "test//element(*, nt:base)", Query.XPATH, Long.MAX_VALUE, 0,
                    QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
            Set<String> paths = new HashSet<String>();
            for (ResultRow rr : result.getRows()) {
                paths.add(rr.getPath());
            }
            assertEquals(new HashSet<String>(Arrays.asList("/test/node1", "/test/node2", "/test/node3")), paths);
        } finally {
            s.close();
        }
    }
}
