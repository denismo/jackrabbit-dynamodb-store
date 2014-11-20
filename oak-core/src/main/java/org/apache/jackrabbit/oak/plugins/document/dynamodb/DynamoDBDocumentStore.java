package org.apache.jackrabbit.oak.plugins.document.dynamodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.*;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.cache.CachingDocumentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.util.*;

/**
 * Collection is table.
 * Documents map to nodes.
 * Key is DynamoDB key.
 * User: Denis Mikhalkin
 * Date: 13/11/2014
 * Time: 11:06 PM
 * TODO: Write integration test (similar to MongoIT)
 * TODO: Implement initialization service
 * TODO: Initialize Git in repository
 */
public class DynamoDBDocumentStore implements CachingDocumentStore {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBDocumentStore.class);

    private final Comparator<Revision> comparator = StableRevisionComparator.REVERSE;
    AmazonDynamoDBClient dynamoDB;

    public DynamoDBDocumentStore() {
        dynamoDB = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());
        dynamoDB.setRegion(Region.getRegion(Regions.AP_SOUTHEAST_2));

        init();
    }

    private void init() {
        ensureTableExists(collectionToTable(Collection.NODES));
        ensureTableExists(collectionToTable(Collection.CLUSTER_NODES));
        ensureTableExists(collectionToTable(Collection.SETTINGS));
    }

    public DynamoDBDocumentStore(AmazonDynamoDBClient client) {
        dynamoDB = client;
        init();
    }

    private void ensureTableExists(String tableName) {
        try {
            dynamoDB.describeTable(tableName);
        } catch (ResourceNotFoundException rnfe) {
            // Table does not exist
            dynamoDB.createTable(new CreateTableRequest(tableName, Arrays.asList(new KeySchemaElement(Document.ID, KeyType.HASH))));
        } catch (AmazonClientException e) {
            LOG.error("Exception checking Nodes table", e);
        }
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        GetItemResult res = dynamoDB.getItem(new GetItemRequest(collectionToTable(collection), Collections.singletonMap(Document.ID, new AttributeValue(key))));
        if (res != null) {
            return itemToDocument(res.getItem());
        }
        return null;
    }

    private <T extends Document> T itemToDocument(Map<String, AttributeValue> item) {
        return null;
    }

    private <T extends Document> String collectionToTable(Collection<T> collection) {
        return collection.name;
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key, int maxAge) {
        return find(collection, key); // ignore maxAge as we are not caching
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
        return query(collection, fromKey, toKey, null, 0, limit);
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, String indexedProperty, long startValue, int limit) {
        // fromKey, toKey - assumes "key" is ordered sequence
        // indexedProperty - assumes ordered property, startValue refers to its value and above. Can be MODIFIED_IN_SECS, HAS_BINARY_FLAG
        QueryRequest request = new QueryRequest(collectionToTable(collection));
        request.addExclusiveStartKeyEntry(Document.ID, new AttributeValue(fromKey));
        request.addKeyConditionsEntry(Document.ID, new Condition().withAttributeValueList(new AttributeValue(fromKey), new AttributeValue(toKey)).withComparisonOperator(ComparisonOperator.BETWEEN));
        if (indexedProperty != null) {
            // Assumes this kind of query will only ever run using a local index
            request.addKeyConditionsEntry(indexedProperty, new Condition().withAttributeValueList(new AttributeValue(String.valueOf(startValue))).withComparisonOperator(ComparisonOperator.GE));
            request.setIndexName(indexedProperty+"Index");
        }
        request.setSelect(Select.ALL_ATTRIBUTES);
        request.setLimit(limit);
        QueryResult result = dynamoDB.query(request);
        if (result != null) {
            ArrayList<T> list = new ArrayList<T>();
            for (Map<String, AttributeValue> item : result.getItems()) {
                list.add((T)itemToDocument(item));
            }
            return list;
        }
        return Collections.emptyList();
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        dynamoDB.deleteItem(new DeleteItemRequest(collectionToTable(collection), Collections.singletonMap(Document.ID, new AttributeValue(key))));
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        for (String key : keys) {
            remove(collection, key);
        }
    }

    @CheckForNull
    private <T extends Document> T findAndModify(Collection<T> collection,
                                                 UpdateOp updateOp,
                                                 boolean upsert, // insert if non-existing
                                                 boolean checkConditions) {
        // make sure we don't modify the original updateOp
        // Perform update of an item, according to updateOps.
        // Check condition ensures that an entry in a map is present (for any CONTAINS_MAP_ENTRY op) - those ops are ignored otherwise

        UpdateItemRequest request = getUpdateItemRequest(collection, updateOp, upsert, checkConditions);
        try {
            UpdateItemResult result = dynamoDB.updateItem(request);
            if (result.getAttributes().size() == 0) {
                return null;
            }
            return itemToDocument(result.getAttributes());
        } catch (ConditionalCheckFailedException cf) {
            LOG.error("Conditional check violation for " + updateOp.getId() + " during findAndModify", cf);
            return handleMaxUpdate(collection, updateOp, upsert, checkConditions, cf);
        } catch (ResourceNotFoundException rnf) {
            LOG.error("Resource not found for " + updateOp.getId() + " during findAndModify", rnf);
            throw new MicroKernelException(rnf);
        } catch (Throwable t) {
            throw new MicroKernelException(t);
        }
    }

    private <T extends Document> T handleMaxUpdate(Collection<T> collection, UpdateOp updateOp, boolean upsert, boolean checkConditions, ConditionalCheckFailedException cf) {
        Map<String, Comparable> maxValues = getMaxValues(collection, updateOp);

        // No max values - means violation of some other constraint
        if (maxValues == null) throw new MicroKernelException(cf);

        UpdateOp copy = updateOp.copy();
        boolean greaterExists = false;
        for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> entry : updateOp.getChanges().entrySet()) {
            UpdateOp.Key k = entry.getKey();
            UpdateOp.Operation op = entry.getValue();
            if (op.type == UpdateOp.Operation.Type.MAX) {
                if (maxValues.containsKey(k.getName()) && maxValues.get(k.getName()).compareTo(op.value) > 0) {
                    greaterExists = true;
                    copy.max(k.getName(), maxValues.get(k.getName()));
                }
            }
        }
        // If there was MAX constraint violation, it means there was general constraint violation - just report it
        if (!greaterExists) {
            throw new MicroKernelException(cf);
        }
        try {
            return findAndModify(collection, copy, upsert, checkConditions);
        } catch (Throwable t) {
            throw new MicroKernelException(t);
        }
    }

    private <T extends Document> Map<String, Comparable> getMaxValues(Collection<T> collection, UpdateOp updateOp) {
        GetItemRequest request = new GetItemRequest()
                .withTableName(collectionToTable(collection))
                .addKeyEntry(Document.ID, new AttributeValue(updateOp.getId()));
        ArrayList<String> maxAttributes = new ArrayList<String>();
        for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> entry : updateOp.getChanges().entrySet()) {
            UpdateOp.Key k = entry.getKey();
            UpdateOp.Operation op = entry.getValue();
            if (op.type == UpdateOp.Operation.Type.MAX) {
                maxAttributes.add(k.getName());
            }
        }
        if (maxAttributes.size() == 0) {
            return null;
        }
        request = request.withAttributesToGet(maxAttributes);
        GetItemResult result = dynamoDB.getItem(request);
        Map<String, Comparable> ret = new HashMap<String, Comparable>();
        for (Map.Entry<String, AttributeValue> entry : result.getItem().entrySet()) {
            ret.put(entry.getKey(), Long.valueOf(entry.getValue().getN()));
        }
        return ret;
    }

    private <T extends Document> UpdateItemRequest getUpdateItemRequest(Collection<T> collection, UpdateOp updateOp, boolean upsert, boolean checkConditions) {
        UpdateItemRequest request = new UpdateItemRequest()
                .withTableName(collectionToTable(collection))
                .addKeyEntry(Document.ID, new AttributeValue(updateOp.getId()))
                .withReturnValues(ReturnValue.ALL_OLD)
                .addAttributeUpdatesEntry(Document.MOD_COUNT, new AttributeValueUpdate(new AttributeValue("1"), AttributeAction.ADD));

        for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> entry : updateOp.getChanges().entrySet()) {
            UpdateOp.Key k = entry.getKey();
            if (k.getName().equals(Document.ID) || k.getName().equals(Document.MOD_COUNT)) {
                // avoid exception "Mod on _id not allowed"
                continue;
            }
            UpdateOp.Operation op = entry.getValue();
            switch (op.type) {
                case SET:
                case SET_MAP_ENTRY:
                    request.addAttributeUpdatesEntry(k.toString(), new AttributeValueUpdate(new AttributeValue(op.value.toString()), AttributeAction.PUT));
                    break;
                case MAX:
                    request.addAttributeUpdatesEntry(k.toString(), new AttributeValueUpdate(new AttributeValue(op.value.toString()), AttributeAction.PUT));
                    request.addExpectedEntry(k.toString(), new ExpectedAttributeValue(new AttributeValue(op.value.toString())).withComparisonOperator(ComparisonOperator.LE));
                    break;
                case INCREMENT:
                    request.addAttributeUpdatesEntry(k.toString(), new AttributeValueUpdate(new AttributeValue(op.value.toString()), AttributeAction.ADD));
                    break;
                case REMOVE_MAP_ENTRY:
                    request.addAttributeUpdatesEntry(k.toString(), new AttributeValueUpdate(new AttributeValue(op.value.toString()), AttributeAction.DELETE));
                    break;
                case CONTAINS_MAP_ENTRY:
                    if (checkConditions) {
                        if (Boolean.TRUE.equals(op.value)) {
                            request.addExpectedEntry(k.toString(), new ExpectedAttributeValue(true));
                        } else {
                            request.addExpectedEntry(k.toString(), new ExpectedAttributeValue(false));
                        }
                    }
                    break;
            }
        }
        // If creation is not allowed, we add a condition for "should exist"
        if (!upsert) {
            request.addExpectedEntry(Document.ID, new ExpectedAttributeValue(true));
        }
        return request;
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> list) {
        BatchWriteItemRequest request = new BatchWriteItemRequest();
        ArrayList<WriteRequest> writes = new ArrayList<WriteRequest>();
        request.addRequestItemsEntry(collectionToTable(collection), writes);
        for (UpdateOp update : list) {
            PutRequest put = create(collection, update);
            writes.add(new WriteRequest(put));
            if (writes.size() == 25) {
                try {
                    dynamoDB.batchWriteItem(request);
                    writes.clear();
                } catch (Throwable t) {
                    return false;
                }
            }
        }
        if (writes.size() > 0) {
            try {
                dynamoDB.batchWriteItem(request);
                writes.clear();
            } catch (Throwable t) {
                return false;
            }
        }
        return true;
    }

    private <T extends Document> PutRequest create(Collection<T> collection, UpdateOp update) {
        PutRequest request = new PutRequest();
        boolean seenModCount = false;
        for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> entry : update.getChanges().entrySet()) {
            UpdateOp.Key k = entry.getKey();
            UpdateOp.Operation op = entry.getValue();
            seenModCount = seenModCount || k.getName().equals(Document.MOD_COUNT);
            switch (op.type) {
                case SET:
                case MAX:
                case INCREMENT: {
                    request.addItemEntry(k.toString(), new AttributeValue(op.value.toString()));
                    break;
                }
                case SET_MAP_ENTRY: {
                    Revision r = k.getRevision();
                    if (r == null) {
                        throw new IllegalStateException(
                                "SET_MAP_ENTRY must not have null revision");
                    }
                    request.addItemEntry(k.getName(), new AttributeValue()
                            .withM(Collections.singletonMap(r.toString(), new AttributeValue(op.value.toString()))));
                    break;
                }
                case REMOVE_MAP_ENTRY:
                    // nothing to do for new entries
                    break;
                case CONTAINS_MAP_ENTRY:
                    // no effect
                    break;
            }
        }
        if (!seenModCount) {
            request.addItemEntry(Document.MOD_COUNT, new AttributeValue("1"));
        }
        return request;
    }

    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        UpdateItemRequest request = getUpdateItemRequest(collection, updateOp, false, true);
        try {
            for (String key : keys) {
                dynamoDB.updateItem(request.withKey(Collections.singletonMap(Document.ID, new AttributeValue(key))));
            }
        } catch (Throwable t) {
            throw new MicroKernelException(t);
        }
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp updateOp) {
        return findAndModify(collection, updateOp, true, false);
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp updateOp) {
        return findAndModify(collection, updateOp, false, true);
    }

    @Override
    public void invalidateCache() {
        // No caching
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String s) {
        // No caching - do nothing
    }

    @Override
    public void dispose() {
        dynamoDB.shutdown();
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String s) {
        return null;
    }

    @Override
    public void setReadWriteMode(String s) {

    }

    @Override
    public CacheStats getCacheStats() {
        return null;
    }
}
