package org.apache.jackrabbit.oak.plugins.document.dynamodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.plugins.document.*;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.print.Doc;
import java.util.*;

/**
 * User: Denis Mikhalkin
 * Date: 13/11/2014
 * Time: 11:06 PM
 * TODO: Implement initialization service
 * TODO: Add _modified index
 * TODO: Handle store.query(Collection.NODES, NodeDocument.MIN_ID_VALUE, NodeDocument.MAX_ID_VALUE, NodeDocument.MODIFIED_IN_SECS,  NodeDocument.getModifiedInSecs(startTime), Integer.MAX_VALUE);
 *       - this means handling queries across levels (currently I only support queries within one level)
 */
public class DynamoDBDocumentStore implements DocumentStore {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBDocumentStore.class);
    private static final String MODIFIED = "_modified";

    private final Comparator<Revision> comparator = StableRevisionComparator.REVERSE;

    public static final String PATH = "dynamodb:Path";
    public static final String NAME = "dynamodb:Name";

    AmazonDynamoDBClient dynamoDB;

    public DynamoDBDocumentStore() {
        dynamoDB = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());
        dynamoDB.setRegion(Region.getRegion(Regions.AP_SOUTHEAST_2));

        init();
    }

    private void init() {
        ensureTableExists(collectionToTable(Collection.NODES)/*,
                new GlobalSecondaryIndex()
                        .withIndexName(MODIFIED)
                        .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY))
                        .withKeySchema(new KeySchemaElement(MODIFIED, KeyType.HASH))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))*/);
        ensureTableExists(collectionToTable(Collection.CLUSTER_NODES));
        ensureTableExists(collectionToTable(Collection.SETTINGS));
    }

    public DynamoDBDocumentStore(AmazonDynamoDBClient client) {
        dynamoDB = client;
        init();
    }

    private void ensureTableExists(String tableName, GlobalSecondaryIndex... indexes) {
        try {
            dynamoDB.describeTable(tableName);
        } catch (ResourceNotFoundException rnfe) {
            // Table does not exist
            CreateTableRequest createTableRequest = new CreateTableRequest(tableName, Arrays.asList(new KeySchemaElement(PATH, KeyType.HASH), new KeySchemaElement(NAME, KeyType.RANGE)))
                    .withAttributeDefinitions(new AttributeDefinition(PATH, ScalarAttributeType.S), new AttributeDefinition(NAME, ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
/*
            if ("nodes".equals(tableName)) {
                // TODO Cleanup hard-coded attribute
                createTableRequest.getAttributeDefinitions().add(new AttributeDefinition(MODIFIED, ScalarAttributeType.N));
            }
*/
            if (indexes != null && indexes.length > 0) {
                createTableRequest.setGlobalSecondaryIndexes(Arrays.asList(indexes));
            }
            dynamoDB.createTable(createTableRequest);
        } catch (AmazonClientException e) {
            LOG.error("Exception checking Nodes table", e);
        }
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        log("find", collectionToTable(collection), key);
        GetItemResult res = dynamoDB.getItem(new GetItemRequest(collectionToTable(collection), keyMap(key)));
        if (res != null) {
            T t = itemToDocument(collection, res.getItem());
            log("find ->", t);
            return t;
        }
        log("find ->", "{}");
        return null;
    }

    private Map<String, AttributeValue> keyMap(String key) {
        int index = key.lastIndexOf('/');
        if (index == -1 || key.length() == 1) {
            HashMap<String, AttributeValue> res = new HashMap<String, AttributeValue>();
            res.put(PATH, new AttributeValue("0:"));
            res.put(NAME, new AttributeValue(key));
            return res;
        } else {
            if (index == key.length()-1) {
                index = key.lastIndexOf('/', index-1);
            }
            if (index == -1) { // only 1 slash
                HashMap<String, AttributeValue> res = new HashMap<String, AttributeValue>();
                res.put(PATH, new AttributeValue("0:"));
                res.put(NAME, new AttributeValue(key));
                return res;
            }
            HashMap<String, AttributeValue> res = new HashMap<String, AttributeValue>();
            res.put(PATH, new AttributeValue(key.substring(0, index)));
            res.put(NAME, new AttributeValue(key.substring(index+1)));
            return res;
        }
    }

    private AttributeValue getHashKey(String key) {
        int index = key.lastIndexOf('/');
        if (index == -1 || key.length() == 1) {
            return new AttributeValue("0:");
        } else {
            if (index == key.length()-1) {
                index = key.lastIndexOf('/', index-1);
            }
            if (index == -1) return new AttributeValue("0:");
            return new AttributeValue(key.substring(0, index));
        }
    }

    private AttributeValue getRangeKey(String key) {
        int index = key.lastIndexOf('/');
        if (index == -1 || key.length() == 1) {
            return new AttributeValue(key);
        } else {
            if (index == key.length()-1) {
                index = key.lastIndexOf('/', index-1);
            }
            if (index == -1) return new AttributeValue(key);
            return new AttributeValue(key.substring(index+1));
        }
    }

    private <T extends Document> T itemToDocument(Collection<T> collection, Map<String, AttributeValue> item) {
        if (item == null) return null;

        T doc = collection.newDocument(this);
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            String key = entry.getKey();
            if (PATH.equals(key) || NAME.equals(key)) continue;
            if (key.contains(".")) {
                String name = key.substring(0, key.indexOf('.'));
                String revision = key.substring(key.indexOf('.')+1);
                Map<Revision, Object> map = (Map<Revision, Object>) doc.get(name);
                if (map == null) {
                    map = new TreeMap<Revision, Object>(comparator);
                    doc.put(name, map);
                }
                map.put(Revision.fromString(revision), attrToObject(entry.getValue()));
            } else {
                doc.put(key, attrToObject(entry.getValue()));
            }
        }
        return doc;
    }

    private Object attrToObject(AttributeValue value) {
        if (value == null) {
            return null;
        }
        if (value.getBOOL() != null) {
            return value.getBOOL();
        } else if (value.getN() != null) {
            return Long.valueOf(value.getN());
        } else if (value.getM() != null) {
            return docMap(value.getM());
        } else return value.getS();
    }

    private Map<Revision, Object> docMap(Map<String, AttributeValue> m) {
        Map<Revision, Object> map = new TreeMap<Revision, Object>(comparator);
        for (Map.Entry<String, AttributeValue> entry : m.entrySet()) {
            map.put(Revision.fromString(entry.getKey()), attrToObject(entry.getValue()));
        }
        return map;
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
    public <T extends Document> List<T> query(Collection<T> collection,
                                              String fromKey,
                                              String toKey,
                                              String indexedProperty,
                                              long startValue,
                                              int limit) {
        log("query", collectionToTable(collection), fromKey, toKey, indexedProperty, startValue, limit);
        // fromKey, toKey - assumes "key" is ordered sequence
        // indexedProperty - assumes ordered property, startValue refers to its value and above. Can be MODIFIED_IN_SECS, HAS_BINARY_FLAG
        try {
            QueryRequest request = new QueryRequest(collectionToTable(collection));
            request.addKeyConditionsEntry(PATH, new Condition().withAttributeValueList(getHashKey(fromKey)).withComparisonOperator(ComparisonOperator.EQ));
            if (!getHashKey(fromKey).equals(getHashKey(toKey))) {
                request.addKeyConditionsEntry(NAME, new Condition().withAttributeValueList(getRangeKey(fromKey)).withComparisonOperator(ComparisonOperator.GE));
            } else {
                request.addKeyConditionsEntry(NAME, new Condition().withAttributeValueList(getRangeKey(fromKey), getRangeKey(toKey)).withComparisonOperator(ComparisonOperator.BETWEEN));
            }
            if (indexedProperty != null) {
                if (fromKey.equals(NodeDocument.MIN_ID_VALUE) && toKey.equals(NodeDocument.MAX_ID_VALUE)) {
                    // Global index
                    if (MODIFIED.equals(indexedProperty)) {
                        return Collections.emptyList();
                        // TODO For now, we don't support modified queries as it is a corner case for unclean shutdown
//                        request.getKeyConditions().clear();
//                        request.addKeyConditionsEntry(indexedProperty, new Condition().withAttributeValueList(new AttributeValue(String.valueOf(startValue))).withComparisonOperator(ComparisonOperator.GE));
//                        request.setIndexName(MODIFIED);
                    } else {
                        throw new MicroKernelException("Unsupported index: fromKey=" + fromKey + ", toKey=" + toKey + ", indexedProperty: " + indexedProperty);
                    }
                } else {
                    throw new MicroKernelException("Unsupported index: fromKey=" + fromKey + ", toKey=" + toKey + ", indexedProperty: " + indexedProperty);
                    // Assumes this kind of query will only ever run using a local index
    //                request.addKeyConditionsEntry(indexedProperty, new Condition().withAttributeValueList(new AttributeValue(String.valueOf(startValue))).withComparisonOperator(ComparisonOperator.GE));
    //                request.setIndexName(indexedProperty+"Index");
                }
            }
            request.setSelect(Select.ALL_ATTRIBUTES);
            request.setLimit(limit);
            QueryResult result = dynamoDB.query(request);
            if (result != null) {
                ArrayList<T> list = new ArrayList<T>();
                for (Map<String, AttributeValue> item : result.getItems()) {
                    list.add((T)itemToDocument(collection, item));
                }
                log("query -> ", list);
                return list;
            }
        } catch (RuntimeException e) {
            LOG.error(String.format("query(%s, %s, %s, %s)", collectionToTable(collection), fromKey, toKey, indexedProperty), e);
            throw e;
        }
        log("query -> ", "{}");
        return Collections.emptyList();
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        log("remove", collectionToTable(collection), key);
        dynamoDB.deleteItem(new DeleteItemRequest(collectionToTable(collection), keyMap(key)));
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        log("remove", collectionToTable(collection), keys);
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
            if (result == null || result.getAttributes() == null || result.getAttributes().size() == 0) {
                return null;
            }
            return itemToDocument(collection, result.getAttributes());
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
                .addKeyEntry(PATH, new AttributeValue(updateOp.getId()));
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
                .addKeyEntry(PATH, getHashKey(updateOp.getId()))
                .addKeyEntry(NAME, getRangeKey(updateOp.getId()))
                .withReturnValues(ReturnValue.ALL_OLD)
                /*.addAttributeUpdatesEntry(Document.MOD_COUNT, new AttributeValueUpdate(new AttributeValue().withN("1"), AttributeAction.ADD))*/;

        StringBuilder sets = new StringBuilder("SET ");
        StringBuilder deletes = new StringBuilder("DELETE ");
        StringBuilder adds = new StringBuilder("ADD #att0 :val0, ");
        StringBuilder removes = new StringBuilder("REMOVE ");

        ArrayList<String> names = new ArrayList<String>();
        names.add(Document.MOD_COUNT);
        ArrayList<AttributeValue> values = new ArrayList<AttributeValue>();
        values.add(new AttributeValue().withN("1"));
        StringBuilder expected = new StringBuilder();
        for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> entry : updateOp.getChanges().entrySet()) {
            UpdateOp.Key k = entry.getKey();
            if (k.getName().equals(Document.ID) || k.getName().equals(Document.MOD_COUNT)) {
                // avoid exception "Mod on _id not allowed"
                continue;
            }
            UpdateOp.Operation op = entry.getValue();
            switch (op.type) {
                case SET_MAP_ENTRY: {
                    Revision r = k.getRevision();
                    if (r == null) {
                        throw new IllegalStateException(
                                "SET_MAP_ENTRY must not have null revision");
                    }
                }
                // Fallthrough
                case SET:
                    if (op.value == null) {
                        // null in DynamoDB means DELETE
//                        request.addAttributeUpdatesEntry(k.toString(), new AttributeValueUpdate(null, AttributeAction.DELETE));
                        removes.append("#att").append(names.size()).append(", ");
                        names.add(k.toString());
                    } else {
                        sets.append("#att").append(names.size()).append(" = ").append(":val").append(values.size()).append(", ");
                        values.add(attributeValueForObject(op.value));
                        names.add(k.toString());
//                        request.addAttributeUpdatesEntry(k.toString(), new AttributeValueUpdate(attributeValueForObject(op.value), AttributeAction.PUT));
                    }
                    break;
/*
                case SET_MAP_ENTRY: {
                    Revision r = k.getRevision();
                    if (r == null) {
                        throw new IllegalStateException(
                                "SET_MAP_ENTRY must not have null revision");
                    }
                    if (op.value == null) {
                        // null in DynamoDB means DELETE
//                        request.addAttributeUpdatesEntry(k.getName(), new AttributeValueUpdate(new AttributeValue()
//                                .withM(Collections.singletonMap(r.toString(), (AttributeValue) null)), AttributeAction.DELETE));
                        removes.append("#att").append(names.size()).append(".").append("#att").append(names.size()+1).append(", ");
                        names.add(k.getName());
                        names.add(r.toString());
                    } else {
//                        request.addAttributeUpdatesEntry(k.getName(), new AttributeValueUpdate(new AttributeValue()
//                                .withM(Collections.singletonMap(r.toString(), attributeValueForObject(op.value))), AttributeAction.PUT));
                        sets.append("#att").append(names.size()).append(".").append("#att").append(names.size()+1).append(" = ").append(":val").append(values.size()).append(", ");
                        names.add(k.getName());
                        names.add(r.toString());
                        values.add(attributeValueForObject(op.value));
//                        names.add(k.toString());
                    }
                    break;
                }
*/
                case MAX:
//                    request.addAttributeUpdatesEntry(k.toString(), new AttributeValueUpdate(new AttributeValue().withN(op.value.toString()), AttributeAction.PUT));
                    sets.append("#att").append(names.size()).append(" = ").append(":val").append(values.size()).append(", ");
                    values.add(attributeValueForObject(op.value));

//                    request.addExpectedEntry(k.toString(), new ExpectedAttributeValue(new AttributeValue().withN(op.value.toString())).withComparisonOperator(ComparisonOperator.LE));
                    expected.append(" and ").append("#att").append(names.size()).append(" <= ").append(":val").append(values.size());
                    names.add(k.toString());
                    values.add(new AttributeValue().withN(op.value.toString()));
                    break;
                case INCREMENT:
//                    request.addAttributeUpdatesEntry(k.toString(), new AttributeValueUpdate(new AttributeValue().withN(op.value.toString()), AttributeAction.ADD));
                    adds.append("#att").append(names.size()).append(' ').append(":val").append(values.size()).append(", ");
                    names.add(k.toString());
                    values.add(new AttributeValue().withN(op.value.toString()));
                    break;
                case REMOVE_MAP_ENTRY: {
                    Revision r = k.getRevision();
                    if (r == null) {
                        throw new IllegalStateException(
                                "SET_MAP_ENTRY must not have null revision");
                    }
//                    request.addAttributeUpdatesEntry(k.getName(), new AttributeValueUpdate(new AttributeValue()
//                            .withM(Collections.singletonMap(r.toString(), (AttributeValue) null)), AttributeAction.DELETE));
                    removes.append("#att").append(names.size())./*append(".").append("#att").append(names.size()+1).*/append(", ");
//                    names.add(k.getName());
//                    names.add(r.toString());
                    names.add(k.toString());
                    break;
                }
                case CONTAINS_MAP_ENTRY: {
                    if (checkConditions) {
                        Revision r = k.getRevision();
                        if (r == null) {
                            throw new IllegalStateException(
                                    "SET_MAP_ENTRY must not have null revision");
                        }
                        expected.append(" and ").append(Boolean.TRUE.equals(op.value) ? "attribute_exists(" : "attribute_not_exists(").
                                append("#att").append(names.size())./*append(".").append("#att").append(names.size()+1).*/append(")");
//                        names.add(k.getName());
//                        names.add(r.toString());
                        names.add(k.toString());
//                        if (Boolean.TRUE.equals(op.value)) {
//                            request.addExpectedEntry(k.toString(), new ExpectedAttributeValue(true));
//                        } else {
//                            request.addExpectedEntry(k.toString(), new ExpectedAttributeValue(false));
//                        }
                    }
                    break;
                }
            }
        }
        StringBuilder expression = new StringBuilder();
        if (deletes.length() > "delete ".length()) {
            deletes.setLength(deletes.length()-2);
            expression.append(deletes).append(' ');
        }
        if (sets.length() > "set ".length()) {
            sets.setLength(sets.length() - 2);
            expression.append(sets).append(' ');
        }
        if (adds.length() > "add ".length()) {
            adds.setLength(adds.length()-2);
            expression.append(adds).append(' ');
        }
        if (removes.length() > "remove ".length()) {
            removes.setLength(removes.length()-2);
            expression.append(removes);
        }
        if (expression.length() > 0) request.setUpdateExpression(expression.toString());

        // If creation is not allowed, we add a condition for "should exist"
        if (!upsert) {
            expected.append(" and ").append("attribute_exists(").append("#att").append(names.size()).append(")");
            names.add(PATH);
//            request.addExpectedEntry(PATH, new ExpectedAttributeValue(getHashKey(updateOp.getId())));
        }

        Map<String, String> convertedNames = new HashMap<String, String>();
        for (String name : names) {
            convertedNames.put("#att"+convertedNames.size(), name);
        }
        request.setExpressionAttributeNames(convertedNames);
        Map<String, AttributeValue> convertedValues = new HashMap<String, AttributeValue>();
        for (AttributeValue value : values) {
            convertedValues.put(":val" + convertedValues.size(), value);
        }
        request.setExpressionAttributeValues(convertedValues);

        if (expected.length() > 0) {
            expected.delete(0, 5);
            request.setConditionExpression(expected.toString());
        }
        return request;
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> list) {
        log("create", collectionToTable(collection), list);
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
                case SET_MAP_ENTRY:
                    Revision r = k.getRevision();
                    if (r == null) {
                        throw new IllegalStateException(
                                "SET_MAP_ENTRY must not have null revision");
                    }
                    // Fallthrough
                case SET:
                case MAX:
                case INCREMENT: {
                    request.addItemEntry(k.toString(), attributeValueForObject(op.value));
                    break;
                }
/*
                case SET_MAP_ENTRY: {
                    Revision r = k.getRevision();
                    if (r == null) {
                        throw new IllegalStateException(
                                "SET_MAP_ENTRY must not have null revision");
                    }
                    request.addItemEntry(k.getName(), new AttributeValue()
                            .withM(Collections.singletonMap(r.toString(), attributeValueForObject(op.value))));
                    break;
                }
*/
                case REMOVE_MAP_ENTRY:
                    // nothing to do for new entries
                    break;
                case CONTAINS_MAP_ENTRY:
                    // no effect
                    break;
            }
        }
        if (!seenModCount) {
            request.addItemEntry(Document.MOD_COUNT, attributeValueForObject(1));
        }
        request.addItemEntry(PATH, getHashKey(update.getId()));
        request.addItemEntry(NAME, getRangeKey(update.getId()));

/*
        if (collection == Collection.NODES) {
            addEmptyMap(request, "_lastRev", "_commitRoot", "_deleted", "_revisions", "_collisions");
        }
*/

        return request;
    }

    private void addEmptyMap(PutRequest request, String ... names) {
        for (String name: names) {
            if (!request.getItem().containsKey(name)) request.addItemEntry(name, new AttributeValue().withM(Collections.<String, AttributeValue>emptyMap()));
        }
    }


    private AttributeValue attributeValueForObject(Object value) {
        if (value instanceof String) {
            return new AttributeValue((String)value);
        } else if (value instanceof Number) {
            return new AttributeValue().withN(String.valueOf(value));
        } else if (value instanceof Boolean) {
            return new AttributeValue().withBOOL((Boolean)value);
        } else throw new MicroKernelException("Unsupported attribute value " + value + "(" + (value != null ? value.getClass() : "") + ")");
    }

    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        log("update", collectionToTable(collection), keys, updateOp);
        UpdateItemRequest request = getUpdateItemRequest(collection, updateOp, false, true);
        try {
            for (String key : keys) {
                dynamoDB.updateItem(request.withKey(keyMap(key)));
            }
        } catch (Throwable t) {
            throw new MicroKernelException(t);
        }
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp updateOp) {
        log("createOrUpdate", collectionToTable(collection), updateOp);
        return findAndModify(collection, updateOp, true, false);
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp updateOp) {
        log("findAndUpdate", collectionToTable(collection), updateOp);
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
    private static void log(String message, Object... args) {
        if (LOG.isInfoEnabled()) {
            String argList = Arrays.toString(args);
            if (argList.length() > 10000) {
                argList = argList.length() + ": " + argList;
            }
            LOG.info(message + argList);
            System.err.println(message + argList);
        }
    }
}
