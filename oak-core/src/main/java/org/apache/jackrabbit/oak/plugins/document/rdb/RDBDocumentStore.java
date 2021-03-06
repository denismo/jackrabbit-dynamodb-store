/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.rdb;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.UpdateUtils;
import org.apache.jackrabbit.oak.plugins.document.cache.CachingDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.Striped;

public class RDBDocumentStore implements CachingDocumentStore {

    /**
     * Creates a {@linkplain RDBDocumentStore} instance using the provided
     * {@link DataSource}.
     */
    public RDBDocumentStore(DataSource ds, DocumentMK.Builder builder) {
        try {
            initialize(ds, builder);
        } catch (Exception ex) {
            throw new MicroKernelException("initializing RDB document store", ex);
        }
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String id) {
        return find(collection, id, Integer.MAX_VALUE);
    }

    @Override
    public <T extends Document> T find(final Collection<T> collection, final String id, int maxCacheAge) {
        if (collection != Collection.NODES) {
            return readDocument(collection, id);
        } else {
            CacheValue cacheKey = new StringValue(id);
            NodeDocument doc;
            if (maxCacheAge > 0) {
                // first try without lock
                doc = nodesCache.getIfPresent(cacheKey);
                if (doc != null) {
                    if (maxCacheAge == Integer.MAX_VALUE || System.currentTimeMillis() - doc.getCreated() < maxCacheAge) {
                        return castAsT(unwrap(doc));
                    }
                }
            }
            try {
                Lock lock = getAndLock(id);
                try {
                    if (maxCacheAge == 0) {
                        invalidateCache(collection, id);
                    }
                    while (true) {
                        doc = nodesCache.get(cacheKey, new Callable<NodeDocument>() {
                            @Override
                            public NodeDocument call() throws Exception {
                                NodeDocument doc = (NodeDocument) readDocument(collection, id);
                                if (doc != null) {
                                    doc.seal();
                                }
                                return wrap(doc);
                            }
                        });
                        if (maxCacheAge == 0 || maxCacheAge == Integer.MAX_VALUE) {
                            break;
                        }
                        if (System.currentTimeMillis() - doc.getCreated() < maxCacheAge) {
                            break;
                        }
                        // too old: invalidate, try again
                        invalidateCache(collection, id);
                    }
                } finally {
                    lock.unlock();
                }
                return castAsT(unwrap(doc));
            } catch (ExecutionException e) {
                throw new IllegalStateException("Failed to load document with " + id, e);
            }
        }
    }

    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
        // TODO cache
        return query(collection, fromKey, toKey, null, 0, limit);
    }

    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, String indexedProperty,
            long startValue, int limit) {
        // TODO cache
        return internalQuery(collection, fromKey, toKey, indexedProperty, startValue, limit);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String id) {
        delete(collection, id);
        invalidateCache(collection, id);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        // TODO Use batch delete
        for (String key : keys) {
            remove(collection, key);
        }
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        return internalCreate(collection, updateOps);
    }

    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        internalUpdate(collection, keys, updateOp);
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) throws MicroKernelException {
        return internalCreateOrUpdate(collection, update, true, false);
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) throws MicroKernelException {
        return internalCreateOrUpdate(collection, update, false, true);
    }

    @Override
    public void invalidateCache() {
        nodesCache.invalidateAll();
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String id) {
        if (collection == Collection.NODES) {
            Lock lock = getAndLock(id);
            try {
                nodesCache.invalidate(new StringValue(id));
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void dispose() {
        this.ds = null;
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String id) {
        if (collection != Collection.NODES) {
            return null;
        } else {
            NodeDocument doc = nodesCache.getIfPresent(new StringValue(id));
            return castAsT(doc);
        }
    }

    @Override
    public CacheStats getCacheStats() {
        return this.cacheStats;
    }

    // implementation

    private final String MODIFIED = "_modified";
    private final String MODCOUNT = "_modCount";

    private static final Logger LOG = LoggerFactory.getLogger(RDBDocumentStore.class);

    private final Comparator<Revision> comparator = StableRevisionComparator.REVERSE;

    private Exception callStack;

    private DataSource ds;

    // string length at which we switch to BLOB storage
    private static int DATALIMIT = 16384 / 4;

    // number of retries for updates
    private static int RETRIES = 10;

    private void initialize(DataSource ds, DocumentMK.Builder builder) throws Exception {

        this.ds = ds;
        this.callStack = LOG.isDebugEnabled() ? new Exception("call stack of RDBDocumentStore creation") : null;

        this.nodesCache = builder.buildCache(builder.getDocumentCacheSize());
        this.cacheStats = new CacheStats(nodesCache, "Document-Documents", builder.getWeigher(), builder.getDocumentCacheSize());

        Connection con = ds.getConnection();

        try {
            con.setAutoCommit(false);

            for (String tableName : new String[] { "CLUSTERNODES", "NODES", "SETTINGS" }) {
                try {
                    PreparedStatement stmt = con.prepareStatement("select ID from " + tableName + " where ID = ?");
                    stmt.setString(1, "0:/");
                    stmt.executeQuery();
                } catch (SQLException ex) {
                    // table does not appear to exist
                    con.rollback();

                    String dbtype = con.getMetaData().getDatabaseProductName();
                    LOG.info("Attempting to create table " + tableName + " in " + dbtype);

                    Statement stmt = con.createStatement();

                    // the code below likely will need to be extended for new
                    // database types
                    if ("PostgreSQL".equals(dbtype)) {
                        stmt.execute("create table "
                                + tableName
                                + " (ID varchar(1000) not null primary key, MODIFIED bigint, MODCOUNT bigint, SIZE bigint, DATA varchar(16384), BDATA bytea)");
                    } else if ("DB2".equals(dbtype) || (dbtype != null && dbtype.startsWith("DB2/"))) {
                        stmt.execute("create table "
                                + tableName
                                + " (ID varchar(1000) not null primary key, MODIFIED bigint, MODCOUNT bigint, SIZE bigint, DATA varchar(16384), BDATA blob)");
                    } else {
                        stmt.execute("create table "
                                + tableName
                                + " (ID varchar(1000) not null primary key, MODIFIED bigint, MODCOUNT bigint, SIZE bigint, DATA varchar(16384), BDATA blob)");
                    }
                    stmt.close();

                    con.commit();
                }
            }
        } finally {
            con.close();
        }
    }

    @Override
    public void finalize() {
        if (this.ds != null && this.callStack != null) {
            LOG.debug("finalizing RDBDocumentStore that was not disposed", this.callStack);
        }
    }

    @CheckForNull
    private <T extends Document> boolean internalCreate(Collection<T> collection, List<UpdateOp> updates) {
        try {
            for (UpdateOp update : updates) {
                T doc = collection.newDocument(this);
                update.increment(MODCOUNT, 1);
                UpdateUtils.applyChanges(doc, update, comparator);
                insertDocument(collection, doc);
                addToCache(collection, doc);
            }
            return true;
        } catch (MicroKernelException ex) {
            return false;
        }
    }

    @CheckForNull
    private <T extends Document> T internalCreateOrUpdate(Collection<T> collection, UpdateOp update, boolean allowCreate,
            boolean checkConditions) {
        T oldDoc = readDocument(collection, update.getId());

        if (oldDoc == null) {
            if (!allowCreate) {
                return null;
            } else if (!update.isNew()) {
                throw new MicroKernelException("Document does not exist: " + update.getId());
            }
            T doc = collection.newDocument(this);
            if (checkConditions && !UpdateUtils.checkConditions(doc, update)) {
                return null;
            }
            update.increment(MODCOUNT, 1);
            UpdateUtils.applyChanges(doc, update, comparator);
            try {
                insertDocument(collection, doc);
                addToCache(collection, doc);
                return oldDoc;
            }
            catch (MicroKernelException ex) {
                // may have failed due to a race condition; try update instead
                oldDoc = readDocument(collection, update.getId());
                if (oldDoc == null) {
                    // something else went wrong
                    throw(ex);
                }
                return internalUpdate(collection, update, oldDoc, checkConditions, RETRIES);
            }
        } else {
            return internalUpdate(collection, update, oldDoc, checkConditions, RETRIES);
        }
    }

    @CheckForNull
    private <T extends Document> T internalUpdate(Collection<T> collection, UpdateOp update, T oldDoc, boolean checkConditions,
            int retries) {
        T doc = applyChanges(collection, oldDoc, update, checkConditions);
        if (doc == null) {
            return null;
        } else {
            boolean success = false;

            while (!success && retries > 0) {
                success = updateDocument(collection, doc, (Long) oldDoc.get(MODCOUNT));
                if (!success) {
                    // retry with a fresh document
                    retries -= 1;
                    oldDoc = readDocument(collection, update.getId());
                    doc = applyChanges(collection, oldDoc, update, checkConditions);
                    if (doc == null) {
                        return null;
                    }
                } else {
                    applyToCache(collection, oldDoc, doc);
                }
            }

            if (!success) {
                throw new MicroKernelException("failed update (race?)");
            }

            return oldDoc;
        }
    }

    @CheckForNull
    private <T extends Document> T applyChanges(Collection<T> collection, T oldDoc, UpdateOp update, boolean checkConditions) {
        T doc = collection.newDocument(this);
        oldDoc.deepCopy(doc);
        if (checkConditions && !UpdateUtils.checkConditions(doc, update)) {
            return null;
        }
        update.increment(MODCOUNT, 1);
        UpdateUtils.applyChanges(doc, update, comparator);
        doc.seal();
        return doc;
    }

    @CheckForNull
    private <T extends Document> void internalUpdate(Collection<T> collection, List<String> ids, UpdateOp update) {
        Connection connection = null;
        String tableName = getTable(collection);
        try {
            connection = getConnection();
            for (String id : ids) {
                String in = dbRead(connection, tableName, id);
                if (in == null) {
                    throw new MicroKernelException(tableName + " " + id + " not found");
                }
                T doc = fromString(collection, in);
                Long oldmodcount = (Long) doc.get(MODCOUNT);
                update.increment(MODCOUNT, 1);
                UpdateUtils.applyChanges(doc, update, comparator);
                String data = asString(doc);
                Long modified = (Long) doc.get(MODIFIED);
                Long modcount = (Long) doc.get(MODCOUNT);
                dbUpdate(connection, tableName, id, modified, modcount, oldmodcount, data);
                invalidateCache(collection, id); // TODO
            }
            connection.commit();
        } catch (Exception ex) {
            throw new MicroKernelException(ex);
        } finally {
            closeConnection(connection);
        }
    }

    private <T extends Document> List<T> internalQuery(Collection<T> collection, String fromKey, String toKey,
            String indexedProperty, long startValue, int limit) {
        Connection connection = null;
        String tableName = getTable(collection);
        List<T> result = new ArrayList<T>();
        if (indexedProperty != null && !MODIFIED.equals(indexedProperty)) {
            throw new MicroKernelException("indexed property " + indexedProperty + " not supported");
        }
        try {
            connection = getConnection();
            List<String> dbresult = dbQuery(connection, tableName, fromKey, toKey, indexedProperty, startValue, limit);
            for (String data : dbresult) {
                T doc = fromString(collection, data);
                doc.seal();
                result.add(doc);
                addToCacheIfNotNewer(collection, doc);
            }
        } catch (Exception ex) {
            throw new MicroKernelException(ex);
        } finally {
            closeConnection(connection);
        }
        return result;
    }

    private static <T extends Document> String getTable(Collection<T> collection) {
        if (collection == Collection.CLUSTER_NODES) {
            return "CLUSTERNODES";
        } else if (collection == Collection.NODES) {
            return "NODES";
        } else if (collection == Collection.SETTINGS) {
            return "SETTINGS";
        } else {
            throw new IllegalArgumentException("Unknown collection: " + collection.toString());
        }
    }

    private static String asString(@Nonnull Document doc) {
        JSONObject obj = new JSONObject();
        for (String key : doc.keySet()) {
            Object value = doc.get(key);
            obj.put(key, value);
        }
        return obj.toJSONString();
    }

    private <T extends Document> T fromString(Collection<T> collection, String data) throws ParseException {
        T doc = collection.newDocument(this);
        Map<String, Object> obj = (Map<String, Object>) new JSONParser().parse(data);
        for (Map.Entry<String, Object> entry : obj.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value == null) {
                // ???
                doc.put(key, value);
            } else if (value instanceof Boolean || value instanceof Long || value instanceof String) {
                doc.put(key, value);
            } else if (value instanceof JSONObject) {
                doc.put(key, convertJsonObject((JSONObject) value));
            } else {
                throw new RuntimeException("unexpected type: " + value.getClass());
            }
        }
        return doc;
    }

    @Nonnull
    private Map<Revision, Object> convertJsonObject(@Nonnull JSONObject obj) {
        Map<Revision, Object> map = new TreeMap<Revision, Object>(comparator);
        Set<Map.Entry> entries = obj.entrySet();
        for (Map.Entry entry : entries) {
            // not clear why every persisted map is a revision map
            map.put(Revision.fromString(entry.getKey().toString()), entry.getValue());
        }
        return map;
    }

    @CheckForNull
    private <T extends Document> T readDocument(Collection<T> collection, String id) {
        Connection connection = null;
        String tableName = getTable(collection);
        try {
            connection = getConnection();
            String in = dbRead(connection, tableName, id);
            return in != null ? fromString(collection, in) : null;
        } catch (Exception ex) {
            throw new MicroKernelException(ex);
        } finally {
            closeConnection(connection);
        }
    }

    private <T extends Document> void delete(Collection<T> collection, String id) {
        Connection connection = null;
        String tableName = getTable(collection);
        try {
            connection = getConnection();
            dbDelete(connection, tableName, id);
            connection.commit();
        } catch (Exception ex) {
            throw new MicroKernelException(ex);
        } finally {
            closeConnection(connection);
        }
    }

    private <T extends Document> boolean updateDocument(@Nonnull Collection<T> collection, @Nonnull T document, Long oldmodcount) {
        Connection connection = null;
        String tableName = getTable(collection);
        try {
            connection = getConnection();
            String data = asString(document);
            Long modified = (Long) document.get(MODIFIED);
            Long modcount = (Long) document.get(MODCOUNT);
            boolean success = dbUpdate(connection, tableName, document.getId(), modified, modcount, oldmodcount, data);
            connection.commit();
            return success;
        } catch (SQLException ex) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                // TODO
            }
            throw new MicroKernelException(ex);
        } finally {
            closeConnection(connection);
        }
    }

    private <T extends Document> void insertDocument(Collection<T> collection, T document) {
        Connection connection = null;
        String tableName = getTable(collection);
        try {
            connection = getConnection();
            String data = asString(document);
            Long modified = (Long) document.get(MODIFIED);
            Long modcount = (Long) document.get(MODCOUNT);
            dbInsert(connection, tableName, document.getId(), modified, modcount, data);
            connection.commit();
        } catch (SQLException ex) {
            LOG.debug("insert of " + document.getId() + " failed", ex);
            try {
                connection.rollback();
            } catch (SQLException e) {
                // TODO
            }
            throw new MicroKernelException(ex);
        } finally {
            closeConnection(connection);
        }
    }

    // low level operations

    private String getData(ResultSet rs, int stringIndex, int blobIndex) throws SQLException {
        try {
            String data = rs.getString(stringIndex);
            byte[] bdata = rs.getBytes(blobIndex);
            if (bdata == null) {
                return data;
            } else {
                return IOUtils.toString(bdata, "UTF-8");
            }
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    private static ByteArrayInputStream asInputStream(String data) {
        try {
            return new ByteArrayInputStream(data.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException ex) {
            LOG.error("This REALLY is not supposed to happen", ex);
            return null;
        }
    }

    @CheckForNull
    private String dbRead(Connection connection, String tableName, String id) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("select DATA, BDATA from " + tableName + " where ID = ?");
        try {
            stmt.setString(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return getData(rs, 1, 2);
            } else {
                return null;
            }
        } catch (SQLException ex) {
            LOG.error("attempting to read " + id + " (id length is " + id.length() + ")", ex);
            // DB2 throws an SQLException for invalid keys; handle this more
            // gracefully
            if ("22001".equals(ex.getSQLState())) {
                connection.rollback();
                return null;
            } else {
                throw (ex);
            }
        } finally {
            stmt.close();
        }
    }

    private List<String> dbQuery(Connection connection, String tableName, String minId, String maxId, String indexedProperty,
            long startValue, int limit) throws SQLException {
        String t = "select DATA, BDATA from " + tableName + " where ID > ? and ID < ?";
        if (indexedProperty != null) {
            t += " and MODIFIED >= ?";
        }
        t += " order by ID";
        if (limit != Integer.MAX_VALUE) {
            t += " limit ?";
        }
        PreparedStatement stmt = connection.prepareStatement(t);
        List<String> result = new ArrayList<String>();
        try {
            int si = 1;
            stmt.setString(si++, minId);
            stmt.setString(si++, maxId);
            if (indexedProperty != null) {
                stmt.setLong(si++, startValue);
            }
            if (limit != Integer.MAX_VALUE) {
                stmt.setInt(si++, limit);
            }
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String data = getData(rs, 1, 2);
                result.add(data);
            }
        } finally {
            stmt.close();
        }
        return result;
    }

    private boolean dbUpdate(Connection connection, String tableName, String id, Long modified, Long modcount, Long oldmodcount,
            String data) throws SQLException {
        String t = "update " + tableName + " set MODIFIED = ?, MODCOUNT = ?, SIZE = ?, DATA = ?, BDATA = ? where ID = ?";
        if (oldmodcount != null) {
            t += " and MODCOUNT = ?";
        }
        PreparedStatement stmt = connection.prepareStatement(t);
        try {
            int si = 1;
            stmt.setObject(si++, modified, Types.BIGINT);
            stmt.setObject(si++, modcount, Types.BIGINT);
            stmt.setObject(si++, data.length(), Types.BIGINT);

            if (data.length() < DATALIMIT) {
                stmt.setString(si++, data);
                stmt.setBinaryStream(si++, null, 0);
            } else {
                stmt.setString(si++, "truncated...:" + data.substring(0, 1023));
                ByteArrayInputStream bis = asInputStream(data);
                stmt.setBinaryStream(si++, bis, bis.available());
            }

            stmt.setString(si++, id);
            if (oldmodcount != null) {
                stmt.setObject(si++, oldmodcount, Types.BIGINT);
            }
            int result = stmt.executeUpdate();
            if (result != 1) {
                LOG.debug("DB update failed for " + tableName + "/" + id + " with oldmodcount=" + oldmodcount);
            }
            return result == 1;
        } finally {
            stmt.close();
        }
    }

    private boolean dbInsert(Connection connection, String tableName, String id, Long modified, Long modcount, String data)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("insert into " + tableName + " values(?, ?, ?, ?, ?, ?)");
        try {
            int si = 1;
            stmt.setString(si++, id);
            stmt.setObject(si++, modified, Types.BIGINT);
            stmt.setObject(si++, modcount, Types.BIGINT);
            stmt.setObject(si++, data.length(), Types.BIGINT);
            if (data.length() < DATALIMIT) {
                stmt.setString(si++, data);
                stmt.setBinaryStream(si++, null, 0);
            } else {
                stmt.setString(si++, "truncated...:" + data.substring(0, 1023));
                ByteArrayInputStream bis = asInputStream(data);
                stmt.setBinaryStream(si++, bis, bis.available());
            }

            int result = stmt.executeUpdate();
            if (result != 1) {
                LOG.debug("DB insert failed for " + tableName + "/" + id);
            }
            return result == 1;
        } finally {
            stmt.close();
        }
    }

    private boolean dbDelete(Connection connection, String tableName, String id) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("delete from " + tableName + " where ID = ?");
        try {
            stmt.setString(1, id);
            int result = stmt.executeUpdate();
            if (result != 1) {
                LOG.debug("DB delete failed for " + tableName + "/" + id);
            }
            return result == 1;
        } finally {
            stmt.close();
        }
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        // ignored
    }

    @SuppressWarnings("unchecked")
    private static <T extends Document> T castAsT(NodeDocument doc) {
        return (T) doc;
    }

    // Memory Cache
    private Cache<CacheValue, NodeDocument> nodesCache;
    private CacheStats cacheStats;
    private final Striped<Lock> locks = Striped.lock(64);

    private Lock getAndLock(String key) {
        Lock l = locks.get(key);
        l.lock();
        return l;
    }

    @CheckForNull
    private static NodeDocument unwrap(@Nonnull NodeDocument doc) {
        return doc == NodeDocument.NULL ? null : doc;
    }

    @Nonnull
    private static NodeDocument wrap(@CheckForNull NodeDocument doc) {
        return doc == null ? NodeDocument.NULL : doc;
    }

    /**
     * Adds a document to the {@link #nodesCache} iff there is no document in
     * the cache with the document key. This method does not acquire a lock from
     * {@link #locks}! The caller must ensure a lock is held for the given
     * document.
     * 
     * @param doc
     *            the document to add to the cache.
     * @return either the given <code>doc</code> or the document already present
     *         in the cache.
     */
    @Nonnull
    private NodeDocument addToCache(@Nonnull final NodeDocument doc) {
        if (doc == NodeDocument.NULL) {
            throw new IllegalArgumentException("doc must not be NULL document");
        }
        doc.seal();
        // make sure we only cache the document if it wasn't
        // changed and cached by some other thread in the
        // meantime. That is, use get() with a Callable,
        // which is only used when the document isn't there
        try {
            CacheValue key = new StringValue(doc.getId());
            for (;;) {
                NodeDocument cached = nodesCache.get(key, new Callable<NodeDocument>() {
                    @Override
                    public NodeDocument call() {
                        return doc;
                    }
                });
                if (cached != NodeDocument.NULL) {
                    return cached;
                } else {
                    nodesCache.invalidate(key);
                }
            }
        } catch (ExecutionException e) {
            // will never happen because call() just returns
            // the already available doc
            throw new IllegalStateException(e);
        }
    }

    @Nonnull
    private void applyToCache(@Nonnull final NodeDocument oldDoc, @Nonnull final NodeDocument newDoc) {
        NodeDocument cached = addToCache(newDoc);
        if (cached == newDoc) {
            // successful
            return;
        } else if (oldDoc == null) {
            // this is an insert and some other thread was quicker
            // loading it into the cache -> return now
            return;
        } else {
            CacheValue key = new StringValue(newDoc.getId());
            // this is an update (oldDoc != null)
            if (Objects.equal(cached.getModCount(), oldDoc.getModCount())) {
                nodesCache.put(key, newDoc);
            } else {
                // the cache entry was modified by some other thread in
                // the meantime. the updated cache entry may or may not
                // include this update. we cannot just apply our update
                // on top of the cached entry.
                // therefore we must invalidate the cache entry
                nodesCache.invalidate(key);
            }
        }
    }

    private <T extends Document> void addToCache(Collection<T> collection, T doc) {
        if (collection == Collection.NODES) {
            Lock lock = getAndLock(doc.getId());
            try {
                addToCache((NodeDocument) doc);
            } finally {
                lock.unlock();
            }
        }
    }

    private <T extends Document> void applyToCache(Collection<T> collection, T oldDoc, T newDoc) {
        if (collection == Collection.NODES) {
            Lock lock = getAndLock(newDoc.getId());
            try {
                applyToCache((NodeDocument) oldDoc, (NodeDocument) newDoc);
            } finally {
                lock.unlock();
            }
        }
    }

    private <T extends Document> void addToCacheIfNotNewer(Collection<T> collection, T doc) {
        if (collection == Collection.NODES) {
            String id = doc.getId();
            Lock lock = getAndLock(id);
            CacheValue cacheKey = new StringValue(id);
            try {
                // do not overwrite document in cache if the
                // existing one in the cache is newer
                NodeDocument cached = nodesCache.getIfPresent(cacheKey);
                if (cached != null && cached != NodeDocument.NULL) {
                    // check mod count
                    Number cachedModCount = cached.getModCount();
                    Number modCount = doc.getModCount();
                    if (cachedModCount == null || modCount == null) {
                        throw new IllegalStateException("Missing " + Document.MOD_COUNT);
                    }
                    if (modCount.longValue() > cachedModCount.longValue()) {
                        nodesCache.put(cacheKey, (NodeDocument) doc);
                    }
                } else {
                    nodesCache.put(cacheKey, (NodeDocument) doc);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private Connection getConnection() throws SQLException {
        Connection c = this.ds.getConnection();
        c.setAutoCommit(false);
        return c;
    }

    private void closeConnection(Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (SQLException ex) {
                // log me
            }
        }
    }
}
