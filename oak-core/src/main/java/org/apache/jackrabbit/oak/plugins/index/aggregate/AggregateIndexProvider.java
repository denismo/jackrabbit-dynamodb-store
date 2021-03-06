/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.aggregate;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A provider for aggregate indexes. It wraps all full-text query indexes.
 */
public class AggregateIndexProvider implements QueryIndexProvider {
    
    private final QueryIndexProvider baseProvider;
    
    AggregateIndexProvider(QueryIndexProvider baseProvider) {
        this.baseProvider = baseProvider;
    }
    
    @Nonnull
    public static QueryIndexProvider wrap(
            @Nonnull QueryIndexProvider baseProvider) {
        return new AggregateIndexProvider(baseProvider);
    }

    @Override @Nonnull
    public List<? extends QueryIndex> getQueryIndexes(NodeState state) {
        List<? extends QueryIndex> list = baseProvider.getQueryIndexes(state);
        ArrayList<AggregateIndex> aggregateList = new ArrayList<AggregateIndex>();
        for (int i = 0; i < list.size(); i++) {
            QueryIndex index = list.get(i);
            if (index instanceof FulltextQueryIndex) {
                aggregateList
                        .add(new AggregateIndex((FulltextQueryIndex) index));
            }
        }
        return aggregateList;
    }

}
