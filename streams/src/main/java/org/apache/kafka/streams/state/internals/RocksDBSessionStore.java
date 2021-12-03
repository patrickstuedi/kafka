/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.SessionWindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;


public class RocksDBSessionStore
    extends WrappedStateStore<SegmentedBytesStore, Object, Object>
    implements SessionStore<Bytes, byte[]> {

    RocksDBSessionStore(final SegmentedBytesStore bytesStore) {
        super(bytesStore);
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound,
        final boolean collectExecutionInfo) {

        if (query instanceof SessionWindowKeyQuery){
            @SuppressWarnings("unchecked") final SessionWindowKeyQuery<Bytes, byte[]> sessionWindowKeyQuery = (SessionWindowKeyQuery<Bytes, byte[]>) query;
            final Bytes key = sessionWindowKeyQuery.getKey();
            final KeyValueIterator<Windowed<Bytes>, byte[]> kvIterator = this.fetch(key);
            @SuppressWarnings("unchecked") final R result = (R) kvIterator;
            final QueryResult<R> queryResult = QueryResult.forResult(result);
            return queryResult;
        } else if (query instanceof WindowRangeQuery){
            @SuppressWarnings("unchecked") final WindowRangeQuery<Bytes, Long> windowRangeQuery = (WindowRangeQuery<Bytes, Long>) query;
            final Bytes key = windowRangeQuery.getLowerBound();
            final long windowLower = windowRangeQuery.getWindowLowerBound();
            final long windowUpper = windowRangeQuery.getWindowUpperBound();
            if (windowRangeQuery.getUpperBound().isPresent()){
                final Bytes upperKey = windowRangeQuery.getUpperBound().get();
                final KeyValueIterator<Windowed<Bytes>, byte[]> kvIterator = this.findSessions(key, upperKey, windowLower, windowUpper);
                @SuppressWarnings("unchecked") final R result = (R) kvIterator;
                final QueryResult<R> queryResult = QueryResult.forResult(result);
                return queryResult;
            } else {
                final KeyValueIterator<Windowed<Bytes>, byte[]> kvIterator = this.findSessions(key, windowLower, windowUpper);
                @SuppressWarnings("unchecked") final R result = (R) kvIterator;
                final QueryResult<R> queryResult = QueryResult.forResult(result);
                return queryResult;
            }
        }

        return StoreQueryUtils.handleBasicQueries(query, positionBound, collectExecutionInfo, this);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(
            key,
            earliestSessionEndTime,
            latestSessionStartTime
        );
        return new WrappedSessionStoreIterator(bytesIterator);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes key,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(
            key,
            earliestSessionEndTime,
            latestSessionStartTime
        );
        return new WrappedSessionStoreIterator(bytesIterator);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                  final Bytes keyTo,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(
            keyFrom,
            keyTo,
            earliestSessionEndTime,
            latestSessionStartTime
        );
        return new WrappedSessionStoreIterator(bytesIterator);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes keyFrom,
                                                                          final Bytes keyTo,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(
            keyFrom,
            keyTo,
            earliestSessionEndTime,
            latestSessionStartTime
        );
        return new WrappedSessionStoreIterator(bytesIterator);
    }

    @Override
    public byte[] fetchSession(final Bytes key,
                               final long earliestSessionEndTime,
                               final long latestSessionStartTime) {
        return wrapped().get(SessionKeySchema.toBinary(
            key,
            earliestSessionEndTime,
            latestSessionStartTime
        ));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes key) {
        return backwardFindSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo) {
        return findSessions(keyFrom, keyTo, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo) {
        return backwardFindSessions(keyFrom, keyTo, 0, Long.MAX_VALUE);
    }

    @Override
    public void remove(final Windowed<Bytes> key) {
        wrapped().remove(SessionKeySchema.toBinary(key));
    }

    @Override
    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
        wrapped().put(SessionKeySchema.toBinary(sessionKey), aggregate);
    }
}
