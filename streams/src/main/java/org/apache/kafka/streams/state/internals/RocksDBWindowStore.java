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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class RocksDBWindowStore
    extends WrappedStateStore<SegmentedBytesStore, Object, Object>
    implements WindowStore<Bytes, byte[]> {

    private final boolean retainDuplicates;
    private final long windowSize;

    private int seqnum = 0;

    private final Position position;
    private StateStoreContext stateStoreContext;

    RocksDBWindowStore(final SegmentedBytesStore bytesStore,
                       final boolean retainDuplicates,
                       final long windowSize) {
        super(bytesStore);
        this.retainDuplicates = retainDuplicates;
        this.windowSize = windowSize;
        this.position = Position.emptyPosition();
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        super.init(context, root);
        this.stateStoreContext = context;
    }

    Position getPosition() {
        return position;
    }

    @Override
    public void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
        // Skip if value is null and duplicates are allowed since this delete is a no-op
        if (!(value == null && retainDuplicates)) {
            maybeUpdateSeqnumForDups();
            wrapped().put(WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, seqnum), value);

            StoreQueryUtils.updatePosition(position, stateStoreContext);
        }
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp) {
        return wrapped().get(WindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum));
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(key, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).valuesIterator();
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(key, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).valuesIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                           final Bytes keyTo,
                                                           final long timeFrom,
                                                           final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                   final Bytes keyTo,
                                                                   final long timeFrom,
                                                                   final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().all();
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardAll();
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetchAll(timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetchAll(timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound,
        final boolean collectExecutionInfo) {

        if (query instanceof WindowKeyQuery) {
            @SuppressWarnings("unchecked") final WindowKeyQuery<Bytes, byte[]> windowKeyQuery = (WindowKeyQuery<Bytes, byte[]>) query;
            if (windowKeyQuery.getWindowLower().isPresent()) {
                final Bytes key = windowKeyQuery.getKey();
                final long time = windowKeyQuery.getWindowLower().get();
                final byte[] value = this.fetch(key, time);
                @SuppressWarnings("unchecked") final R result = (R) value;
                final QueryResult<R> queryResult = QueryResult.forResult(result);
                return queryResult;
            }
        } else if (query instanceof WindowRangeQuery) {
            @SuppressWarnings("unchecked") final WindowRangeQuery<Bytes, byte[]> windowRangeQuery = (WindowRangeQuery<Bytes, byte[]>) query;
            if (windowRangeQuery.getKeyLowerBound().isPresent() &&
                windowRangeQuery.getKeyUpperBound().isPresent() &&
                windowRangeQuery.getWindowLowerBound().isPresent() &&
                windowRangeQuery.getWindowUpperBound().isPresent()) {

                final Bytes keyLower = windowRangeQuery.getKeyLowerBound().get();
                final Bytes keyUpper = windowRangeQuery.getKeyUpperBound().get();
                final long windowLower = windowRangeQuery.getWindowLowerBound().get();
                final long windowUpper = windowRangeQuery.getWindowUpperBound().get();
                final KeyValueIterator<Windowed<Bytes>, byte[]> kvIterator = this.fetch(keyLower, keyUpper, windowLower, windowUpper);
                @SuppressWarnings("unchecked") final R result = (R) kvIterator;
                final QueryResult<R> queryResult = QueryResult.forResult(result);
                return queryResult;
            }
        }

        return StoreQueryUtils.handleBasicQueries(query, positionBound, collectExecutionInfo, this);
    }

    private void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }
}
