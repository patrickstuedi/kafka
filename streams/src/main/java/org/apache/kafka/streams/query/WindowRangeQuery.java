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

package org.apache.kafka.streams.query;

import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Optional;

public class WindowRangeQuery<K, V> implements Query<KeyValueIterator<K, V>> {

    private final Optional<K> lower;
    private final Optional<K> upper;
    private final Optional<Long> windowLower;
    private final Optional<Long> windowUpper;

    private WindowRangeQuery(final Optional<K> lower, final Optional<K> upper, final Optional<Long> windowLower, final Optional<Long> windowUpper) {
        this.lower = lower;
        this.upper = upper;
        this.windowLower = windowLower;
        this.windowUpper = windowUpper;
    }

    public static <K, V> WindowRangeQuery<K, V> withKeyRange(final K lower, final K upper, final Optional<Long> windowLower, final Optional<Long> windowUpper) {
        return new WindowRangeQuery<>(Optional.of(lower), Optional.of(upper), windowLower, windowUpper);
    }

    public static <K, V> WindowRangeQuery<K, V> withKeyUpperBound(final K upper, final Optional<Long> windowLower, final Optional<Long> windowUpper) {
        return new WindowRangeQuery<>(Optional.empty(), Optional.of(upper), windowLower, windowUpper);
    }

    public static <K, V> WindowRangeQuery<K, V> withKeyLowerBound(final K lower, final Optional<Long> windowLower, final Optional<Long> windowUpper) {
        return new WindowRangeQuery<>(Optional.of(lower), Optional.empty(), windowLower, windowUpper);
    }

    public static <K, V> WindowRangeQuery<K, V> withNoKeyBounds(final Optional<Long> windowLower, final Optional<Long> windowUpper) {
        return new WindowRangeQuery<>(Optional.empty(), Optional.empty(), windowLower, windowUpper);
    }

    public Optional<K> getKeyLowerBound() {
        return lower;
    }

    public Optional<K> getKeyUpperBound() {
        return upper;
    }

    public Optional<Long> getWindowLowerBound() {
        return windowLower;
    }

    public Optional<Long> getWindowUpperBound() {
        return windowUpper;
    }
}
