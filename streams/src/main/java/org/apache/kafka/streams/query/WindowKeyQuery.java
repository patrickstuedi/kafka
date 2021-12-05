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

import java.util.Optional;

public class WindowKeyQuery<K, V> implements Query<V> {

    private final K key;
    private final Optional<Long> windowLower;

    private WindowKeyQuery(final K key, final Optional<Long> windowLower) {
        this.key = key;
        this.windowLower = windowLower;
    }

    public static <K, V> WindowKeyQuery<K, V> withKey(final K key) {
        return new WindowKeyQuery<>(key, Optional.empty());
    }

    public static <K, V> WindowKeyQuery<K, V> withKeyAndWindowLowerBound(final K key, final long time) {
        return new WindowKeyQuery<>(key, Optional.of(time));
    }

    public K getKey() {
        return key;
    }

    public Optional<Long> getWindowLower() {
        return windowLower;
    }
}