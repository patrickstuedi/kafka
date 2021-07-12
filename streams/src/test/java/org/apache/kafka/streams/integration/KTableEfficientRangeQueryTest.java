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

package org.apache.kafka.streams.integration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;
import java.util.LinkedList;

//@RunWith(Parameterized.class)
public class KTableEfficientRangeQueryTest {

    private static final String TABLE_NAME = "mytable";

    @Test
    public void test() {
        //Create topology: table from input topic
        final StreamsBuilder builder = new StreamsBuilder();
        final KeyValueBytesStoreSupplier stateStoreSupplier = Stores.inMemoryKeyValueStore(TABLE_NAME);
        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> stateStoreConfig = Materialized
                .<String, String>as(stateStoreSupplier)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withCachingDisabled()
                .withLoggingDisabled();
        final KTable<String, String> table =
                builder.table("input", stateStoreConfig);
        final Topology topology = builder.build();

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            //get input topic and stateStore
            final TestInputTopic<String, String> input = driver
                    .createInputTopic("input", new StringSerializer(), new StringSerializer());
            final ReadOnlyKeyValueStore<String, String> stateStore = driver.getKeyValueStore(TABLE_NAME);

            //write some data
            System.out.println("### writing some data");
            input.pipeInput("a", "value-a");
            input.pipeInput("b", "value-b");
            input.pipeInput("c", "value-c");
            input.pipeInput("d", "value-d");
            input.pipeInput("e", "value-e");

            //query the state store
            final KeyValueIterator<String, String> allIterator = stateStore.all();
            final LinkedList<KeyValue<String, String>> allResult = new LinkedList<>();
            while (allIterator.hasNext()) {
                final KeyValue<String, String> next = allIterator.next();
                allResult.add(next);
            }
            System.out.println("### entire store: " + allResult);

            final String val = stateStore.get("a");
            System.out.println("### single value, key a, value: " + val);

            final KeyValueIterator<String, String> rangeIterator = stateStore.range("b", "d");
            final LinkedList<KeyValue<String, String>> rangeResult = new LinkedList<>();
            while (rangeIterator.hasNext()) {
                final KeyValue<String, String> next = rangeIterator.next();
                rangeResult.add(next);
            }
            System.out.println("### range, b-d: " + rangeResult);

            final KeyValueIterator<String, String> untilIterator = stateStore.rangeUntil("c");
            final LinkedList<KeyValue<String, String>> untilResult = new LinkedList<>();
            while (untilIterator.hasNext()) {
                final KeyValue<String, String> next = untilIterator.next();
                untilResult.add(next);
            }
            System.out.println("### until c: " + untilResult);

            final KeyValueIterator<String, String> fromIterator = stateStore.rangeFrom("c");
            final LinkedList<KeyValue<String, String>> fromResult = new LinkedList<>();
            while (fromIterator.hasNext()) {
                final KeyValue<String, String> next = fromIterator.next();
                fromResult.add(next);
            }
            System.out.println("### from c: " + fromResult);
        }
    }
}
