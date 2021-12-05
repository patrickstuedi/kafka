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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static java.util.Collections.singleton;
import static org.apache.kafka.streams.query.StateQueryRequest.inStore;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class IQv2WindowStoreIntegrationTest {


    private static final int NUM_BROKERS = 1;
    public static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    private static int port = 0;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final Position INPUT_POSITION = Position.emptyPosition();
    private static final String STORE_NAME = "kv-store";

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private KafkaStreams kafkaStreams;
    private enum StoreType { InMemory, RocksDB };
    private IQv2WindowStoreIntegrationTest.StoreType storeType;
    private boolean enableLogging;
    private boolean enableCaching;
    private boolean forward;
    private Properties props;

    public IQv2WindowStoreIntegrationTest(final IQv2WindowStoreIntegrationTest.StoreType storeType, final boolean enableLogging, final boolean enableCaching, final boolean forward) {
        this.storeType = storeType;
        this.enableLogging = enableLogging;
        this.enableCaching = enableCaching;
        this.forward = forward;
        this.props = streamsConfiguration(false, false, "bla");
    }

    @Parameterized.Parameters(name = "storeType={0}, enableLogging={1}, enableCaching={2}, forward={3}")
    public static Collection<Object[]> data() {
        final List<StoreType> types = Arrays.asList(StoreType.RocksDB);
        final List<Boolean> logging = Arrays.asList(true);
        final List<Boolean> caching = Arrays.asList(true);
        final List<Boolean> forward = Arrays.asList(true);
        return buildParameters(types, logging, caching, forward);
    }

    private static Collection<Object[]> buildParameters(final List<?>... argOptions) {
        List<Object[]> result = new LinkedList<>();
        result.add(new Object[0]);

        for (final List<?> argOption : argOptions) {
            result = times(result, argOption);
        }

        return result;
    }

    private static List<Object[]> times(final List<Object[]> left, final List<?> right) {
        final List<Object[]> result = new LinkedList<>();
        for (final Object[] args : left) {
            for (final Object rightElem : right) {
                final Object[] resArgs = new Object[args.length + 1];
                System.arraycopy(args, 0, resArgs, 0, args.length);
                resArgs[args.length] = rightElem;
                result.add(resArgs);
            }
        }
        return result;
    }

    @BeforeClass
    public static void before()
            throws InterruptedException, IOException, ExecutionException, TimeoutException {
        CLUSTER.start();
        final int partitions = 2;
        CLUSTER.createTopic(INPUT_TOPIC_NAME, partitions, 1);

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        final List<Future<RecordMetadata>> futures = new LinkedList<>();
        try (final Producer<Integer, Integer> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < 3; i++) {
                final Future<RecordMetadata> send = producer.send(
                        new ProducerRecord<>(
                                INPUT_TOPIC_NAME,
                                i % partitions,
                                Time.SYSTEM.milliseconds(),
                                i,
                                i,
                                null
                        )
                );
                futures.add(send);
                Time.SYSTEM.sleep(1L);
            }
            producer.flush();

            for (final Future<RecordMetadata> future : futures) {
                final RecordMetadata recordMetadata = future.get(1, TimeUnit.MINUTES);
                assertThat(recordMetadata.hasOffset(), is(true));
                INPUT_POSITION.withComponent(
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset()
                );
            }
        }

        assertThat(INPUT_POSITION, equalTo(
                Position
                        .emptyPosition()
                        .withComponent(INPUT_TOPIC_NAME, 0, 1L)
                        .withComponent(INPUT_TOPIC_NAME, 1, 0L)
        ));
    }

    @Before
    public void beforeTest() {
        final StreamsBuilder builder = new StreamsBuilder();
        final Supplier<WindowBytesStoreSupplier> createStore = () -> {
            if (storeType == StoreType.InMemory) {
                return Stores.inMemoryWindowStore(STORE_NAME, Duration.ofDays(1), Duration.ofDays(1), true);
            } else if (storeType == StoreType.RocksDB) {
                return Stores.persistentWindowStore(STORE_NAME, Duration.ofDays(1), Duration.ofDays(1), true);
            } else {
                return Stores.persistentWindowStore(STORE_NAME, Duration.ofDays(1), Duration.ofDays(1), true);
            }
        };

        final WindowBytesStoreSupplier supplier = createStore.get();
        final Materialized<Integer, Integer, WindowStore<Bytes, byte[]>> materialized =
                Materialized.as(supplier);
        if (enableCaching) {
            materialized.withCachingEnabled();
        } else {
            materialized.withCachingDisabled();
        }

        if (enableLogging) {
            materialized.withLoggingEnabled(Collections.emptyMap());
        } else {
            materialized.withCachingDisabled();
        }

        builder.stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer())).groupByKey()

            .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))
            .aggregate(
                () -> 0,
                (key, value, aggregate) -> aggregate + value,
                materialized
        );
        kafkaStreams = IntegrationTestUtils.getStartedStreams(
            streamsConfiguration(enableCaching, enableLogging, supplier.getClass().getSimpleName()),
            builder,
           true
        );

    }

    @After
    public void afterTest() {
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }

    @AfterClass
    public static void after() {
        CLUSTER.stop();
    }


    @Test
    public void shouldQueryUsingWindowKeyQuery() {
        final Integer key = 1;
        final WindowKeyQuery<Integer, byte[]> query = WindowKeyQuery.withKeyAndWindowLowerBound(key, 0);

        final int partition = 1;
        final Set<Integer> partitions = singleton(partition);
        final StateQueryRequest<byte[]> request =
                inStore(STORE_NAME).withQuery(query).withPartitions(partitions);

        final StateQueryResult<byte[]> result =
                IntegrationTestUtils.iqv2WaitForPartitionsOrGlobal(kafkaStreams, request, partitions);

        assertThat(result.getPartitionResults().keySet(), equalTo(partitions));
    }

    private static Properties streamsConfiguration(final boolean cache, final boolean log,
                                                   final String supplier) {
        final String safeTestName = IQv2WindowStoreIntegrationTest.class.getName();
        final Properties config = new Properties();
        config.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + (++port));
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 200);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        return config;
    }




}
