package org.apache.kafka.clients.producer;

public interface TopicPartitionOffset {
    String topic();

    int partition();

    long offset();
}
