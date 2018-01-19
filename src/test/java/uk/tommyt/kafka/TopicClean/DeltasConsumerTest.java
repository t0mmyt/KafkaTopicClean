package uk.tommyt.kafka.TopicClean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class DeltasConsumerTest {
    private MockConsumer<byte[], byte[]> consumer;
    private DeltasConsumer dc;
    private Node[] nodes;
    private String[] topics;
    private String pattern;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        nodes = new Node[3];
        nodes[0] = new Node(0, "node1", 9092);
        nodes[1] = new Node(1, "node2", 9092);
        nodes[2] = new Node(2, "node3", 9092);
        topics = new String[] {"TRX-1-TRX-SYS", "TRX-2-PDC-SYS", "TRX-3-PDC-SYS",  "some_other_topic"};
        pattern = "^TRX\\-\\d+\\-\\w+\\-\\w+";
        for (String topic: topics) {
            List<PartitionInfo> partitionInfos = new ArrayList<>();
            for (int j=0;j<12;j++) {
                partitionInfos.add(new PartitionInfo(topic, j, nodes[j % 3], nodes, nodes));
            }
            consumer.updatePartitions(topic, partitionInfos);
            consumer.updateBeginningOffsets(partitionInfos.stream().collect(
                    Collectors.toMap(p -> new TopicPartition(p.topic(), p.partition()),p -> 0L)));
            consumer.updateEndOffsets(partitionInfos.stream().collect(
                    Collectors.toMap(p -> new TopicPartition(p.topic(), p.partition()),p -> 0L)));
        }
    }

    @Test
    public void FindTopics() {
        dc = new DeltasConsumer(consumer);
        Set<String> matchedTopics = dc.getTopicsForPattern(pattern);
        assertEquals(3, matchedTopics.size());
    }

    @Test
    public void FindOnlyEmptyTopics() {
        dc = new DeltasConsumer(consumer);
        Set<TopicPartition> tps = new HashSet<>();
        for (String topic: topics) {
            tps.addAll(consumer.partitionsFor(topic)
                    .stream()
                    .map(t -> new TopicPartition(topic, t.partition()))
                    .collect(Collectors.toSet())
            );
        }
        consumer.assign(tps);
        consumer.addRecord(new ConsumerRecord<>(topics[0], 0, 0L, "key".getBytes(), "value".getBytes()));
        consumer.updateEndOffsets(Collections.singletonMap(new TopicPartition(topics[0], 0), 1L));
        Set<String> matchedTopics = dc.getTopicsForPattern(pattern)
                .stream()
                .filter(t -> dc.getDeltaForTopic(t) == 0)
                .collect(Collectors.toSet());
        assertEquals(2, matchedTopics.size());
    }

    @Test
    public void CheckPatternMatch() {
        String pattern = "^TRX\\-\\d+\\-\\w+\\-\\w+";
        Set<String> topics = Stream.of(
                "ignore-me",
                "TRX-1-a-EOD",
                "TRX-123-PDC-SYS",
                "TRX-9-SDC-CHRONO"
                ).collect(Collectors.toSet());
        Set<String> selection = DeltasConsumer.filterByPattern(topics, pattern);
        assertEquals(3, selection.size());
    }
}