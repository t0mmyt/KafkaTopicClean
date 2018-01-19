package uk.tommyt.kafka.TopicClean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DeltasConsumer implements AutoCloseable {
    private final Logger LOG = LoggerFactory.getLogger(getClass().getName());
    private Consumer<byte[], byte[]> consumer;

    /**
     * Create a KafkaConsumer from properties.  Does not use key.serializer or value.serializer
     */
    public DeltasConsumer(Properties properties) {
        Properties newProperties = new Properties();
        newProperties.putAll(properties);
        newProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        newProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.consumer = new KafkaConsumer<>(newProperties);
    }

    /**
     * Use a pre-fabricated consumer, e.g. MockConsumer for testing or custom serializers if required
     */
    DeltasConsumer(Consumer<byte[], byte[]> consumer){
        this.consumer = consumer;
    }

    /**
     * Return a list of topics that match the regex provided by pattern
     */
    public Set<String> getTopicsForPattern(String pattern) {
        LOG.debug("Searching for topics with regex: {}", pattern);
        Set<String> topics = filterByPattern(consumer.listTopics().keySet(), pattern);
        LOG.debug("Found {} topics", topics.size());
        return topics;
    }

    /**
     * Get the "delta" for a topic, (delta in this case being the difference between the oldest and newest message or
     * how many messages are available for consumption)
     */
    public long getDeltaForTopic(String topic) {
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream().
                map(i -> new TopicPartition(topic, i.partition())).collect(Collectors.toList());
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        long oldest = partitions.stream().mapToLong(consumer::position).sum();
        consumer.seekToEnd(partitions);
        long newest = partitions.stream().mapToLong(consumer::position).sum();
        consumer.assign(Collections.emptyList());
        long delta = newest - oldest;
        LOG.debug("Delta for {}: {}", topic, delta);
        return delta;
    }

    /**
     * Close the consumer
     */
    public void close() {
        consumer.close();
    }

    static Set<String> filterByPattern(Set<String> topics, String pattern) {
        Pattern p = Pattern.compile(pattern);
        return topics.stream()
                .filter(t -> p.matcher(t).find())
                .collect(Collectors.toSet());

    }
}
