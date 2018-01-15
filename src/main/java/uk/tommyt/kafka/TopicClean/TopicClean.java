package uk.tommyt.kafka.TopicClean;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

public class TopicClean {
    private static final Logger LOG = LoggerFactory.getLogger(TopicClean.class.getName());

    public static void main(String[] args) {
        Config config;
        try {
            config = new Config("/config.properties");
        } catch (IOException e) {
            LOG.error(e.toString());
            System.exit(101);
            return;
        }
        String pattern = config.getProperty("pattern").toString();

        // Use Apache Curator + Zookeeper to provide a global mutex
        CuratorClient curatorClient = new CuratorClient(config.getProperty("lock.zkConnectionString").toString());
        Lock lock = new Lock(curatorClient, config.getProperty("lock.path").toString(),
                Long.parseLong(config.getProperty("lock.ttl").toString()));

        // Delete all topics that have match a given regex and have a "delta" of zero (delta being the difference
        // between the oldest and newest message).  A delta of zero implies there are no messages on that topic or that
        // they have all expired.
        try {
            if (!lock.acquire()) {
                LOG.error("Could not lock so aborting");
                System.exit(103);
            } else {
                try (
                        AdminClient adminClient = AdminClient.create(config.propertiesFor("admin"));
                        DeltasConsumer deltasConsumer = new DeltasConsumer(config.propertiesFor("consumer"))
                ) {
                    DeleteTopicsResult result = adminClient.deleteTopics(
                            deltasConsumer.getTopicsForPattern(pattern)
                                    .stream()
                                    .filter(t -> deltasConsumer.getDeltaForTopic(t) == 0)
                                    .collect(Collectors.toList()));
                    result.all().get();

                    Set<String> deletedTopics = result.values().keySet();
                    if (deletedTopics.size() == 0) {
                        LOG.info("No topics found for deletion");
                    } else {
                        LOG.info("Deleted topics: {}", deletedTopics.stream().collect(Collectors.joining(", ")));
                    }
                    lock.release();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}