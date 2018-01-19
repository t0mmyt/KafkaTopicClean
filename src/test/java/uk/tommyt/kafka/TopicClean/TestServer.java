package uk.tommyt.kafka.TopicClean;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.curator.test.TestingServer;

import java.util.Properties;

public class TestServer {
    private KafkaServerStartable kf;
    private TestingServer zk;

    @Before
    public void bootKafka() throws Exception {
        zk = new TestingServer();
        zk.start();
        String zkConnectString = zk.getConnectString();
        System.out.printf("Starting zookeeper: %s\n", zkConnectString);
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zk.getConnectString());
        kf = new KafkaServerStartable(new KafkaConfig(properties));
        kf.startup();
    }

    @After
    public void stopKafka() throws Exception {
        zk.stop();
        kf.shutdown();
    }

    @Test
    public void foo() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("boostrap.servers", "localhost:9092");
//        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        Thread.sleep(30000);
    }

}
