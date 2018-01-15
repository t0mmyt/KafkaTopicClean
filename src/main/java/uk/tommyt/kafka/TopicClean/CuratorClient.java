package uk.tommyt.kafka.TopicClean;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

public class CuratorClient {
    RetryPolicy retryPolicy;
    CuratorFramework client;

    public CuratorClient(String zkConnString) {
        retryPolicy = new RetryNTimes(0, 0);
        client = CuratorFrameworkFactory.newClient(zkConnString, retryPolicy);
        client.start();
    }
}
