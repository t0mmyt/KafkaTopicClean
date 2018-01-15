package uk.tommyt.kafka.TopicClean;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Lock {
    private final Logger LOG = LoggerFactory.getLogger(getClass().getName());
    private CuratorClient client;
    private String path;
    private InterProcessMutex lock;
    private long ttl;

    public Lock(CuratorClient client, String path, long ttl) {
        this.client = client;
        this.path = path;
        this.ttl = ttl;
        lock = new InterProcessMutex(client.client, path);
    }

    public boolean acquire() throws Exception {
        boolean locked = lock.acquire(ttl, TimeUnit.MILLISECONDS);
        if (locked) {
            LOG.debug("Acquired lock at {}", path);
        } else {
            LOG.warn("Could not get a lock at {}", path);
        }
        return locked;
    }

    public void release() throws Exception {
        lock.release();
        LOG.debug("Released lock at {}", path);
    }
}
