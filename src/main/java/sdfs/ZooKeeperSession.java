package sdfs;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Create ZooKeeper session
 */
public class ZooKeeperSession implements Watcher {

    private static final int SESSION_TIMEOUT = 5000;

    private ZooKeeper zk;
    private CompletableFuture<ZooKeeper> future = new CompletableFuture<>();

    public Future<ZooKeeper> connect(String hosts) throws IOException, InterruptedException {
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        return future;
    }

    @Override
    public void process(WatchedEvent e) {
        if (e.getState() == KeeperState.SyncConnected) {
            future.complete(zk);
        }

        future.completeExceptionally(new IllegalStateException(String.format("zk connection failed: %s", e.getState())));
    }
}
