package sdfs.namenode;

import avro.AvroJsonDecoder;
import avro.namenode.DataNodeInfo;
import avro.namenode.NameNodeRPC;
import org.apache.avro.AvroRemoteException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import sdfs.ZooKeeperSession;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * The current implementation is missing a recovery. Such might based on:
 * 1) write-ahead logging (WAL)
 * 2) nodes are advertising their current dir/files layout.
 * 3) a warm replica
 */
public class NameNodeServer implements NameNodeRPC, Watcher {

    private static final Logger logger = LogManager.getLogger();
    private static final String ZK_HOSTS = Optional.ofNullable(System.getenv("ZOOKEEPER")).orElse("localhost:2181");
    private static final String GROUP_PATH = "/sdfs";
    private static final int REPLICATION_FACTOR = 2;

    private static final ConcurrentHashMap<Path, List<DataNodeInfo>> dir = new ConcurrentHashMap<>();
    private volatile Collection<DataNodeInfo> nodes;

    private final AvroJsonDecoder decoder = new AvroJsonDecoder();
    private ZooKeeper zk;

    /**
     * Reserve a path entry to avoid race conditions.
     *
     * @param path The target path/file
     * @return If the path already exist returning the corresponding nodes for that path.
     * Otherwise two less utilized nodes are served.
     * @throws AvroRemoteException Any unhandled exception wrapped in AvroRemoteException
     */
    @Override
    public List<DataNodeInfo> askForNodes(String path) throws AvroRemoteException {

        return dir.computeIfAbsent(Paths.get(path),
                key -> nodes.stream()
                        .sorted(Comparator.comparingLong(DataNodeInfo::getDiskUsage))
                        .limit(REPLICATION_FACTOR)
                        .collect(Collectors.toList()));
    }

    /**
     * Disk usage advertisement.
     *
     * @param node The dataNode which advertises its disk usage
     * @return null is enforced by auto-gen protocol code
     * @throws AvroRemoteException Any unhandled exception wrapped in AvroRemoteException
     */
    @Override
    public Void advertise(DataNodeInfo node) throws AvroRemoteException {

        String addr = node.getAddress();

        nodes.stream()
                .filter(n -> n.getAddress()
                        .equalsIgnoreCase(addr))
                .findFirst()
                .ifPresent(n -> n.setDiskUsage(node.getDiskUsage()));

        return null;
    }


    @Override
    public void process(WatchedEvent watchedEvent) {

        try {

            switch (watchedEvent.getType()) {

                case None:
                    break;

                case NodeDeleted:
                    // sync is made by NodeChildrenChanged
                    break;

                case NodeCreated:
                    // no watcher is assigned by exists
                    break;

                case NodeDataChanged:
                    // node data is immutable
                    break;

                case NodeChildrenChanged:
                    try {
                        syncGroup();
                    } catch (TimeoutException | IOException e) {
                        // should retry syncGroup or ignore current sync
                    }
                    break;
            }

        } catch (KeeperException e) {
            logger.error(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void connect() throws IOException, InterruptedException, TimeoutException, ExecutionException, KeeperException {

        ZooKeeperSession session = new ZooKeeperSession();

        zk = session.connect(ZK_HOSTS).get(10, TimeUnit.SECONDS);

        syncGroup();
    }

    private synchronized void syncGroup() throws KeeperException, InterruptedException, TimeoutException, IOException {

        List<DataNodeInfo> nodes = new ArrayList<>();

        for (String znode : zk.getChildren(GROUP_PATH, this)) {

            String path = GROUP_PATH + "/" + znode;

            byte[] bytes = zk.getData(path, this, null);

            DataNodeInfo info = decoder.deserialize(DataNodeInfo.class, bytes);

            nodes.add(info);
        }

        this.nodes = Collections.unmodifiableCollection(nodes);
    }
}
