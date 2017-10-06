package sdfs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Create persistent znode named /sdfs other members should be joined to
 * RUN JUST ONCE
 */
public class CreateGroup {

    private static final Logger logger = LogManager.getLogger();
    private static final String ZK_HOSTS = Optional.ofNullable(System.getenv("ZOOKEEPER")).orElse("localhost:2181");
    private static final String GROUP_NAME = "sdfs";

    public static void main(String[] args) throws Exception {

        ZooKeeper zk = null;

        try {

            zk = createGroup();

        } finally {
            if (zk != null) zk.close();
        }
    }

    private static ZooKeeper createGroup() throws InterruptedException, ExecutionException, TimeoutException, IOException, KeeperException {

        ZooKeeperSession session = new ZooKeeperSession();

        ZooKeeper zk = session.connect(ZK_HOSTS).get(2, TimeUnit.SECONDS);

        String path = "/" + GROUP_NAME;

        String createdPath = zk.create(path, null/*data*/, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        logger.info("Created " + createdPath);

        return zk;
    }
}
