package sdfs.datanode;

import avro.AvroJsonEncoder;
import avro.commands.DataNodeRPC;
import avro.namenode.DataNodeInfo;
import avro.namenode.NameNodeRPC;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import sdfs.ZooKeeperSession;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * Multiple data nodes can advertise themselves to the name node.
 */
public class DataNode {

    private static final Logger logger = LogManager.getLogger();
    private static final String ZK_HOSTS = Optional.ofNullable(System.getenv("ZOOKEEPER")).orElse("localhost:2181");
    private static final String GROUP_NAME = "sdfs";
    private static final String MEMBER_NAME = "znode-";

    private static Server server;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService advertiser = Executors.newSingleThreadScheduledExecutor();

    private final CountDownLatch latch = new CountDownLatch(1);
    private final AvroJsonEncoder encoder = new AvroJsonEncoder();

    private ZooKeeper zk;

    public static void main(String[] args) throws IOException {

        if (args.length != 1)
            throw new IllegalArgumentException("a port number e.g. in the range: [65112-65200] is required");

        int port = Integer.parseInt(args[0]);

        DataNode node = new DataNode();
        node.bootstrap(port);
        Runtime.getRuntime().addShutdownHook(new Thread(node::shutdown));

        System.out.println("DataNode has been advertised. Hit enter to stop...");
        //noinspection ResultOfMethodCallIgnored
        System.in.read();

        node.shutdown();
    }

    public void shutdown() {
        latch.countDown();
        executor.shutdown();
        advertiser.shutdown();
    }

    private void bootstrap(final int port) {
        bootstrap(port, null);
    }

    public void bootstrap(final int port, final String testPath) {

        executor.submit(() -> {

            try {

                String createdPath = joinGroup(port);

                String perNodePath = (testPath != null) ? testPath : createdPath;

                NettyTransceiver netty = null;

                try {

                    String hostname = Optional
                            .ofNullable(System.getenv("NAME_NODE"))
                            .orElse(InetAddress.getLocalHost().getHostName());

                    netty = new NettyTransceiver(new InetSocketAddress(hostname, 65111));

                    final NameNodeRPC proxy = SpecificRequestor.getClient(NameNodeRPC.class, netty);

                    advertiser.scheduleAtFixedRate(() -> {

                        Path folder = Paths.get(DataNodeServer.DATA, perNodePath);
                        if (!Files.exists(folder)) return;

                        long size = 0;
                        try {
                            size = Files.walk(folder)
                                    .filter(p -> p.toFile().isFile())
                                    .mapToLong(p -> p.toFile().length())
                                    .sum();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        try {
                            /* disk usage advertisement */
                            proxy.advertise(getDataNodeInfo(port, size));
                        } catch (AvroRemoteException | UnknownHostException e) {
                            logger.error(e);
                        }

                    }, 0, 2, TimeUnit.SECONDS);

                    startServer(perNodePath, port);

                    latch.await();

                } finally {
                    if (server != null) server.close();
                    if (netty != null) netty.close();
                }

            } catch (IOException | KeeperException | ExecutionException | TimeoutException e) {
                logger.error(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

            } finally {
                if (zk != null) {
                    try {
                        zk.close();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

        });
    }


    private String joinGroup(int port) throws IOException, InterruptedException, TimeoutException, ExecutionException, KeeperException {

        ZooKeeperSession session = new ZooKeeperSession();

        zk = session.connect(ZK_HOSTS).get(2, TimeUnit.SECONDS);

        String path = "/" + GROUP_NAME + "/" + MEMBER_NAME;

        DataNodeInfo info = getDataNodeInfo(port, 0);

        byte[] payload = encoder.encode(info);

        String createdPath = zk.create(path, payload, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        logger.info("Created " + createdPath);

        return createdPath;
    }

    private DataNodeInfo getDataNodeInfo(int port, long size) throws UnknownHostException {

        return DataNodeInfo.newBuilder()
                .setAddress(InetAddress.getLocalHost().getHostName())
                .setPort(port)
                .setDiskUsage(size)
                .build();
    }

    /**
     * The server implements the DataNodeRPC protocol (DataNodeServer)
     *
     * @param perNodePath Is created per node while running on your local machine.
     * @param port        A different port is required while running multiple instances of data nodes on local machine.
     * @throws IOException DataNode RPC server failure.
     */
    private static void startServer(String perNodePath, int port) throws IOException {

        server = new NettyServer(new SpecificResponder(DataNodeRPC.class, new DataNodeServer(perNodePath)), new InetSocketAddress(port));
    }
}
