package sdfs.namenode;

import avro.namenode.NameNodeRPC;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.*;

/**
 * The NameNode maintains the namespace tree and the mapping of files to DataNodes.
 */
public class NameNode {

    private static final Logger logger = LogManager.getLogger();

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final CountDownLatch latch = new CountDownLatch(1);

    private static Server server;

    public static void main(String[] args) throws IOException {

        NameNode node = new NameNode();
        node.bootstrap();
        Runtime.getRuntime().addShutdownHook(new Thread(node::shutdown));

        System.out.println("NameNode has been started. Hit enter to stop...");
        System.in.read();

        node.shutdown();
    }

    public void shutdown() {
        latch.countDown();
        executor.shutdown();
    }

    public void bootstrap() {

        executor.execute(() -> {
            try {
                startServer();
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException | ExecutionException | TimeoutException | KeeperException e) {
                logger.error(e);
            } finally {
                if (server != null) server.close();
            }
        });
    }

    /**
     * The server implements the NameNodeRPC protocol (DataNodeServer)
     *
     * @throws IOException
     */
    private static void startServer() throws IOException, InterruptedException, ExecutionException, TimeoutException, KeeperException {

        NameNodeServer nameNodeServer = new NameNodeServer();

        nameNodeServer.connect();

        server = new NettyServer(new SpecificResponder(NameNodeRPC.class, nameNodeServer), new InetSocketAddress(65111));
    }
}
