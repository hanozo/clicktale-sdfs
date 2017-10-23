package processes;

import mq.rabbit.RabbitRPCServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * Fetch dir/file commands from RMQ and invokes RPC on the corresponding data node.
 */
public class CommandDispatcher {

    private static final Logger logger = LogManager.getLogger();
    private static final String RMQ_HOST = Optional.ofNullable(System.getenv("RMQ_HOST")).orElse("localhost");

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, TimeoutException {

        CommandDispatcher dispatcher = new CommandDispatcher();
        dispatcher.bootstrap();
        Runtime.getRuntime().addShutdownHook(new Thread(dispatcher::shutdown));

        System.out.println("CommandDispatcher started. Hit enter to stop...");
        //noinspection ResultOfMethodCallIgnored
        System.in.read();

        dispatcher.shutdown();
    }

    public void shutdown() {
        logger.info("CommandDispatcher shutdown is in progress...");
        latch.countDown();
        executor.shutdown();
    }

    public void bootstrap() {

        executor.execute(this::exec);
    }

    private void exec() {

        logger.info("Client built, got proxy.");

        try (RabbitRPCServer server = new RabbitRPCServer(RMQ_HOST)) {

            CommandExecutor cmdExecutor = new CommandExecutor();

            server.process(cmd -> {

                logger.info(" [x] Received '" + cmd.getSchema().getName() + "'");

                try {

                    cmdExecutor.execute(cmd);

                    return cmd; // response

                } catch (IOException e) {
                    logger.error(e);

                    return cmd; // error
                }
            });

            latch.await();

        } catch (TimeoutException | IOException e) {
            logger.error(e);
        } catch (InterruptedException ignore) {
        }
    }
}
