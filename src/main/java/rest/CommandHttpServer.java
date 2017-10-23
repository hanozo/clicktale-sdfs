package rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import mq.MQProducer;
import mq.MQRPCClient;
import mq.rabbit.RabbitProducer;
import mq.rabbit.RabbitRPCClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import rest.controllers.DirCommandController;
import rest.controllers.FileCommandController;
import rest.domain.DirDomain;
import rest.domain.FileDomain;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * Grizzly HTTP server
 */
public class CommandHttpServer {

    private static final Logger logger = LogManager.getLogger();
    private static final String BASE_URI;
    private static final String RPC_COMMANDS_QUEUE = "commands";

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final CountDownLatch latch = new CountDownLatch(1);

    static {

        log4jSetup();

        BASE_URI = String.format("http://%s:%s/sdfs/",
                Optional.ofNullable(System.getenv("HOSTNAME")).orElse("localhost"),
                Optional.ofNullable(System.getenv("PORT")).orElse("8080"));

    }

    public static void main(String[] args) throws IOException {

        CommandHttpServer server = new CommandHttpServer();

//        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));

        server.bootstrap();

        System.out.println(String.format("Jersey app started with WADL available at "
                + "%sapplication.wadl\nHit enter to stop it...", BASE_URI));
        //noinspection ResultOfMethodCallIgnored
        System.in.read();

        server.shutdown();
    }


    public void shutdown() {
        latch.countDown();
        executor.shutdown();
    }

    public void bootstrap() {

        executor.execute(() -> {

            try (MQRPCClient client = new RabbitRPCClient("localhost")) {

                try (DirDomain dirDomain = new DirDomain(client); FileDomain fileDomain = new FileDomain(client)) {
                    final HttpServer server = startServer(dirDomain, fileDomain);

                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        server.shutdown();
                    }
                }
            } catch (TimeoutException | IOException e) {
                logger.error(e);
            }
        });

    }

    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     *
     * @return Grizzly HTTP server.
     */
    private static HttpServer startServer(DirDomain dirDomain, FileDomain fileDomain) {

        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
        provider.setMapper(mapper);

        // create a resource config that scans for JAX-RS resources
        final ResourceConfig rc = new ResourceConfig()
                .packages(FileCommandController.class.getPackage().getName())
                .packages(DirCommandController.class.getPackage().getName())
                .register(provider)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(dirDomain).to(DirDomain.class);
                        bind(fileDomain).to(FileDomain.class);
                    }
                });

        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
    }

    private static void log4jSetup() {
        try {
            System.setProperty("myHostName", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            System.setProperty("myHostName", "invalidHostName");
        }

        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

}
