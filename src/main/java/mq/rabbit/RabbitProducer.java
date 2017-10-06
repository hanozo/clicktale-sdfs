package mq.rabbit;

import avro.AvroJsonEncoder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import mq.MQProducer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * Please serialize producer access single thread at the time.
 */
public class RabbitProducer implements MQProducer {

    private static final Logger logger = LogManager.getLogger();

    private final BlockingQueue<SpecificRecord> sequentialSend = new LinkedBlockingQueue<>(1024 * 1024);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Future<?> publisher;

    private final AvroJsonEncoder encoder = new AvroJsonEncoder();
    private final Connection connection;
    private final Channel channel;

    public RabbitProducer(final String host, final String queue) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(queue, false, false, false, null);

        publisher = executor.submit(() -> {

            // Serialize MQ access through blocking queue
            while (!executor.isShutdown()) {
                try {
                    SpecificRecord message = sequentialSend.take();
                    channel.basicPublish("", queue, null, encoder.serialize(message));
                } catch (InterruptedException e) {
                    break;
                } catch (IOException e) {
                    logger.error(e);
                }
            }

        });
    }

    /**
     * @param message The specific message.
     * @param <T>     SpecificRecord interface used to constraint to avro messages.
     * @throws InterruptedException
     */
    public <T extends SpecificRecord> void send(T message) throws InterruptedException {

        sequentialSend.put(message);
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
        publisher.cancel(true);
        try {
            if (channel.isOpen()) channel.close();
        } catch (TimeoutException e) {
            logger.error(e);
        }
        if (connection.isOpen()) connection.close();
    }
}
