package mq.rabbit;

import avro.AvroJsonDecoder;
import avro.AvroJsonEncoder;
import avro.commands.BareResponse;
import com.rabbitmq.client.*;
import mq.MQRPCClient;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * Please serialize producer access single thread at the time.
 */
public class RabbitRPCClient implements MQRPCClient {

    private static final Logger logger = LogManager.getLogger();
    private static final String RPC_COMMANDS_QUEUE = "commands";
    private final String replyQueueName;
    private static Map<String, CompletableFuture<BareResponse>> map = new ConcurrentHashMap<>();

    private final AvroJsonEncoder encoder = new AvroJsonEncoder();
    private final AvroJsonDecoder decoder = new AvroJsonDecoder();

    private final Connection connection;
    private final Channel channel;

    public RabbitRPCClient(final String host) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        connection = factory.newConnection();
        channel = connection.createChannel();
        replyQueueName = channel.queueDeclare().getQueue();

        channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                CompletableFuture<BareResponse> future = map.remove(properties.getCorrelationId());

                if (future != null) {
                    BareResponse response = decoder.deserialize(BareResponse.class, body);
                    if (response.getSucceeded())
                        future.complete(response);
                    else
                        future.completeExceptionally(new Exception(response.getFeedback()));
                }
            }
        });
    }

    public <T extends SpecificRecord> CompletableFuture<BareResponse> call(T request) throws IOException, InterruptedException {

        String corrId = UUID.randomUUID().toString();

        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", RPC_COMMANDS_QUEUE, props, encoder.serialize(request));

        CompletableFuture<BareResponse> future = new CompletableFuture<>();

        map.put(corrId, future);

        return future;
    }


    @Override
    public void close() throws IOException {
        try {
            if (channel != null && channel.isOpen()) channel.close();
        } catch (TimeoutException e) {
            logger.error(e);
        }
        if (connection != null && connection.isOpen()) connection.close();
    }
}
