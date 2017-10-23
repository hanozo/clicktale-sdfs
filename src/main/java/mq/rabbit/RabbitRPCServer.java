package mq.rabbit;

import avro.AvroJsonDecoder;
import avro.AvroJsonEncoder;
import avro.commands.BareResponse;
import com.rabbitmq.client.*;
import mq.MQRPCServer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.*;

public class RabbitRPCServer implements MQRPCServer {

    private static final Logger logger = LogManager.getLogger();
    private static final String RPC_COMMANDS_QUEUE = "commands";

    private final AvroJsonDecoder decoder = new AvroJsonDecoder();
    private final AvroJsonEncoder encoder = new AvroJsonEncoder();

    private final Connection connection;
    private final Channel channel;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private Future<?> future;

    public RabbitRPCServer(String host) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public void process(java.util.function.Function<SpecificRecord, BareResponse> handler) {

        future = executor.submit(() ->
        {
            try {

                channel.queueDeclare(RPC_COMMANDS_QUEUE, false, false, false, null);

                channel.basicQos(1);

                logger.info(" [x] Awaiting RPC requests");

                Consumer consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                                .Builder()
                                .correlationId(properties.getCorrelationId())
                                .build();

                        try {
                            SpecificRecord cmd = decoder.deserialize(body);
                            BareResponse response = handler.apply(cmd);
                            channel.basicPublish("", properties.getReplyTo(), replyProps, encoder.encode(response));
                        } catch (RuntimeException e) {
                            System.out.println(" [.] " + e.toString());
                        } finally {
                            channel.basicAck(envelope.getDeliveryTag(), false);
                            // RabbitMq consumer worker thread notifies the RPC server owner thread
                            synchronized (this) {
                                this.notify();
                            }
                        }
                    }
                };

                channel.basicConsume(RPC_COMMANDS_QUEUE, false, consumer);
                // Wait and be prepared to consume the message from RPC client.
                while (!Thread.currentThread().isInterrupted()) {
                    //noinspection SynchronizationOnLocalVariableOrMethodParameter
                    synchronized (consumer) {
                        try {
                            consumer.wait();
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }

            } catch (IOException e) {
                logger.error(e);
            } finally {
                if (connection != null)
                    try {
                        connection.close();
                    } catch (IOException ignore) {
                    }
            }
        });
    }

    @Override
    public void close() throws IOException {
        try {
            future.cancel(true);
        } catch (CancellationException ignore) {

        }

        executor.shutdown();
    }
}
