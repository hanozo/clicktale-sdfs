package rest.domain;

import avro.commands.*;
import mq.MQProducer;

import java.io.Closeable;
import java.io.IOException;

/**
 * Schedule file commands through MQ
 */
public class FileDomain implements Closeable {

    private final MQProducer producer;

    public FileDomain(MQProducer producer) {

        this.producer = producer;
    }

    public void create(final CreateFileCommand cmd) throws InterruptedException {
        producer.send(cmd);
    }

    public void remove(final RemoveFileCommand cmd) throws InterruptedException {
        producer.send(cmd);
    }

    public void update(final UpdateFileCommand cmd) throws InterruptedException {
        producer.send(cmd);
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
