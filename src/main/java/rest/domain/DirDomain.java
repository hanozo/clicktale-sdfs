package rest.domain;

import avro.commands.*;
import mq.MQProducer;

import java.io.Closeable;
import java.io.IOException;

/**
 * Schedule directory commands through MQ
 */
public class DirDomain implements Closeable {

    private final MQProducer producer;

    public DirDomain(MQProducer producer) {

        this.producer = producer;
    }

    public void make(final MakeDirCommand cmd) throws InterruptedException {
        producer.send(cmd);
    }

    public void remove(final RemoveDirCommand cmd) throws InterruptedException {
        producer.send(cmd);
    }

    public void rename(final RenameDirCommand cmd) throws InterruptedException {
        producer.send(cmd);
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
