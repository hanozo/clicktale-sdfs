package rest.domain;

import avro.commands.CreateFileCommand;
import avro.commands.RemoveFileCommand;
import avro.commands.UpdateFileCommand;
import mq.MQRPCClient;

import java.io.Closeable;
import java.io.IOException;

/**
 * Schedule file commands through MQ
 */
public class FileDomain implements Closeable {

    private final MQRPCClient client;

    public FileDomain(MQRPCClient client) {

        this.client = client;
    }

    public void create(final CreateFileCommand cmd) throws IOException, InterruptedException {
        client.call(cmd);
    }

    public void remove(final RemoveFileCommand cmd) throws IOException, InterruptedException {
        client.call(cmd);
    }

    public void update(final UpdateFileCommand cmd) throws IOException, InterruptedException {
        client.call(cmd);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
