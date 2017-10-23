package rest.domain;

import avro.commands.BareResponse;
import avro.commands.CreateFileCommand;
import avro.commands.RemoveFileCommand;
import avro.commands.UpdateFileCommand;
import mq.MQRPCClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

/**
 * Schedule file commands through MQ
 */
public class FileDomain implements Closeable {

    private final MQRPCClient client;

    public FileDomain(MQRPCClient client) {

        this.client = client;
    }

    public Future<BareResponse> create(final CreateFileCommand cmd) throws IOException, InterruptedException {
        return client.call(cmd);
    }

    public Future<BareResponse> remove(final RemoveFileCommand cmd) throws IOException, InterruptedException {
        return client.call(cmd);
    }

    public Future<BareResponse> update(final UpdateFileCommand cmd) throws IOException, InterruptedException {
        return client.call(cmd);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
