package rest.domain;

import avro.commands.BareResponse;
import avro.commands.MakeDirCommand;
import avro.commands.RemoveDirCommand;
import avro.commands.RenameDirCommand;
import mq.MQRPCClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

/**
 * Schedule directory commands through MQ
 */
public class DirDomain implements Closeable {

    private final MQRPCClient client;

    public DirDomain(MQRPCClient client) {

        this.client = client;
    }

    public Future<BareResponse> make(final MakeDirCommand cmd) throws IOException, InterruptedException {
        return client.call(cmd);
    }

    public Future<BareResponse> remove(final RemoveDirCommand cmd) throws IOException, InterruptedException {
        return client.call(cmd);
    }

    public Future<BareResponse> rename(final RenameDirCommand cmd) throws IOException, InterruptedException {
        return client.call(cmd);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
