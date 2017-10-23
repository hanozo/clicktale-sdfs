package rest.domain;

import avro.commands.MakeDirCommand;
import avro.commands.RemoveDirCommand;
import avro.commands.RenameDirCommand;
import mq.MQRPCClient;

import java.io.Closeable;
import java.io.IOException;

/**
 * Schedule directory commands through MQ
 */
public class DirDomain implements Closeable {

    private final MQRPCClient client;

    public DirDomain(MQRPCClient client) {

        this.client = client;
    }

    public void make(final MakeDirCommand cmd) throws IOException, InterruptedException {
        client.call(cmd);
    }

    public void remove(final RemoveDirCommand cmd) throws IOException, InterruptedException {
        client.call(cmd);
    }

    public void rename(final RenameDirCommand cmd) throws IOException, InterruptedException {
        client.call(cmd);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
