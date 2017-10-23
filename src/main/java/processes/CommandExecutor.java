package processes;

import avro.commands.*;
import avro.namenode.DataNodeInfo;
import avro.namenode.NameNodeRPC;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;

public class CommandExecutor {

    private static final Logger logger = LogManager.getLogger();
    private final String namenodeHost = Optional.ofNullable(System.getenv("NAME_NODE")).orElse(InetAddress.getLocalHost().getHostName());

    public CommandExecutor() throws UnknownHostException {
    }


    /**
     * There is no native polymorphism avro support hence ruining the command design pattern
     *
     * @param cmd The actual dir/file command.
     * @return boolean/path feedback
     * @throws Curse Thrown when namenode connection can't be established or not enough replications are available to fulfill the request.
     */
    Object execute(SpecificRecord cmd) throws Curse {

        List<DataNodeInfo> nodes;

        try (NettyTransceiver netty = new NettyTransceiver(new InetSocketAddress(namenodeHost, 65111))) {

            NameNodeRPC proxy = SpecificRequestor.getClient(NameNodeRPC.class, netty);

            nodes = proxy.askForNodes(getPath(cmd));

            logger.info("askForNodes replied with: " + nodes);

        } catch (IOException e) {

            throw new Curse(e);
        }

        if (nodes == null || nodes.size() <= 1)
            throw Curse.newBuilder().setMessage$("At least two replications are required to satisfy any request").build();

        Object response = null;

        for (DataNodeInfo node : nodes) {

            String host = node.getAddress();

            try (NettyTransceiver netty = new NettyTransceiver(new InetSocketAddress(host, node.getPort()))) {

                DataNodeRPC proxy = SpecificRequestor.getClient(DataNodeRPC.class, netty);

                if (cmd instanceof MakeDirCommand) {
                    response = proxy.makeDir((MakeDirCommand) cmd);
                } else if (cmd instanceof RemoveDirCommand) {
                    response = proxy.removeDir((RemoveDirCommand) cmd);
                } else if (cmd instanceof RenameDirCommand) {
                    response = proxy.renameDir((RenameDirCommand) cmd);
                } else if (cmd instanceof CreateFileCommand) {
                    response = proxy.createFile((CreateFileCommand) cmd);
                } else if (cmd instanceof RemoveFileCommand) {
                    response = proxy.removeFile((RemoveFileCommand) cmd);
                } else if (cmd instanceof UpdateFileCommand) {
                    response = proxy.updateFile((UpdateFileCommand) cmd);
                }
            } catch (Exception e) {
                logger.error(e);
                // replication retry
            }
        }

        if (response == null) throw Curse.newBuilder().setMessage$("Command failed.").build();

        return response;
    }

    private String getPath(SpecificRecord cmd) {

        if (cmd instanceof MakeDirCommand) {
            return ((MakeDirCommand) cmd).getPath();
        } else if (cmd instanceof RemoveDirCommand) {
            return ((RemoveDirCommand) cmd).getPath();
        } else if (cmd instanceof RenameDirCommand) {
            return ((RenameDirCommand) cmd).getOldName();
        } else if (cmd instanceof CreateFileCommand) {
            return ((CreateFileCommand) cmd).getFile();
        } else if (cmd instanceof RemoveFileCommand) {
            return ((RemoveFileCommand) cmd).getFile();
        } else {
            return ((UpdateFileCommand) cmd).getFile();
        }
    }
}
