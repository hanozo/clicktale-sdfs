package sdfs.datanode;

import avro.commands.*;
import avro.namenode.NameNodeRPC;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/**
 * Created by user on 01/10/2017.
 */
class DataNodeServer implements DataNodeRPC, Closeable {

    private static final Logger logger = LogManager.getLogger();

    static final String DATA = Optional.ofNullable(System.getenv("DATA")).orElse("C:\\Data");
    String nameNodeHost = Optional.ofNullable(System.getenv("NAME_NODE")).orElse(InetAddress.getLocalHost().getHostName());
    private final Path path;

    private final NettyTransceiver netty;
    private final NameNodeRPC proxy;

    public DataNodeServer(String perNodePath) throws IOException {

        this.path = Paths.get(DATA, perNodePath);

        netty = new NettyTransceiver(new InetSocketAddress(nameNodeHost, 65111));

        proxy = SpecificRequestor.getClient(NameNodeRPC.class, netty);
    }

    @Override
    public Void makeDir(MakeDirCommand command) throws AvroRemoteException {

        try {

            Path dir = this.path.resolve(command.getPath().toString());

            Files.createDirectories(dir);

        } catch (IOException e) {
            logger.error(e);
            throw new AvroRemoteException(e);
        }

        return null;
    }

    @Override
    public boolean removeDir(RemoveDirCommand command) throws AvroRemoteException {

        try {

            Path path = this.path.resolve(command.getPath().toString());

            return Files.deleteIfExists(path);

        } catch (IOException e) {
            logger.error(e);
            throw new AvroRemoteException(e);
        }
    }

    @Override
    public Void renameDir(RenameDirCommand command) throws AvroRemoteException {

        try {

            Path src = this.path.resolve(command.getOldName().toString());

            Path trg = this.path.resolve(command.getNewName().toString());

            Files.move(src, trg);

        } catch (IOException e) {
            logger.error(e);
            throw new AvroRemoteException(e);
        }

        return null;
    }

    @Override
    public Void createFile(CreateFileCommand command) throws AvroRemoteException {

        try {

            Path path = this.path.resolve(command.getFile().toString());

            Files.createDirectories(path.getParent());

            Files.write(path, command.getContent().toString().getBytes(), StandardOpenOption.CREATE);

        } catch (IOException e) {
            logger.error(e);
            throw new AvroRemoteException(e);
        }

        return null;
    }

    @Override
    public boolean removeFile(RemoveFileCommand command) throws AvroRemoteException {

        try {

            Path path = this.path.resolve(command.getFile().toString());

            return Files.deleteIfExists(path);

        } catch (IOException e) {
            logger.error(e);
            throw new AvroRemoteException(e);
        }
    }

    @Override
    public Void updateFile(UpdateFileCommand command) throws AvroRemoteException {

        try {

            Path path = this.path.resolve(command.getFile().toString());

            Files.write(path, command.getContent().toString().getBytes(), StandardOpenOption.APPEND);

        } catch (IOException e) {
            logger.error(e);
            throw new AvroRemoteException(e);
        }

        return null;
    }

    @Override
    public void close() throws IOException {
        netty.close();
    }
}
