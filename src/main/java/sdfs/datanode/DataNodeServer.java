package sdfs.datanode;

import avro.commands.*;
import org.apache.avro.AvroRemoteException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

class DataNodeServer implements DataNodeRPC {

    private static final Logger logger = LogManager.getLogger();

    static final String DATA = Optional.ofNullable(System.getenv("DATA")).orElse("/home/alonhe/sdfs");
    private final Path path;

    DataNodeServer(String perNodePath) throws IOException {

        this.path = Paths.get(DATA, perNodePath);
    }

    @Override
    public String makeDir(MakeDirCommand command) throws Curse {

        try {

            Path dir = this.path.resolve(command.getPath());

            Files.createDirectories(dir);

        } catch (IOException e) {
            logger.error(e);
            throw new Curse(e);
        }

        return command.getPath();
    }

    @Override
    public boolean removeDir(RemoveDirCommand command) throws Curse {

        try {

            Path path = this.path.resolve(command.getPath());

            return Files.deleteIfExists(path);

        } catch (IOException e) {
            logger.error(e);
            throw new Curse(e);
        }
    }

    @Override
    public String renameDir(RenameDirCommand command) throws Curse {

        try {

            Path src = this.path.resolve(command.getOldName());

            Path trg = this.path.resolve(command.getNewName());

            Files.move(src, trg);

        } catch (IOException e) {
            logger.error(e);
            throw new Curse(e);
        }

        return command.getNewName();
    }

    @Override
    public String createFile(CreateFileCommand command) throws Curse {

        try {

            Path path = this.path.resolve(command.getFile());

            Files.createDirectories(path.getParent());

            Files.write(path, command.getContent().getBytes(), StandardOpenOption.CREATE);

        } catch (IOException e) {
            logger.error(e);
            throw new Curse(e);
        }

        return command.getFile();
    }

    @Override
    public boolean removeFile(RemoveFileCommand command) throws Curse {

        try {

            Path path = this.path.resolve(command.getFile());

            return Files.deleteIfExists(path);

        } catch (IOException e) {
            logger.error(e);
            throw new Curse(e);
        }
    }

    @Override
    public String updateFile(UpdateFileCommand command) throws Curse {

        try {

            Path path = this.path.resolve(command.getFile());

            Files.write(path, command.getContent().getBytes(), StandardOpenOption.APPEND);

        } catch (IOException e) {
            logger.error(e);
            throw new Curse(e);
        }

        return command.getFile();
    }
}
