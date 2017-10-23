import avro.commands.CreateFileCommand;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.avro.specific.SpecificRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import processes.CommandDispatcher;
import rest.CommandHttpServer;
import sdfs.datanode.DataNode;
import sdfs.namenode.NameNode;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class CommandHttpTest {

    private final static DataNode dataNode1 = new DataNode();
    private final static DataNode dataNode2 = new DataNode();
    private final static NameNode nameNode = new NameNode();
    private final static CommandDispatcher dispatcher = new CommandDispatcher();
    private final static CommandHttpServer server = new CommandHttpServer();

    private final static RandomString randomString = new RandomString();
    private final static String PATH = "C:\\Data\\Test";

    @BeforeClass
    public static void setup() {
        nameNode.bootstrap();
        dataNode1.bootstrap(65112, "Test");
        dataNode2.bootstrap(65113, "Test");
        dispatcher.bootstrap();
        server.bootstrap();
    }

    @AfterClass
    public static void teardown() {
        server.shutdown();
        dispatcher.shutdown();
        dataNode1.shutdown();
        dataNode2.shutdown();
    }

    @Test
    public void assertFileCreation() throws JsonProcessingException, InterruptedException {

        String fileName = createFile();

        TimeUnit.SECONDS.sleep(3);

        Assert.assertTrue(Files.exists(Paths.get(PATH, fileName)));
    }

    @Test
    public void assertFileRemoval() throws InterruptedException {

        String fileName = createFile();

        TimeUnit.SECONDS.sleep(3);

        delete(fileName);

        TimeUnit.SECONDS.sleep(3);

        Assert.assertFalse(Files.exists(Paths.get(PATH, fileName)));
    }



    private String createFile() throws InterruptedException {

        String fileName = randomString.nextString();

        CreateFileCommand cmd = CreateFileCommand.newBuilder()
                .setFile(fileName)
                .setContent("some text...")
                .build();

        post(cmd);
        return fileName;
    }

    private void post(SpecificRecord cmd) throws InterruptedException {

        Client client = Client.create();

        WebResource webResource = client.resource("http://localhost:8080/sdfs/files");

        ClientResponse response = webResource
                .type("application/json")
                .post(ClientResponse.class, cmd.toString());

        Assert.assertTrue(response.getStatus() == 200);

        TimeUnit.SECONDS.sleep(3);
    }

    private void delete(String fileName) throws InterruptedException {

        Client client = Client.create();

        WebResource webResource = client.resource("http://localhost:8080/sdfs/files/" + fileName);

        ClientResponse response = webResource.delete(ClientResponse.class);

        Assert.assertTrue(response.getStatus() == 200);

        TimeUnit.SECONDS.sleep(3);
    }
}