package org.janelia.jacsstorage.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.service.StorageService;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

public class StorageClientApp {

    private static abstract class AbstractCommand {
        @Parameter(names = "-localPath", description = "Local path")
        protected String localPath;
        @Parameter(names = "-owner", description = "Data bundle owner")
        protected String owner;
        @Parameter(names = "-name", description = "Data bundle name")
        protected String name;
    }

    private static class CommandMain {
        @Parameter(names = "-server", description = "Storage (master) server URL")
        private String serverURL = "http://localhost:8080/jacsstorage/master-api";
    }

    @Parameters(commandDescription = "Send data to the storage server")
    private static class CommandGet extends AbstractCommand {
    }

    @Parameters(commandDescription = "Retrieve data from the storage server")
    private static class CommandPut extends AbstractCommand {
        @Parameter(names = "-dataFormat", description = "Data bundle format")
        private JacsStorageFormat dataFormat = JacsStorageFormat.DATA_DIRECTORY;
    }

    @Parameters(commandDescription = "Ping an agent listener")
    private static class CommandPing extends AbstractCommand {
        @Parameter(names = "-connectionInfo", description = "Connection info", required = true)
        private String connectionInfo;
    }

    private final StorageClient storageClient;

    public StorageClientApp(StorageClient storageClient) {
        this.storageClient = storageClient;
    }

    public static void main(String[] args) throws Exception {
        CommandMain cm = new CommandMain();
        CommandGet cmdGet = new CommandGet();
        CommandPut cmdPut = new CommandPut();
        CommandPing cmdPing = new CommandPing();
        JCommander jc = JCommander.newBuilder()
                .addObject(cm)
                .addCommand("get", cmdGet)
                .addCommand("put", cmdPut)
                .addCommand("ping", cmdPing)
                .build();

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            usage(jc);
        }
        if (jc.getParsedCommand() == null) {
            usage(jc);
        }

        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        StorageClient socketStorageClient = new SocketStorageClient(
                container.select(StorageService.class).get()
        );
        DataStorageInfo storageInfo;
        StorageClientImpl storageClientImpl = new StorageClientImpl(socketStorageClient);
        switch (jc.getParsedCommand()) {
            case "get":
                storageInfo = new DataStorageInfo()
                        .setConnectionInfo(cm.serverURL)
                        .setOwner(cmdGet.owner)
                        .setName(cmdGet.name);
                storageClientImpl.retrieveData(cmdGet.localPath, storageInfo);
                return;
            case "put":
                storageInfo = new DataStorageInfo()
                        .setConnectionInfo(cm.serverURL)
                        .setStorageFormat(cmdPut.dataFormat)
                        .setOwner(cmdPut.owner)
                        .setName(cmdPut.name);
                storageClientImpl.persistData(cmdPut.localPath, storageInfo);
                return;
            case "ping":
                storageClientImpl.ping(cmdPing.connectionInfo);
            default:
                usage(jc);
        }
    }

    private static void usage(JCommander jc) {
        jc.usage();
        System.exit(1);
    }

}
