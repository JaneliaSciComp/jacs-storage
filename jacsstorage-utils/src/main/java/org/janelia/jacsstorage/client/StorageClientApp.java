package org.janelia.jacsstorage.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.service.DataTransferService;
import org.janelia.jacsstorage.utils.StorageClientImplHelper;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

public class StorageClientApp {

    private static abstract class AbstractCommand {
        @Parameter(names = "-localPath", description = "Local path")
        protected String localPath;
        @Parameter(names = "-name", description = "Data bundle name")
        protected String name;
    }

    private static class CommandMain {
        @Parameter(names = "-server", description = "Storage (master) server URL")
        private String serverURL = "http://localhost:8080/jacsstorage/master-api";
        @Parameter(names = "-useHttp", description = "Use Http to persist/retrieve data")
        private Boolean useHttp = false;
        @Parameter(names = "-username", description = "User name")
        protected String username;
        @Parameter(names = "-password", description = "User password authentication")
        protected String password;
    }

    @Parameters(commandDescription = "Send data to the storage server")
    private static class CommandGet extends AbstractCommand {
    }

    @Parameters(commandDescription = "Retrieve data from the storage server")
    private static class CommandPut extends AbstractCommand {
        @Parameter(names = "-dataFormat", description = "Data bundle format")
        private JacsStorageFormat dataFormat = JacsStorageFormat.DATA_DIRECTORY;
    }

    @Parameters(commandDescription = "Create new storage storage directory")
    private static class CommandCreateNewStorageDir extends AbstractCommand {
        @Parameter(names = "-bundleId", description = "bundle id", required = true)
        private Long bundleId;
        @Parameter(names = "-newPath", description = "directory path relative to the data bundle", required = true)
        private String newPath;
    }

    @Parameters(commandDescription = "Ping an agent listener")
    private static class CommandPing extends AbstractCommand {
        @Parameter(names = "-connectionInfo", description = "Connection info", required = true)
        private String connectionInfo;
    }

    public static void main(String[] args) throws Exception {
        CommandMain cm = new CommandMain();
        CommandGet cmdGet = new CommandGet();
        CommandPut cmdPut = new CommandPut();
        CommandPing cmdPing = new CommandPing();
        CommandCreateNewStorageDir cmdCreateNewDir = new CommandCreateNewStorageDir();
        JCommander jc = JCommander.newBuilder()
                .addObject(cm)
                .addCommand("get", cmdGet)
                .addCommand("put", cmdPut)
                .addCommand("ping", cmdPing)
                .addCommand("createNewDir", cmdCreateNewDir)
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
        StorageClient storageClient;
        if (cm.useHttp) {
            storageClient = new StorageClientHttpImpl(
                    container.select(DataTransferService.class).get()
            );
        } else {
            storageClient = new StorageClientImpl(new SocketStorageClient(
                    container.select(DataTransferService.class).get())
            );
        }
        StorageClientImplHelper storageClientHelper = new StorageClientImplHelper();
        DataStorageInfo storageInfo;
        String dataOwner = cm.username;
        switch (jc.getParsedCommand()) {
            case "get":
                storageInfo = new DataStorageInfo()
                        .setConnectionURL(cm.serverURL)
                        .setOwner(dataOwner)
                        .setName(cmdGet.name);
                storageClient.retrieveData(cmdGet.localPath, storageInfo, storageClientHelper.authenticate(cm.username, cm.password));
                return;
            case "put":
                storageInfo = new DataStorageInfo()
                        .setConnectionURL(cm.serverURL)
                        .setStorageFormat(cmdPut.dataFormat)
                        .setOwner(dataOwner)
                        .setName(cmdPut.name);
                storageClient.persistData(cmdPut.localPath, storageInfo, storageClientHelper.authenticate(cm.username, cm.password));
                return;
            case "createNewDir":
                storageClientHelper.createNewDirectory(cmdCreateNewDir.bundleId, cmdCreateNewDir.newPath, storageClientHelper.authenticate(cm.username, cm.password));
                return;
            case "ping":
                storageClient.ping(cmdPing.connectionInfo);
            default:
                usage(jc);
        }
    }

    private static void usage(JCommander jc) {
        jc.usage();
        System.exit(1);
    }

}
