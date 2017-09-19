package org.janelia.jacsstorage.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

public class StorageClientApp {

    private static abstract class AbstractCommand {
        @Parameter(names = "-localPath", description = "Local path")
        protected String localPath;
        @Parameter(names = "-name", description = "Data bundle name")
        protected String name;
        @Parameter(names = "-dataFormat", description = "Data bundle format")
        protected JacsStorageFormat dataFormat = JacsStorageFormat.DATA_DIRECTORY;
    }

    private static class CommandMain {
        @Parameter(names = "-server", description = "Storage (master) server URL")
        private String serverURL = "http://localhost:8081/jacsstorage/master-api";
    }

    @Parameters(commandDescription = "Send data to the storage server")
    private static class CommandGet extends AbstractCommand {
    }

    @Parameters(commandDescription = "Retrieve data from the storage server")
    private static class CommandPut extends AbstractCommand {
    }

    private final StorageClient storageClient;

    public StorageClientApp(StorageClient storageClient) {
        this.storageClient = storageClient;
    }

    public static void main(String[] args) throws Exception {
        CommandMain cm = new CommandMain();
        CommandGet cmdGet = new CommandGet();
        CommandPut cmdPut = new CommandPut();
        JCommander jc = JCommander.newBuilder()
                .addObject(cm)
                .addCommand("get", cmdGet)
                .addCommand("put", cmdPut)
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
        DataStorageInfo storageInfo;
        StorageClientImpl storageClientImpl = new StorageClientImpl();
        switch (jc.getParsedCommand()) {
            case "get":
                return;
            case "put":
                storageInfo = new DataStorageInfo()
                        .setConnectionInfo(cm.serverURL)
                        .setStorageFormat(cmdPut.dataFormat)
                        .setPath(cmdPut.name);
                storageClientImpl.persistData(cmdPut.localPath, storageInfo);
                return;
            default:
                usage(jc);
        }
    }

    private static void usage(JCommander jc) {
        jc.usage();
        System.exit(1);
    }

}
