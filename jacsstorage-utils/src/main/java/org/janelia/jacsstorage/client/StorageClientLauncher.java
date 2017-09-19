package org.janelia.jacsstorage.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.protocol.StorageProtocol;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

public class StorageClientLauncher {

    private static abstract class AbstractCommand {
        @Parameter(names = "-localPath", description = "Local path")
        protected String localPath;
        @Parameter(names = "-remotePath", description = "Remote path")
        protected String remotePath;
        @Parameter(names = "-dataFormat", description = "Data bundle format", required = true)
        protected JacsStorageFormat dataFormat;
    }

    private static class CommandMain {
        @Parameter(names = "-serverIP", description = "Server IP address")
        private String serverIP = "localhost";
        @Parameter(names = "-serverPort", description = "Server port number")
        private int serverPortNo = 10000;
    }

    @Parameters(commandDescription = "Send data to the storage server")
    private static class CommandGet extends AbstractCommand {
    }

    @Parameters(commandDescription = "Retrieve data from the storage server")
    private static class CommandPut extends AbstractCommand {
    }

    private final StorageClient storageClient;

    public StorageClientLauncher(StorageClient storageClient) {
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
        StorageClient socketStorageClient = new SocketStorageClient(
                container.select(StorageProtocol.class).get(),
                container.select(StorageProtocol.class).get()
        );
        DataStorageInfo storageInfo;
        StorageClientLauncher storageClientLauncher = new StorageClientLauncher(socketStorageClient);
        switch (jc.getParsedCommand()) {
            case "get":
                storageInfo = new DataStorageInfo()
                        .setStorageFormat(cmdGet.dataFormat)
                        .setPath(cmdGet.remotePath);
                storageClientLauncher.storageClient.retrieveData(cmdGet.localPath, storageInfo);
                return;
            case "put":
                storageInfo = new DataStorageInfo()
                        .setStorageFormat(cmdPut.dataFormat)
                        .setPath(cmdPut.remotePath);
                storageClientLauncher.storageClient.persistData(cmdPut.localPath, storageInfo);
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
