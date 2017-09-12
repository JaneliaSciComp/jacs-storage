package org.janelia.jacsstorage.service;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.janelia.jacsstorage.cdi.ApplicationProducer;
import org.janelia.jacsstorage.dao.Dao;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

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

        SeContainerInitializer containerInit = SeContainerInitializer.newInstance()
                .disableDiscovery()
                .addPackages(true,
                        Dao.class,
                        StorageClientLauncher.class,
                        DataBundleIOProvider.class,
                        ApplicationProducer.class
                )
                ;
        try (SeContainer container = containerInit.initialize()) {
            StorageClient socketStorageClient = new SocketStorageClient(
                    container.select(StorageAgent.class).get(),
                    container.select(StorageAgent.class).get(),
                    cm.serverIP,
                    cm.serverPortNo
            );
            StorageClientLauncher storageClientLauncher = new StorageClientLauncher(socketStorageClient);
            switch (jc.getParsedCommand()) {
                case "get":
                    storageClientLauncher.storageClient.retrieveData(cmdGet.localPath, cmdGet.remotePath, cmdGet.dataFormat);
                    return;
                case "put":
                    storageClientLauncher.storageClient.persistData(cmdPut.localPath, cmdPut.remotePath, cmdPut.dataFormat);
                    return;
                default:
                    usage(jc);
            }
        }
    }

    private static void usage(JCommander jc) {
        jc.usage();
        System.exit(1);
    }

}
