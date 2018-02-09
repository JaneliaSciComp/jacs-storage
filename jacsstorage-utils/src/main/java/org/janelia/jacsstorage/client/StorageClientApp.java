package org.janelia.jacsstorage.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.datatransfer.DataTransferService;
import org.janelia.jacsstorage.clientutils.AuthClientImplHelper;
import org.janelia.jacsstorage.clientutils.StorageClientImplHelper;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StorageClientApp {

    private static class CommandMain {
        @Parameter(names = "-server", description = "Storage (master) server URL")
        private String serverURL = "http://localhost:8080/jacsstorage/master_api/v1";
        @Parameter(names = "-authServer", description = "Authentication server URL")
        private String authURL = "https://jacs-dev.int.janelia.org//SCSW/AuthenticationService/v1";
        @Parameter(names = "-username", description = "User name")
        String username;
        @Parameter(names = "-password", description = "User password authentication")
        String password;

        String getUserKey() {
            return "user:" + username;
        }
    }

    private static abstract class AbstractCommand {
        @Parameter(names = "-localPath", description = "Local path")
        String localPath;
        @Parameter(names = "-name", description = "Data bundle name")
        String name;
        @Parameter(names = "-bundleId", description = "bundle id")
        Long bundleId = 0L;

        Long getBundleId() {
            return bundleId != null && bundleId != 0L ? bundleId : null;
        }
    }

    @Parameters(commandDescription = "Send data to the storage server")
    private static class CommandGet extends AbstractCommand {
    }

    @Parameters(commandDescription = "Retrieve data from the storage server")
    private static class CommandPut extends AbstractCommand {
        @Parameter(names = "-dataFormat", description = "Data bundle format")
        private JacsStorageFormat dataFormat = JacsStorageFormat.DATA_DIRECTORY;
    }

    @Parameters(commandDescription = "Create new storage storage content - file or directory")
    private static class CommandCreateNewStorageContent extends AbstractCommand {
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
        CommandCreateNewStorageContent cmdCreateNewContent = new CommandCreateNewStorageContent();
        JCommander jc = JCommander.newBuilder()
                .addObject(cm)
                .addCommand("get", cmdGet)
                .addCommand("put", cmdPut)
                .addCommand("ping", cmdPing)
                .addCommand("createNewDir", cmdCreateNewContent)
                .addCommand("addNewFile", cmdCreateNewContent)
                .build();

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            usage("", jc);
        }
        if (jc.getParsedCommand() == null) {
            usage("", jc);
        }

        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        StorageClient storageClient = new StorageClientHttpImpl(
                container.select(DataTransferService.class).get()
        );
        StorageClientImplHelper storageClientHelper = new StorageClientImplHelper();
        AuthClientImplHelper authClientImplHelper = new AuthClientImplHelper(cm.authURL);
        DataStorageInfo storageInfo;
        String dataOwnerKey = cm.getUserKey();
        switch (jc.getParsedCommand()) {
            case "get":
                storageInfo = new DataStorageInfo()
                        .setNumericId(cmdGet.getBundleId())
                        .setConnectionURL(cm.serverURL)
                        .setOwnerKey(dataOwnerKey)
                        .setName(cmdGet.name);
                storageClient.retrieveData(cmdGet.localPath, storageInfo, authClientImplHelper.authenticate(cm.username, cm.password));
                return;
            case "put":
                storageInfo = new DataStorageInfo()
                        .setConnectionURL(cm.serverURL)
                        .setStorageFormat(cmdPut.dataFormat)
                        .setOwnerKey(dataOwnerKey)
                        .setName(cmdPut.name);
                storageClient.persistData(cmdPut.localPath, storageInfo, authClientImplHelper.authenticate(cm.username, cm.password));
                return;
            case "createNewDir":
                storageClientHelper.createNewDirectory(cm.serverURL, cmdCreateNewContent.bundleId, cmdCreateNewContent.newPath, authClientImplHelper.authenticate(cm.username, cm.password));
                return;
            case "addNewFile":
                String localFileName = cmdCreateNewContent.localPath;
                if (StringUtils.isBlank(localFileName)) {
                    usage("LocalPath - must be specified for adding a new file", jc);
                }
                Path localPath = Paths.get(localFileName);
                if (Files.notExists(localPath)) {
                    usage("LocalPath - " + localFileName + " not found for adding a new file", jc);
                }
                if (Files.isDirectory(localPath)) {
                    usage("LocalPath - " + localFileName + " must be a file for adding a new file", jc);
                }
                storageClientHelper.createNewFile(cm.serverURL, cmdCreateNewContent.bundleId, cmdCreateNewContent.newPath, new FileInputStream(localPath.toFile()), authClientImplHelper.authenticate(cm.username, cm.password));
                return;
            case "ping":
                storageClient.ping(cmdPing.connectionInfo);
                return;
            default:
                usage("", jc);
        }
    }

    private static void usage(String message, JCommander jc) {
        if (StringUtils.isNotBlank(message)) {
            System.err.println(message);
        }
        jc.usage();
        System.exit(1);
    }

}
