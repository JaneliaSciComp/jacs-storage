package org.janelia.jacsstorage.client;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.datatransfer.DataTransferService;
import org.janelia.jacsstorage.clientutils.AuthClientImplHelper;
import org.janelia.jacsstorage.clientutils.StorageClientImplHelper;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        @Parameter(names = "-token", description = "Authentication token - the command accepts either username and password or the authentication token directly")
        String authToken;

        String getUserKey() {
            return StringUtils.isNotBlank(username) ? "user:" + username : "";
        }
    }

    private static abstract class AbstractCommand {
        @Parameter(names = "-localPath", description = "Local path")
        String localPath;
        @Parameter(names = "-name", description = "Data bundle name")
        String name;
        @Parameter(names = "-owner", description = "Data bundle owner key")
        String owner;
        @Parameter(names = "-bundleId", description = "bundle id")
        Long bundleId = 0L;

        Long getBundleId() {
            return bundleId != null && bundleId != 0L ? bundleId : null;
        }

        String getOwnerKey() {
            return StringUtils.isNotBlank(owner) ? "user:" + owner : "";
        }

    }

    @Parameters(commandDescription = "Allocate a data bundle.")
    private static class CommandAllocateBundle extends AbstractCommand {
        @Parameter(names = "-dataFormat", description = "Data bundle format. Allowed values: {DATA_DIRECTORY, ARCHIVE_DATA_FILE, SINGLE_DATA_FILE}")
        private String dataFormat = "DATA_DIRECTORY";
        @Parameter(names = "-storageTags", description = "Storage tags used for selecting the storage device")
        private List<String> storageTags = new ArrayList<>();
        @DynamicParameter(names = "-storageProperty", description = "Storage tags used for selecting the storage device")
        private Map<String, Object> bundleProperties = new HashMap<>();
    }

    @Parameters(commandDescription = "Send data to the storage server")
    private static class CommandGet extends AbstractCommand {
        @Parameter(names = "-entry", description = "Bundle entry name")
        private String entryName;
    }

    @Parameters(commandDescription = "Create folder on the storage server")
    private static class CommandMkdir extends AbstractCommand {
        @Parameter(names = "-entry", description = "Bundle entry name", required = true)
        private String entryName;
    }

    @Parameters(commandDescription = "Send data to the storage server")
    private static class CommandPut extends AbstractCommand {
        @Parameter(names = "-entry", description = "Bundle entry name", required = true)
        private String entryName;
    }

    @Parameters(commandDescription = "Search storage server")
    private static class CommandSearch extends AbstractCommand {
        @Parameter(names = "-storageHost", description = "Search for data bundles only on the specified host")
        private String storageHost;
        @Parameter(names = "-storageTags", description = "Search for data bundles only on the storage that has the given tags")
        private List<String> storageTags = new ArrayList<>();
        @Parameter(names = "-pageNumber", description = "Pagination parameter that specifies the page number to be retrieved")
        private Integer pageNumber = 0;
        @Parameter(names = "-pageSize", description = "Pagination parameter that specifies the number of records to be retrieved")
        private Integer pageSize = 0;
    }

    @Parameters(commandDescription = "List the entries ")
    private static class CommandList extends AbstractCommand {
        @Parameter(names = "-entry", description = "Bundle entry name")
        private String entryName;
    }

    @Parameters(commandDescription = "Ping an agent listener")
    private static class CommandPingAgent extends AbstractCommand {
        @Parameter(names = "-connectionInfo", description = "Connection info", required = true)
        private String connectionInfo;
    }

    public static void main(String[] args) throws Exception {
        CommandMain cmdMain = new CommandMain();
        CommandAllocateBundle cmdAlloc = new CommandAllocateBundle();
        CommandGet cmdGet = new CommandGet();
        CommandList cmdList = new CommandList();
        CommandMkdir cmdMkdir = new CommandMkdir();
        CommandPut cmdPut = new CommandPut();
        CommandSearch cmdSearch = new CommandSearch();
        CommandPingAgent cmdPingAgent = new CommandPingAgent();

        JCommander jc = JCommander.newBuilder()
                .addObject(cmdMain)
                .addCommand("allocate", cmdAlloc)
                .addCommand("get", cmdGet)
                .addCommand("list", cmdList)
                .addCommand("mkdir", cmdMkdir)
                .addCommand("put", cmdPut)
                .addCommand("search", cmdSearch)
                .addCommand("pingAgent", cmdPingAgent)
                .build();

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            usage("", jc);
        }
        if (jc.getParsedCommand() == null) {
            usage("", jc);
        }

        String authToken;
        if (StringUtils.isBlank(cmdMain.authToken) && StringUtils.isNotBlank(cmdMain.username)) {
            AuthClientImplHelper authClientImplHelper = new AuthClientImplHelper(cmdMain.authURL);
            authToken = authClientImplHelper.authenticate(cmdMain.username, cmdMain.password);
        } else {
            authToken = cmdMain.authToken;
        }

        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        StorageClient storageClient = new StorageClientHttpImpl(
                container.select(DataTransferService.class).get()
        );
        StorageClientImplHelper storageClientHelper = new StorageClientImplHelper();
        DataStorageInfo storageInfo;
        switch (jc.getParsedCommand()) {
            case "allocate":
                storageInfo = new DataStorageInfo()
                        .setConnectionURL(cmdMain.serverURL)
                        .setStorageFormat(JacsStorageFormat.valueOf(cmdAlloc.dataFormat))
                        .setStorageTags(cmdAlloc.storageTags)
                        .addMetadata(cmdAlloc.bundleProperties)
                        .setOwnerKey(StringUtils.defaultIfBlank(cmdAlloc.getOwnerKey(), cmdMain.getUserKey()))
                        .setName(cmdAlloc.name);
                if (StringUtils.isBlank(cmdAlloc.localPath)) {
                    // allocate only
                    storageClientHelper.allocateStorage(cmdMain.serverURL, storageInfo, authToken);
                } else {
                    // allocate and copy
                    storageClient.persistData(cmdAlloc.localPath, storageInfo, authToken);
                }
                break;
            case "get":
                storageInfo = new DataStorageInfo()
                        .setConnectionURL(cmdMain.serverURL)
                        .setNumericId(cmdGet.getBundleId())
                        .setOwnerKey(cmdGet.getOwnerKey())
                        .setName(cmdGet.name);
                storageClient.retrieveData(cmdGet.localPath, storageInfo, authToken);
                break;
            case "list":
                storageInfo = new DataStorageInfo()
                        .setNumericId(cmdList.getBundleId())
                        .setName(cmdList.name)
                        .setOwnerKey(cmdList.getOwnerKey());
                List<DataNodeInfo> entryList = storageClientHelper.retrieveStorageInfo(cmdMain.serverURL, storageInfo, authToken)
                        .map(ds -> {
                            return storageClientHelper.listStorageContent(ds.getConnectionURL(), ds.getNumericId(), authToken);
                        })
                        .orElseGet(() -> {
                            return ImmutableList.of();
                        });
                System.out.println(entryList);
                break;
            case "mkdir":
                storageInfo = new DataStorageInfo()
                        .setNumericId(cmdMkdir.getBundleId())
                        .setName(cmdMkdir.name)
                        .setOwnerKey(StringUtils.defaultIfBlank(cmdMkdir.getOwnerKey(), cmdMain.getUserKey()));
                String newFolderURL = storageClientHelper.retrieveStorageInfo(cmdMain.serverURL, storageInfo, authToken)
                        .flatMap(ds -> {
                            return storageClientHelper.createNewDirectory(ds.getConnectionURL(), ds.getNumericId(), cmdMkdir.entryName, authToken);
                        })
                        .orElseGet(() -> {
                            return "<none>";
                        });
                System.out.println("New dir entry URL: " + newFolderURL);
            case "put":
                String localFileName = cmdPut.localPath;
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
                storageInfo = new DataStorageInfo()
                        .setNumericId(cmdMkdir.getBundleId())
                        .setName(cmdMkdir.name)
                        .setOwnerKey(StringUtils.defaultIfBlank(cmdMkdir.getOwnerKey(), cmdMain.getUserKey()));
                String newFileURL = storageClientHelper.retrieveStorageInfo(cmdMain.serverURL, storageInfo, authToken)
                        .flatMap(ds -> {
                            try {
                                return storageClientHelper.createNewFile(ds.getConnectionURL(), ds.getNumericId(), cmdMkdir.entryName, new FileInputStream(localPath.toFile()), authToken);
                            } catch (FileNotFoundException e) {
                                throw new UncheckedIOException(e);
                            }
                        })
                        .orElseGet(() -> {
                            return "<none>";
                        });
                System.out.println("New file entry URL: " + newFileURL);
                break;
            case "search":
                Map<String, Object> searchResponse = storageClientHelper.searchBundles(cmdMain.serverURL,
                        cmdSearch.bundleId, cmdSearch.getOwnerKey(), cmdSearch.name, cmdSearch.storageHost, cmdSearch.storageTags,
                        cmdSearch.pageNumber, cmdSearch.pageSize, authToken);
                System.out.println("Search response: " + searchResponse);
                break;
            case "pingAgent":
                storageClient.ping(cmdPingAgent.connectionInfo);
                break;
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
