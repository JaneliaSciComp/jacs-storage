package org.janelia.jacsstorage.example;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class StorageExampleClient {

    private static class CommandMain {
        @Parameter(names = "-server", description = "Storage (master) server URL")
        private String serverURL = "http://localhost:8880/jacsstorage/master_api/v1";
        @Parameter(names = "-authServer", description = "Authentication server URL")
        private String authURL = "https://jacs-dev.int.janelia.org//SCSW/AuthenticationService/v1";
        @Parameter(names = "-username", description = "User name")
        String username;
        @Parameter(names = "-password", description = "User password authentication")
        String password;
        @Parameter(names = "-token", description = "Authentication token - the command accepts either username and password or the authentication token directly")
        String authToken;

        String getUserKey() {
            return StringUtils.isNotBlank(username) ? "user:" + username : null;
        }
    }

    private static abstract class AbstractCommand {
        @Parameter(names = "-localPath", description = "Local path")
        String localPath;
        @Parameter(names = "-name", description = "Entry name relative to the data bundle")
        String name;
        @Parameter(names = "-bundleId", description = "data bundle id")
        Long bundleId = 0L;

        Long getBundleId() {
            return bundleId != null && bundleId != 0L ? bundleId : null;
        }
    }

    @Parameters(commandDescription = "Allocate a data bundle.")
    private static class CommandAllocateBundle extends AbstractCommand {
        @Parameter(names = "-dataFormat", description = "Data bundle format. Allowed values: {DATA_DIRECTORY, ARCHIVE_DATA_FILE, SINGLE_DATA_FILE}")
        private String dataFormat = "DATA_DIRECTORY";
        @Parameter(names = "-storageTags", description = "Storage tags used for selecting the storage device")
        private List<String> storageTags = new ArrayList<>();
        @DynamicParameter(names = "-storageProperty", description = "Storage tags used for selecting the storage device")
        private Map<String, String> bundleProperties = new HashMap<>();
    }

    @Parameters(commandDescription = "Send data to the storage server")
    private static class CommandGet extends AbstractCommand {
        @Parameter(names = "-entry", description = "Bundle entry name")
        private String entryName;
    }

    @Parameters(commandDescription = "Create folder on the storage server")
    private static class CommandMkdir extends AbstractCommand {
        @Parameter(names = "-entry", description = "Bundle entry name")
        private String entryName;
    }

    @Parameters(commandDescription = "Send data to the storage server")
    private static class CommandPut extends AbstractCommand {
        @Parameter(names = "-entry", description = "Bundle entry name")
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
    }

    public static void main(String[] args) throws Exception {
        CommandMain cmdMain = new CommandMain();
        CommandAllocateBundle cmdAlloc = new CommandAllocateBundle();
        CommandGet cmdGet = new CommandGet();
        CommandList cmdList = new CommandList();
        CommandMkdir cmdMkdir = new CommandMkdir();
        CommandPut cmdPut = new CommandPut();
        CommandSearch cmdSearch = new CommandSearch();

        JCommander jc = JCommander.newBuilder()
                .addObject(cmdMain)
                .addCommand("allocate", cmdAlloc)
                .addCommand("get", cmdGet)
                .addCommand("list", cmdList)
                .addCommand("mkdir", cmdMkdir)
                .addCommand("put", cmdPut)
                .addCommand("search", cmdSearch)
                .build();

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            usage("", jc);
        }
        if (jc.getParsedCommand() == null) {
            usage("", jc);
        }

        String authToken = StringUtils.defaultIfBlank(
                cmdMain.authToken,
                StorageClientUtils.authenticate(cmdMain.authURL, cmdMain.username, cmdMain.password)
        );
        if (StringUtils.isBlank(authToken)) {
            usage("Invalid authentication token", jc);
        }
        System.out.println("Authentication token: " + authToken);
        String storageURL;
        Map<String, Object> response;
        switch (jc.getParsedCommand()) {
            case "allocate":
                 response = StorageClientUtils.allocateBundle(cmdMain.serverURL,
                        cmdAlloc.name, cmdAlloc.dataFormat,
                        cmdAlloc.storageTags, cmdAlloc.bundleProperties,
                        authToken);
                System.out.println("Allocate response: " + response);
                break;
            case "get":
                response = StorageClientUtils.getBundleInfo(cmdMain.serverURL, cmdGet.bundleId, cmdMain.getUserKey(), cmdGet.name, authToken);
                storageURL = (String) response.get("connectionURL");
                if (StringUtils.isBlank(storageURL)) {
                    System.out.println("Bundle info response: " + response);
                } else {
                    Number bundleId = new BigInteger((String) response.get("id"));
                    InputStream stream = StorageClientUtils.streamDataEntryFromBundle(storageURL, bundleId, cmdGet.entryName, authToken);
                    if (stream == null) {
                        System.out.println("Error retrieving the entry content for " + cmdGet.entryName);
                    } else if (StringUtils.isNotBlank(cmdGet.localPath)) {
                        Files.copy(stream, Paths.get(cmdGet.localPath));
                    } else {
                        ByteStreams.copy(stream, ByteStreams.nullOutputStream());
                    }
                }
                break;
            case "list":
                response = StorageClientUtils.getBundleInfo(cmdMain.serverURL, cmdList.bundleId, cmdMain.getUserKey(), cmdList.name, authToken);
                storageURL = (String) response.get("connectionURL");
                if (StringUtils.isBlank(storageURL)) {
                    System.out.println("Bundle info response: " + response);
                } else {
                    Number bundleId = new BigInteger((String) response.get("id"));
                    response = StorageClientUtils.listBundleContent(storageURL, bundleId, authToken);
                    System.out.println("List Bundle content response: " + response);
                }
                break;
            case "mkdir":
                response = StorageClientUtils.getBundleInfo(cmdMain.serverURL, cmdMkdir.bundleId, cmdMain.getUserKey(), cmdMkdir.name, authToken);
                storageURL = (String) response.get("connectionURL");
                if (StringUtils.isBlank(storageURL)) {
                    System.out.println("Bundle info response: " + response);
                } else {
                    Number bundleId = new BigInteger((String) response.get("id"));
                    String newFolderURL = StorageClientUtils.addNewBundleFolder(storageURL, bundleId, cmdMkdir.entryName, authToken);
                    System.out.println("New dir entry URL: " + newFolderURL);
                }
                break;
            case "put":
                response = StorageClientUtils.getBundleInfo(cmdMain.serverURL, cmdPut.bundleId, cmdMain.getUserKey(), cmdPut.name, authToken);
                storageURL = (String) response.get("connectionURL");
                if (StringUtils.isBlank(storageURL)) {
                    System.out.println("Bundle info response: " + response);
                } else {
                    Number bundleId = new BigInteger((String) response.get("id"));
                    String newFileURL = StorageClientUtils.addNewBundleFile(storageURL, bundleId, cmdPut.entryName, new FileInputStream(cmdPut.localPath), authToken);
                    System.out.println("New file entry URL: " + newFileURL);
                }
                break;
            case "search":
                response = StorageClientUtils.searchBundles(cmdMain.serverURL,
                        cmdSearch.bundleId, cmdMain.getUserKey(), cmdSearch.name, cmdSearch.storageHost, cmdSearch.storageTags,
                        cmdSearch.pageNumber, cmdSearch.pageSize, authToken);
                System.out.println("Search response: " + response);
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
