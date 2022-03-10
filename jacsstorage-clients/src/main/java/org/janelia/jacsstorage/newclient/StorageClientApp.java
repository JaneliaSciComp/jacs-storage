package org.janelia.jacsstorage.newclient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class StorageClientApp {

    private static class CommandMain {
        @Parameter(names = "-server", description = "URL of the Master JADE API")
        private String serverURL = "http://localhost:8080/jacsstorage/master_api/v1";
        @Parameter(names = "-apikey", description = "API Key for JADE service")
        private String apiKey;
    }

    @Parameters(commandDescription = "List the contents of the given path")
    private static class CommandList {
        @Parameter(description = "<path>")
        private String path;
    }

    @Parameters(commandDescription = "Read a file at the given path")
    private static class CommandRead {
        @Parameter(description = "<path>")
        private String path;
    }

    @Parameters(commandDescription = "Copy a file from a source location (which may be local or JADE-accessible) to a target location (which is local)")
    private static class CommandCopy {
        @Parameter(description = "<source> <target>", arity = 2)
        private List<String> paths = new ArrayList<>();
    }

    private static JadeStorageService helper;

    public static void main(String[] args) throws Exception {
        CommandMain cmdMain = new CommandMain();
        CommandList cmdList = new CommandList();
        CommandRead cmdRead = new CommandRead();
        CommandCopy cmdCopy = new CommandCopy();

        JCommander jc = JCommander.newBuilder()
                .addObject(cmdMain)
                .addCommand("list", cmdList)
                .addCommand("read", cmdRead)
                .addCommand("copy", cmdCopy)
                .build();

        try {
            jc.parse(args);
        }
        catch (ParameterException e) {
            usage("Error parsing parameters", jc);
        }


        StorageService storageService = new StorageService(cmdMain.serverURL, cmdMain.apiKey);
        helper = new JadeStorageService(storageService, null, null);

        String parsedCommand = jc.getParsedCommand();
        if (parsedCommand == null) {
            usage("Specify a command to execute", jc);
        }
        else {
            switch (parsedCommand) {
                case "list":
                    commandList(jc, cmdMain, cmdList);
                    break;
                case "read":
                    commandRead(jc, cmdMain, cmdRead);
                    break;
                case "copy":
                    commandCopy(jc, cmdMain, cmdCopy);
                    break;
                default:
                    usage("Unsupported command: " + parsedCommand, jc);
            }
        }
    }

    private static void commandList(JCommander jc, CommandMain cmdMain, CommandList args) throws Exception {
        StorageLocation storageLocation = getStorageLocation(args.path);
        helper.getChildren(storageLocation, storageLocation.getRelativePath(args.path)).forEach(storageObject -> {
            System.out.println(storageObject.getSizeBytes()+" bytes - "+storageObject.getObjectName());
        });
    }

    private static void commandRead(JCommander jc, CommandMain cmdMain, CommandRead args) throws Exception {
        StorageLocation storageLocation = getStorageLocation(args.path);
        InputStream content = helper.getContent(storageLocation, storageLocation.getRelativePath(args.path));
        IOUtils.copy(content, System.out);
    }

    private static void commandCopy(JCommander jc, CommandMain cmdMain, CommandCopy args) throws Exception {

        if (args.paths.size() != 2) {
            usage("Extract requires source and target path", jc);
        }

        Path sourcePath = Paths.get(args.paths.get(0));
        Path targetPath = Paths.get(args.paths.get(1));

        if (Files.exists(sourcePath)) {
            PathUtils.copyFiles(sourcePath, targetPath);
        }
        else {
            StorageService storageService = new StorageService(cmdMain.serverURL, cmdMain.apiKey);
            JadeStorageService helper = new JadeStorageService(storageService, null, null);
            StorageLocation storageLocation = getStorageLocation(sourcePath.toString());
            String relativePath = storageLocation.getRelativePath(sourcePath.toString());
            FileUtils.copyInputStreamToFile(helper.getContent(storageLocation, relativePath), targetPath.toFile());
        }
    }

    private static StorageLocation getStorageLocation(String path) {
        StorageLocation storageLocation = helper.getStorageObjectByPath(path);
        if (storageLocation == null) {
            System.err.println("Path not found in JADE: "+path);
            System.exit(1);
        }
        return storageLocation;
    }

    private static void usage(String message, JCommander jc) {
        if (StringUtils.isNotBlank(message)) {
            System.err.println(message);
        }
        jc.usage();
        System.exit(1);
    }

}
