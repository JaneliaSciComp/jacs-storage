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
import java.util.Comparator;
import java.util.List;

public class StorageClientApp {

    private static class CommandMain {
        @Parameter(names = {"-s","--server"}, description = "URL of the Master JADE API")
        private String serverURL = "http://localhost:8080/jacsstorage/master_api/v1";
        @Parameter(names = {"-k","--key"}, description = "API Key for JADE service")
        private String apiKey;
    }

    @Parameters(commandDescription = "Recursively list the descendants of the given path to a specified depth. By default, only the immediate children are listed.")
    private static class CommandList {
        @Parameter(description = "<path>")
        private String path;
        @Parameter(names = {"-d","--depth"}, description = "Depth of tree to list")
        private int depth = 1;
    }

    @Parameters(commandDescription = "Prints metadata for the given path.")
    private static class CommandMeta {
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

    private JCommander jc;
    private JadeStorageService helper;

    public static void main(String[] args) throws Exception {
        StorageClientApp app = new StorageClientApp();
        app.interpretCommand(args);
    }

    private void interpretCommand(String[] args) throws Exception {

        CommandMain argsMain = new CommandMain();
        CommandList argsList = new CommandList();
        CommandMeta argsMeta = new CommandMeta();
        CommandRead argsRead = new CommandRead();
        CommandCopy argsCopy = new CommandCopy();

        jc = JCommander.newBuilder()
                .addObject(argsMain)
                .addCommand("list", argsList)
                .addCommand("meta", argsMeta)
                .addCommand("read", argsRead)
                .addCommand("copy", argsCopy)
                .build();

        try {
            jc.parse(args);
        }
        catch (ParameterException e) {
            usage("Error parsing parameters", jc);
        }

        helper = new JadeStorageService(argsMain.serverURL, argsMain.apiKey);

        String parsedCommand = jc.getParsedCommand();
        if (parsedCommand == null) {
            usage("Specify a command to execute", jc);
        }
        else {
            switch (parsedCommand) {
                case "list":
                    commandList(argsList);
                    break;
                case "meta":
                    commandMeta(argsMeta);
                    break;
                case "read":
                    commandRead(argsRead);
                    break;
                case "copy":
                    commandCopy(argsCopy);
                    break;
                default:
                    usage("Unsupported command: " + parsedCommand, jc);
            }
        }
    }

    private void commandList(CommandList args) throws Exception {
        StorageLocation storageLocation = getStorageLocation(args.path);
        List<StorageObject> descendants = helper.getDescendants(storageLocation, storageLocation.getRelativePath(args.path), args.depth);
        descendants.sort(Comparator.comparing(StorageObject::getAbsolutePath));
        descendants.forEach(storageObject -> {
            String size = StringUtils.leftPad(storageObject.getSizeBytes() + "", 12)+" bytes";
            System.out.println(size + " - " + storageObject.getAbsolutePath());
        });
    }

    private void commandMeta(CommandMeta args) throws Exception {
        StorageLocation storageLocation = getStorageLocation(args.path);
        StorageObject metadata = helper.getMetadata(storageLocation, storageLocation.getRelativePath(args.path));
        if (metadata != null) {
            System.out.println("Object name:   " + metadata.getObjectName());
            System.out.println("Absolute path: " + metadata.getAbsolutePath());
            System.out.println("Relative path: " + metadata.getRelativePath());
            System.out.println("Location:      " + metadata.getLocation().getStorageURL());
            System.out.println("Size (bytes):  " + metadata.getSizeBytes());
            System.out.println("isCollection:  " + metadata.isCollection());
        }
    }

    private void commandRead(CommandRead args) throws Exception {
        StorageLocation storageLocation = getStorageLocation(args.path);
        InputStream content = helper.getContent(storageLocation, storageLocation.getRelativePath(args.path));
        IOUtils.copy(content, System.out);
    }

    private void commandCopy(CommandCopy args) throws Exception {

        if (args.paths.size() != 2) {
            usage("Extract requires source and target path", jc);
        }

        Path sourcePath = Paths.get(args.paths.get(0));
        Path targetPath = Paths.get(args.paths.get(1));

        if (Files.exists(sourcePath)) {
            PathUtils.copyFiles(sourcePath, targetPath);
        }
        else {
            StorageLocation storageLocation = getStorageLocation(sourcePath.toString());
            String relativePath = storageLocation.getRelativePath(sourcePath.toString());
            FileUtils.copyInputStreamToFile(helper.getContent(storageLocation, relativePath), targetPath.toFile());
        }
    }

    private StorageLocation getStorageLocation(String path) {
        StorageLocation storageLocation = helper.getStorageObjectByPath(path);
        if (storageLocation == null) {
            System.err.println("Path not found in JADE: "+path);
            System.exit(1);
        }
        return storageLocation;
    }

    private void usage(String message, JCommander jc) {
        if (StringUtils.isNotBlank(message)) {
            System.err.println(message);
        }
        jc.usage();
        System.exit(1);
    }

}
