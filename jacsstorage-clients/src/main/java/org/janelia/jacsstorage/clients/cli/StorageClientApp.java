package org.janelia.jacsstorage.clients.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.clients.api.JadeStorageAttributes;
import org.janelia.jacsstorage.clients.api.JadeStorageService;
import org.janelia.jacsstorage.clients.api.StorageLocation;
import org.janelia.jacsstorage.clients.api.StorageObject;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.saalfeldlab.n5.N5TreeNode;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class StorageClientApp {

    private static class CommandMain {
        @Parameter(names = {"-s", "--server"}, description = "URL of the Master JADE API")
        private String serverURL = "http://localhost:8080/jacsstorage/master_api/v1";
        @Parameter(names = {"-k", "--key"}, description = "API Key for JADE service")
        private String apiKey;
    }

    private static abstract class AbstractCommand {
        @Parameter(names = {"--access-key"}, description = "access key")
        private String accessKey;
        @Parameter(names = {"--secret-key"}, description = "secret key")
        private String secretKey;

        JadeStorageAttributes getStorageOptions() {
            return new JadeStorageAttributes()
                    .setAttributeValue("AccessKey", accessKey)
                    .setAttributeValue("SecretKey", secretKey);
        }
    }

    @Parameters(commandDescription = "Recursively list the descendants of the given path to a specified depth. By default, only the immediate children are listed.")
    private static class CommandList extends AbstractCommand {
        @Parameter(description = "<path>")
        private String path;
        @Parameter(names = {"-d", "--depth"}, description = "Depth of tree to list")
        private int depth = 1;
    }

    @Parameters(commandDescription = "Prints metadata for the given path.")
    private static class CommandMeta extends AbstractCommand {
        @Parameter(description = "<path>")
        private String path;
    }

    @Parameters(commandDescription = "Read a file at the given path")
    private static class CommandRead extends AbstractCommand {
        @Parameter(description = "<path>")
        private String path;
    }

    @Parameters(commandDescription = "Write given local file to an object at the given path")
    private static class CommandWrite extends AbstractCommand {
        @Parameter(description = "<source> <target>", arity = 2)
        private List<String> paths = new ArrayList<>();
    }

    @Parameters(commandDescription = "Copy a file from a source location to a target location (locations may be local or JADE-accessible)")
    private static class CommandCopy extends AbstractCommand {
        @Parameter(description = "<source> <target>", arity = 2)
        private List<String> paths = new ArrayList<>();
        @Parameter(names = {"-v", "--verify"}, description = "Verify by reading the entire file after writing")
        private boolean verify = false;
    }

    @Parameters(commandDescription = "Show N5 data sets at the given path")
    private static class CommandN5Tree extends AbstractCommand {
        @Parameter(description = "<path>")
        private String path;
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
        CommandWrite argsWrite = new CommandWrite();
        CommandCopy argsCopy = new CommandCopy();
        CommandN5Tree argsN5Tree = new CommandN5Tree();

        jc = JCommander.newBuilder()
                .addObject(argsMain)
                .addCommand("list", argsList)
                .addCommand("meta", argsMeta)
                .addCommand("read", argsRead)
                .addCommand("write", argsWrite)
                .addCommand("copy", argsCopy)
                .addCommand("n5tree", argsN5Tree)
                .build();

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            usage("Error parsing parameters", jc);
        }

        helper = new JadeStorageService(argsMain.serverURL, argsMain.apiKey);

        String parsedCommand = jc.getParsedCommand();
        if (parsedCommand == null) {
            usage("Specify a command to execute", jc);
        } else {
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
                case "write":
                    commandWrite(argsWrite);
                    break;
                case "copy":
                    commandCopy(argsCopy);
                    break;
                case "n5tree":
                    commandN5Tree(argsN5Tree);
                    break;
                default:
                    usage("Unsupported command: " + parsedCommand, jc);
            }
        }
    }

    private void commandList(CommandList args) throws Exception {
        StorageLocation storageLocation = getStorageLocation(args.path, args.getStorageOptions());
        List<StorageObject> descendants = helper.getDescendants(storageLocation, storageLocation.getRelativePath(args.path), args.depth);
        descendants.sort(Comparator.comparing(StorageObject::getAbsolutePath));
        descendants.forEach(storageObject -> {
            String size = StringUtils.leftPad(storageObject.getSizeBytes() + "", 12) + " bytes";
            System.out.println(size + " - " + storageObject.getAbsolutePath());
        });
    }

    private void commandMeta(CommandMeta args) throws Exception {
        StorageLocation storageLocation = getStorageLocation(args.path, args.getStorageOptions());
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
        StorageLocation storageLocation = getStorageLocation(args.path, args.getStorageOptions());
        InputStream content = helper.getContent(storageLocation, storageLocation.getRelativePath(args.path));
        IOUtils.copy(content, System.out);
    }

    private void commandWrite(CommandWrite args) throws Exception {
        Path sourcePath = Paths.get(args.paths.get(0));
        Path targetPath = Paths.get(args.paths.get(1));
        try (InputStream inputStream = new FileInputStream(sourcePath.toFile())) {
            setFileStream(targetPath, args.getStorageOptions(), inputStream);
        }
    }

    private void commandCopy(CommandCopy args) throws Exception {

        if (args.paths.size() != 2) {
            usage("Extract requires source and target path", jc);
        }

        Path sourcePath = Paths.get(args.paths.get(0)).toAbsolutePath();
        Path targetPath = Paths.get(args.paths.get(1)).toAbsolutePath();

        if (Files.exists(sourcePath) && Files.exists(targetPath.getParent())) {
            // Both paths are locally available, so just copy
            PathUtils.copyFiles(sourcePath, targetPath);
        } else {
            if (Files.exists(sourcePath)) {
                // Read locally and write to JADE
                try (InputStream inputStream = new FileInputStream(sourcePath.toFile())) {
                    setFileStream(targetPath, args.getStorageOptions(), inputStream);
                }
            } else if (Files.exists(targetPath.getParent())) {
                // Read from JADE and write locally
                try (InputStream source = getFileStream(sourcePath, args.getStorageOptions())) {
                    FileUtils.copyInputStreamToFile(source, targetPath.toFile());
                }
            } else {
                // Read from JADE and write to JADE
                try (InputStream source = getFileStream(sourcePath, args.getStorageOptions())) {
                    setFileStream(targetPath, args.getStorageOptions(), source);
                }
            }
        }

        if (args.verify) {
            System.out.println("Comparing source against target...");
            InputStream source = getFileStream(sourcePath, args.getStorageOptions());
            InputStream target = getFileStream(targetPath, args.getStorageOptions());
            if (IOUtils.contentEquals(source, target)) {
                System.out.println("Verified target bytes");
            } else {
                System.err.println("Post-copy verification failed!");
                System.exit(1);
            }
        }
    }

    private void commandN5Tree(CommandN5Tree args) throws Exception {
        StorageLocation storageLocation = getStorageLocation(args.path, args.getStorageOptions());
        N5TreeNode n5Tree = helper.getN5Tree(storageLocation, storageLocation.getRelativePath(args.path));
        if (n5Tree != null) {
            printN5Tree(n5Tree, "");
        }
    }

    private void printN5Tree(N5TreeNode node, String indent) {
        System.out.println(node.getNodeName());
        for (N5TreeNode n5TreeNode : node.childrenList()) {
            printN5Tree(n5TreeNode, indent + "  ");
        }
    }

    private StorageLocation getStorageLocation(String path, JadeStorageAttributes storageOptions) {
        StorageLocation storageLocation = helper.getStorageLocationByPath(path, storageOptions);
        if (storageLocation == null) {
            System.err.println("Path not found in JADE: " + path);
            System.exit(1);
        }
        return storageLocation;
    }

    private InputStream getFileStream(Path path, JadeStorageAttributes storageOptions) throws FileNotFoundException {

        if (Files.exists(path)) {
            return new FileInputStream(path.toFile());
        }

        StorageLocation sourceStorageLocation = getStorageLocation(path.toString(), storageOptions);
        String sourceRelativePath = sourceStorageLocation.getRelativePath(path.toString());
        InputStream stream = helper.getContent(sourceStorageLocation, sourceRelativePath);
        System.out.println("Found " + sourceRelativePath + " in " + sourceStorageLocation.getStorageURL());
        return stream;
    }

    private void setFileStream(Path path, JadeStorageAttributes storageOptions, InputStream inputStream) {
        StorageLocation storageLocation = getStorageLocation(path.toString(), storageOptions);
        String relativePath = storageLocation.getRelativePath(path.toString());
        helper.setContent(storageLocation, relativePath, inputStream);
        System.out.println("Wrote " + relativePath + " to " + storageLocation);
    }

    private void usage(String message, JCommander jc) {
        if (StringUtils.isNotBlank(message)) {
            System.err.println(message);
        }
        jc.usage();
        System.exit(1);
    }

}
