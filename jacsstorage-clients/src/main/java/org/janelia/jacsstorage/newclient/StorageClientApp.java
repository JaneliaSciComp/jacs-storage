package org.janelia.jacsstorage.newclient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class StorageClientApp {

    private static final Logger LOG = LoggerFactory.getLogger(StorageClientApp.class);

    private static class CommandMain {
        @Parameter(names = "-server", description = "URL of the Master JADE API")
        private String serverURL = "http://localhost:8080/jacsstorage/master_api/v1";
        @Parameter(names = "-apikey", description = "API Key for JADE service")
        private String apiKey;
    }

    @Parameters(commandDescription = "Copy a file from a source location (which may be local or JADE-accessible) to a target location (which is local)")
    private static class CommandExtractBundle {

        @Parameter(description = "<source> <target>", arity = 2)
        private List<String> paths = new ArrayList<>();
    }

    public static void main(String[] args) throws Exception {
        CommandMain cmdMain = new CommandMain();
        CommandExtractBundle cmdExtract = new CommandExtractBundle();

        JCommander jc = JCommander.newBuilder()
                .addObject(cmdMain)
                .addCommand("copy", cmdExtract)
                .build();

        try {
            jc.parse(args);
        }
        catch (ParameterException e) {
            usage("Error parsing parameters", jc);
        }

        String parsedCommand = jc.getParsedCommand();
        if (parsedCommand == null) {
            usage("Specify a command to execute", jc);
        }

        switch (parsedCommand) {
            case "copy":
                copy(jc, cmdMain, cmdExtract);
                break;
            default:
                usage("Unsupported command: "+parsedCommand, jc);
        }
    }

    private static void copy(JCommander jc, CommandMain cmdMain, CommandExtractBundle cmdExtract) throws Exception {

        if (cmdExtract.paths.size() != 2) {
            usage("Extract requires source and target path", jc);
        }

        Path sourcePath = Paths.get(cmdExtract.paths.get(0));
        Path targetPath = Paths.get(cmdExtract.paths.get(1));

        if (Files.exists(sourcePath)) {
            PathUtils.copyFiles(sourcePath, targetPath);
        }
        else {
            // TODO: are these needed here?
            String subjectKey = null;
            String authToken = null;
            StorageService storageService = new StorageService(cmdMain.serverURL, cmdMain.apiKey);
            BetterStorageHelper helper = new BetterStorageHelper(storageService, subjectKey, authToken);

            StorageContentObject storageContentObject = helper
                    .getStorageObjectByPath(sourcePath.toString())
                    .orElseThrow(() -> new Exception("Could not find path in JADE: " + sourcePath.toString()));

            LOG.info("Found content object: {}", storageContentObject);

            FileUtils.copyInputStreamToFile(helper.getContent(storageContentObject), targetPath.toFile());
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
