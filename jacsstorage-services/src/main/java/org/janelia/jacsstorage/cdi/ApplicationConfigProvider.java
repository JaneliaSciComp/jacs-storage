package org.janelia.jacsstorage.cdi;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.config.ApplicationConfigImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class ApplicationConfigProvider {

    private static final Logger log = LoggerFactory.getLogger(ApplicationConfigProvider.class);

    private static final String DEFAULT_APPLICATION_CONFIG_RESOURCES = "/jacsstorage.properties";

    private static final Map<String, String> APP_DYNAMIC_ARGS = new HashMap<>();

    public static Map<String, String> getAppDynamicArgs() {
        return APP_DYNAMIC_ARGS;
    }

    public static void setAppDynamicArgs(Map<String, String> appDynamicArgs) {
        if (appDynamicArgs != null) APP_DYNAMIC_ARGS.putAll(appDynamicArgs);
    }

    private ApplicationConfig applicationConfig = new ApplicationConfigImpl();

    public ApplicationConfigProvider fromDefaultResources() {
        return fromProperties(System.getProperties())
                .fromMap(System.getenv().entrySet().stream().collect(Collectors.toMap(entry -> "env." + entry.getKey(), Map.Entry::getValue)))
                .fromResource(DEFAULT_APPLICATION_CONFIG_RESOURCES);
    }

    public ApplicationConfigProvider fromResource(String resourceName) {
        if (StringUtils.isBlank(resourceName)) {
            return this;
        }
        try (InputStream configStream = this.getClass().getResourceAsStream(resourceName)) {
            return fromInputStream(configStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ApplicationConfigProvider fromEnvVar(String envVarName) {
        if (StringUtils.isBlank(envVarName)) {
            return this;
        }
        String envVarValue = System.getenv(envVarName);
        if (StringUtils.isBlank(envVarValue)) {
            return this;
        }
        log.info("Reading application config from environment {} -> {}", envVarName, envVarValue);
        return fromFile(envVarValue);
    }

    public ApplicationConfigProvider fromFile(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return this;
        }
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            try (InputStream fileInputStream = new FileInputStream(file)) {
                log.info("Reading application config from file {}", file);
                return fromInputStream(fileInputStream);
            } catch (IOException e) {
                log.error("Error reading configuration file {}", fileName, e);
                throw new UncheckedIOException(e);
            }
        } else {
            log.warn("Configuration file {} not found", fileName);
        }
        return this;
    }

    public ApplicationConfigProvider fromProperties(Properties properties) {
        properties.stringPropertyNames().forEach(k -> applicationConfig.put(k, properties.getProperty(k)));
        return this;
    }

    public ApplicationConfigProvider fromMap(Map<String, String> map) {
        applicationConfig.putAll(map);
        return this;
    }

    private ApplicationConfigProvider fromInputStream(InputStream stream) {
        try {
            applicationConfig.load(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return this;
    }

    private ApplicationConfigProvider injectEnvProps() {

        String prefix = "env.jade_";
        for (Object o : Sets.newLinkedHashSet(applicationConfig.asMap().keySet())) {
            String key = o.toString();
            if (key.toLowerCase().startsWith(prefix)) {
                String newKey = key.substring(prefix.length()).replaceAll("_", ".");
                log.debug("Overriding {} with value from env", newKey);
                applicationConfig.put(newKey, applicationConfig.asMap().get(key));
            }
        }

        return this;
    }

    public ApplicationConfig build() {
        injectEnvProps();
        return applicationConfig;
    }

}
