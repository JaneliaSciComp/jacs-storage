package org.janelia.jacsstorage.app;

import com.beust.jcommander.JCommander;
import io.undertow.attribute.ConstantExchangeAttribute;
import io.undertow.attribute.ExchangeAttribute;
import io.undertow.attribute.ResolvedPathAttribute;
import io.undertow.predicate.Predicate;
import io.undertow.predicate.Predicates;
import io.undertow.servlet.api.ListenerInfo;
import org.apache.commons.lang3.StringUtils;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

/**
 * This is the master storage application.
 */
public class JacsMasterStorageApp extends AbstractStorageApp {

    public static void main(String[] args) {
        final AppArgs appArgs = new AppArgs();
        JCommander cmdline = new JCommander(appArgs);
        cmdline.parse(args);
        if (appArgs.displayUsage) {
            cmdline.usage();
            return;
        }
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit
                .initialize();
        JacsMasterStorageApp app = container.select(JacsMasterStorageApp.class).get();
        app.start(appArgs);
    }

    @Override
    String getJaxConfigName() {
        return JAXMasterStorageApp.class.getName();
    }

    @Override
    String getRestApi(AppArgs appArgs) {
        StringBuilder apiPathBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(appArgs.baseContextPath)) {
            apiPathBuilder.append(StringUtils.prependIfMissing(appArgs.baseContextPath, "/"));
        }
        apiPathBuilder.append("/master_api/")
                .append(getApiVersion());
        return apiPathBuilder.toString();
    }

    @Override
    ListenerInfo[] getAppListeners() {
        return new ListenerInfo[] {
        };
    }

    @Override
    protected Predicate getAccessLogFilter() {
        return Predicates.not(
                Predicates.equals(new ExchangeAttribute[]{
                        new ConstantExchangeAttribute("/agents/url"),
                        ResolvedPathAttribute.INSTANCE
                })
        );
    }

}
