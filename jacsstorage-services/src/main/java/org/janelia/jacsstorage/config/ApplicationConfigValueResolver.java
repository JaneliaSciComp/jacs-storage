package org.janelia.jacsstorage.config;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApplicationConfigValueResolver {

    public String resolve(String v, Map<String, String> context) {
        return resolve(v, context, ImmutableSet.of());
    }

    private String resolve(String v, Map<String, String> context, Set<String> resolveHistory) {
        if (StringUtils.isBlank(v)) return v;
        Set<String> newResolveHistory = ImmutableSet.<String>builder().addAll(resolveHistory).add(v).build();
        Pattern placeholderPattern = Pattern.compile("\\$\\{(.+?)\\}");
        Matcher m = placeholderPattern.matcher(v);
        int startIndex = 0;
        StringBuilder resolvedValueBuilder = new StringBuilder();
        while (m.find(startIndex)) {
            int startRegion = m.start();
            int endRegion = m.end();
            String key = v.substring(m.start() + 2, m.end() - 1);
            resolvedValueBuilder.append(v.substring(startIndex, startRegion));
            if (newResolveHistory.contains(key)) {
                throw new IllegalStateException("Circular dependency found while evaluating " + v + " -> " + newResolveHistory);
            }
            String keyValue = context.get(key);
            if (keyValue == null) {
                resolvedValueBuilder.append(v.substring(startRegion, endRegion));
            } else {
                resolvedValueBuilder.append(resolve(keyValue, context, newResolveHistory));
            }
            startIndex = endRegion;
        }
        resolvedValueBuilder.append(v.substring(startIndex));
        return resolvedValueBuilder.toString();
    }

}
