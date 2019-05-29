package org.janelia.jacsstorage.config;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;

public class ApplicationConfigValueResolver {

    private final static String START_PLACEHOLDER = "${";
    private final static char END_PLACEHOLDER_CHAR = '}';
    private final static char START_PLACEHOLDER_CHAR = '$';
    private final static char ESCAPE_CHAR = '\\';

    private enum ResolverState {
        StartPlaceHolder, OutsidePlaceHolder, InsidePlaceHolder, EscapeChar
    }

    public String resolve(String v, ContextValueGetter contextValueGetter) {
        if (contextValueGetter == null) {
            // no context so simply return the value as is
            return v;
        }
        return resolve(v, contextValueGetter, ImmutableSet.of());
    }

    private String resolve(String v, ContextValueGetter contextValueGetter, Set<String> evalHistory) {
        if (StringUtils.isBlank(v)) return v;
        StringBuilder resolvedValueBuilder = new StringBuilder();
        StringBuilder placeHolderBuilder = new StringBuilder();
        ResolverState state = ResolverState.OutsidePlaceHolder;
        for (char currentChar : v.toCharArray()) {
            switch (state) {
                case StartPlaceHolder:
                    placeHolderBuilder.append(currentChar);
                    if (START_PLACEHOLDER.equals(placeHolderBuilder.toString())) {
                        state = ResolverState.InsidePlaceHolder;
                    } else if (!START_PLACEHOLDER.startsWith(placeHolderBuilder.toString())) {
                        resolvedValueBuilder.append(placeHolderBuilder);
                        placeHolderBuilder.setLength(0);
                        state = ResolverState.OutsidePlaceHolder;
                    }
                    break;
                case OutsidePlaceHolder:
                    switch (currentChar) {
                        case ESCAPE_CHAR:
                            state = ResolverState.EscapeChar;
                            break;
                        case START_PLACEHOLDER_CHAR:
                            placeHolderBuilder.append(currentChar);
                            if (START_PLACEHOLDER.equals(placeHolderBuilder.toString())) {
                                state = ResolverState.InsidePlaceHolder;
                            } else {
                                state = ResolverState.StartPlaceHolder;
                            }
                            break;
                        default:
                            resolvedValueBuilder.append(currentChar);
                            break;
                    }
                    break;
                case InsidePlaceHolder:
                    switch (currentChar) {
                        case END_PLACEHOLDER_CHAR:
                            placeHolderBuilder.append(currentChar);
                            String placeHolderString = placeHolderBuilder.toString();
                            String placeHolderKey = placeHolderBuilder.substring(START_PLACEHOLDER.length(), placeHolderBuilder.length() - 1);
                            if (evalHistory.contains(placeHolderKey)) {
                                throw new IllegalStateException("Circular dependency found while evaluating " + v + " -> " + evalHistory);
                            }
                            String placeHolderValue = contextValueGetter.get(placeHolderKey);
                            if (placeHolderValue == null) {
                                // no value found - put the placeholder as is
                                resolvedValueBuilder.append(placeHolderString);
                            } else {
                                resolvedValueBuilder.append(resolve(placeHolderValue, contextValueGetter, ImmutableSet.<String>builder().addAll(evalHistory).add(placeHolderKey).build()));
                            }
                            placeHolderBuilder.setLength(0);
                            state = ResolverState.OutsidePlaceHolder;
                            break;
                        default:
                            placeHolderBuilder.append(currentChar);
                            break;
                    }
                    break;
                case EscapeChar:
                    resolvedValueBuilder.append(currentChar);
                    state = ResolverState.OutsidePlaceHolder;
                    break;
            }
        }
        if (state != ResolverState.OutsidePlaceHolder) {
            throw new IllegalStateException("Unclosed placeholder found while trying to resolve " + v + " -> " + evalHistory);
        }
        return resolvedValueBuilder.toString();
    }

}
