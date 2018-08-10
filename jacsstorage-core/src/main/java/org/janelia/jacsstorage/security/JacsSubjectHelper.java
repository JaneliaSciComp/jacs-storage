package org.janelia.jacsstorage.security;

import org.apache.commons.lang3.StringUtils;

public class JacsSubjectHelper {
    private static final String DEFAULT_SUBJECT_TYPE = "user";

    public static String getNameFromSubjectKey(String subjectKey) {
        if (StringUtils.isBlank(subjectKey)) {
            return null;
        }
        int typeNameSeparatorIndex = subjectKey.indexOf(':');
        if (typeNameSeparatorIndex == -1) {
            return subjectKey.trim();
        } else {
            return subjectKey.substring(typeNameSeparatorIndex + 1).trim();
        }
    }

    static String getTypeFromSubjectKey(String subjectKey) {
        if (StringUtils.isBlank(subjectKey)) {
            return null;
        }
        int typeNameSeparatorIndex = subjectKey.indexOf(':');
        if (typeNameSeparatorIndex == -1) {
            return DEFAULT_SUBJECT_TYPE;
        } else {
            return StringUtils.defaultIfBlank(subjectKey.substring(0, typeNameSeparatorIndex).trim(), DEFAULT_SUBJECT_TYPE);
        }
    }

    static String nameAsSubjectKey(String name) {
        if (StringUtils.isBlank(name)) {
            return null;
        } else {
            return getTypeFromSubjectKey(name) + ":" + getNameFromSubjectKey(name);
        }
    }
}
