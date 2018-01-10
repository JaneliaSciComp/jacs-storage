package org.janelia.jacsstorage.security;

import org.apache.commons.lang3.StringUtils;

public class JacsSubjectHelper {
    private static final String DEFAULT_SUBJECT_TYPE = "user";

    public static String getNameFromSubjectKey(String subjectKey) {
        if (StringUtils.isBlank(subjectKey)) {
            return null;
        }
        int typeNameSeparatorIndex = subjectKey.indexOf(subjectKey);
        if (typeNameSeparatorIndex == -1) {
            return subjectKey.trim();
        } else {
            return subjectKey.substring(typeNameSeparatorIndex + 1).trim();
        }
    }

    public static String getTypeFromSubjectKey(String subjectKey) {
        if (StringUtils.isBlank(subjectKey)) {
            return null;
        }
        int typeNameSeparatorIndex = subjectKey.indexOf(subjectKey);
        if (typeNameSeparatorIndex == -1) {
            return DEFAULT_SUBJECT_TYPE;
        } else {
            return StringUtils.defaultIfBlank(subjectKey.substring(0, typeNameSeparatorIndex), DEFAULT_SUBJECT_TYPE);
        }
    }

}
