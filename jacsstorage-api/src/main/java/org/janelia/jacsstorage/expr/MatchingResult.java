package org.janelia.jacsstorage.expr;

public class MatchingResult {
    private final boolean matchFound;
    private final String mismatchReason;
    private final String matchedPrefix;
    private final String unmatchedSuffix;

    MatchingResult(boolean matchFound,
                   String mismatchReason,
                   String matchedPrefix,
                   String unmatchedSuffix) {
        this.matchFound = matchFound;
        this.mismatchReason = mismatchReason;
        this.matchedPrefix = matchedPrefix;
        this.unmatchedSuffix = unmatchedSuffix;
    }

    public boolean isMatchFound() {
        return matchFound;
    }

    public String getMismatchReason() {
        return mismatchReason;
    }

    public String getMatchedPrefix() {
        return matchedPrefix;
    }

    public String getUnmatchedSuffix() {
        return unmatchedSuffix;
    }
}
