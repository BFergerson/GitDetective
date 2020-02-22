package io.gitdetective.web.model

import groovy.transform.Canonical

import java.time.Instant

@Canonical
class ProjectLiveReferenceTrend {
    String projectId
    List<TrendPoint> trendData = new ArrayList<>()

    @Canonical
    static class TrendPoint implements Comparable<TrendPoint> {
        Instant time
        boolean deletion
        long count

        @Override
        int compareTo(TrendPoint other) {
            int i = time <=> other.time
            if (i != 0) return i
            return deletion <=> other.deletion
        }
    }
}
