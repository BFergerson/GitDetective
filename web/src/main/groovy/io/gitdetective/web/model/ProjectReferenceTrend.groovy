package io.gitdetective.web.model

import groovy.transform.Canonical

import java.time.Instant

@Canonical
class ProjectReferenceTrend {
    String projectId
    List<TrendPoint> trendData = new ArrayList<>()

    @Canonical
    static class TrendPoint implements Comparable<TrendPoint> {
        Instant time
        long count

        @Override
        int compareTo(TrendPoint other) {
            return time <=> other.time
        }
    }
}
