package io.gitdetective.web.service

import io.vertx.core.AbstractVerticle

class ProjectService extends AbstractVerticle {

    @Override
    void start() throws Exception {
        vertx.eventBus()
    }

    void getMostReferencedProjectsInformation() {
    }
}
