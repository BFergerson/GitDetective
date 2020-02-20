package io.gitdetective.web.service.model

import groovy.transform.Canonical

@Canonical
class ProjectReferenceInformation {
    String projectName
    int referenceCount

    String getUsername() {
        return projectName.substring(projectName.indexOf(":") + 1, projectName.indexOf("/"))
    }

    String getUsernameAndProjectName() {
        return projectName.substring(projectName.indexOf(":") + 1)
    }

    String getSimpleProjectName() {
        return projectName.substring(projectName.indexOf("/") + 1)
    }
}
