package io.gitdetective.web.work.importer

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class ImportTimeoutException extends Exception {

    ImportTimeoutException() {
        super("Project failed to import in required time")
    }

}
