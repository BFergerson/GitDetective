package io.gitdetective.indexer.extractor.observer

import com.codebrig.arthur.observe.structure.StructureFilter
import com.codebrig.arthur.observe.structure.filter.CompilationUnitFilter
import com.codebrig.phenomena.code.CodeObserver
import com.codebrig.phenomena.code.ContextualNode

class CompilationUnitObserver extends CodeObserver {

    private final StructureFilter filter = new CompilationUnitFilter()

    @Override
    void applyObservation(ContextualNode contextualNode, ContextualNode parentNode) {
        //do nothing
    }

    @Override
    StructureFilter getFilter() {
        return filter
    }
}
