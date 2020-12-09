package org.example;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CaseTest {
    @Test
    public void should_batch_when_queryingForStream() {
        var enrichmentService = new EnrichmentService();
        Cases.queryingForStream(enrichmentService);

        assertEquals(Source.TOTAL_NUMBER_OF_ITEMS_IN_FLUX / Cases.BATCH_SIZE, enrichmentService.getNumberOfTimesCalled());
    }

    @Test
    public void should_batch_when_queryingForFlux() {
        var enrichmentService = new EnrichmentService();
        Cases.queryingForFlux(enrichmentService);

        assertEquals(Source.TOTAL_NUMBER_OF_ITEMS_IN_FLUX / Cases.BATCH_SIZE, enrichmentService.getNumberOfTimesCalled());
    }

    @Test
    public void should_batch_when_subscribingToStream() {
        var enrichmentService = new EnrichmentService();
        Cases.subscribingToStream(enrichmentService);

        assertEquals(Source.TOTAL_NUMBER_OF_ITEMS_IN_FLUX / Cases.BATCH_SIZE, enrichmentService.getNumberOfTimesCalled());
    }

    @Test
    public void should_notBatch_when_subscribingToFlux() {
        var enrichmentService = new EnrichmentService();
        Cases.subscribingToFlux(enrichmentService);

        assertEquals(Source.TOTAL_NUMBER_OF_ITEMS_IN_FLUX, enrichmentService.getNumberOfTimesCalled());
    }

    @Test
    public void should_batchPerWindow_when_subscribingToWindowedFlux() {
        var enrichmentService = new EnrichmentService();
        var resultFluxOfLists = Cases.subscribingToWindowedFlux(enrichmentService);

        var resultList =
                resultFluxOfLists
                        .collectList()
                        .block()
                        .stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());

        assertEquals(Source.TOTAL_NUMBER_OF_ITEMS_IN_FLUX / Source.NUMBER_OF_ITEMS_PER_WINDOW, enrichmentService.getNumberOfTimesCalled());
        assertEquals(10, resultList.size());
    }
}
