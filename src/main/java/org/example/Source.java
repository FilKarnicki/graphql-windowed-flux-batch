package org.example;

import org.example.dto.Person;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Fakes a source of Person data, which is to be enriched
 */
public class Source {
    public static final int TOTAL_NUMBER_OF_ITEMS_IN_FLUX = 100;
    public static final int NUMBER_OF_ITEMS_PER_WINDOW = 5;

    /**
     * Gets a list of type {@link org.example.dto.Person}, one of which fields needs to be enriched with a 'service call'.
     *
     * @return List of people
     */
    public static CompletableFuture<Stream<Person>> getPersonList() {
        return CompletableFuture.supplyAsync(() ->
                        IntStream.range(1, TOTAL_NUMBER_OF_ITEMS_IN_FLUX).boxed().map(Person::new),
                CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS));
    }

    /**
     * Similar to {@link #getPersonList()}, but instead of a list, it's a flux
     *
     * @return Flux of people
     */
    public static Flux<Person> getPersonFlux() {
        return Mono
                .fromFuture(getPersonList())
                .flatMapMany(Flux::fromStream);
    }

    /**
     * Similar to {@link #getPersonFlux()}, but the entries are windowed into a flux of lists of people
     *
     * @return Flux of windows of people
     */
    public static Flux<List<Person>> getPersonFluxWindowed() {
        return getPersonFlux()
                .window(5)
                .flatMap(Flux::collectList);
    }
}
