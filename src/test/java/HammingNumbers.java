import org.junit.Test;
import org.rhea_core.Stream;

import java.util.Collections;
import java.util.PriorityQueue;

import hazelcast_distribution.HazelcastDistributionStrategy;
import rx_eval.RxjavaEvaluationStrategy;
import test_data.utilities.Threads;

/**
 * @author Orestis Melkonian
 */
public class HammingNumbers {

//    @Test
    public void hamming() {
        Stream.distributionStrategy =
                new HazelcastDistributionStrategy(Collections.singletonList(RxjavaEvaluationStrategy::new));

        Stream<?> stream =
                Stream.just(1).loop(s ->
                        mergeSort(
                            mergeSort(multiply(s, 2), multiply(s, 3)),
                            multiply(s, 5))
                ).distinct();


//        GraphVisualizer.display(stream);
        stream.printAll();

        Threads.sleep();
    }

    static Stream<Integer> multiply(Stream<Integer> stream, int constant) {
        return stream.map(i -> i * constant);
    }

    static Stream<Integer> mergeSort(Stream<Integer> fst, Stream<Integer> snd) {
        return Stream.using(
                PriorityQueue<Integer>::new,
                queue -> Stream.<Integer, Integer, Integer>zip(
                        fst, snd,
                        (x, y) -> {
                            int min = Math.min(x, y);
                            queue.add(Math.max(x, y));
                            if (min < queue.peek())
                                return min;
                            queue.add(min);
                            return queue.poll();
                        }),
                PriorityQueue::clear
        );
    }
}
