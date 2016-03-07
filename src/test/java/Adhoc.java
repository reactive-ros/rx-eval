import graph_viz.GraphVisualizer;
import org.junit.Test;
import org.rhea_core.Stream;
import org.rhea_core.distribution.Distributor;
import rx_eval.RxjavaEvaluationStrategy;
import test_data.utilities.Threads;

/**
 * @author Orestis Melkonian
 */
public class Adhoc {

    @Test
    public void test() {
//        Stream.DEBUG = true;
        Stream.configure(new Distributor(RxjavaEvaluationStrategy::new));

        Stream<Integer> s =
//        Stream.range(0, 20)
        Stream.nat()
//        Stream.merge(Stream.just(0), Stream.just(1))
//        Stream.concat(Stream.just(0), Stream.just(1))
//        Stream.just(0, 1, 2).map(i -> i + 10).filter(i -> i > 10)
                ;


        // Evaluate
        s.printAll();

        Threads.sleep();
    }
}
