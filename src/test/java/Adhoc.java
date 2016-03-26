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
        Stream.configure(new Distributor(RxjavaEvaluationStrategy::new));

        Stream<Integer> s = Stream.nat();

        // Display
//        Stream.DEBUG = true;
        GraphVisualizer.display(s);

        // Evaluate
        s.printAll();

        Threads.sleep();
    }
}
