import graph_viz.GraphVisualizer;
import org.junit.Test;
import org.rhea_core.Stream;
import org.rhea_core.distribution.SingleMachineDistributionStrategy;

import rx_eval.RxjavaEvaluationStrategy;
import test_data.utilities.Threads;

/**
 * @author Orestis Melkonian
 */
public class Adhoc {

    @Test
    public void test() {
        Stream.distributionStrategy = new SingleMachineDistributionStrategy(new RxjavaEvaluationStrategy());

        Stream<Integer> s = Stream.just(0,1,2);//.nat();

        // Display
//        Stream.DEBUG = true;
//        GraphVisualizer.display(s);

        // Evaluate
        s.printAll();

        Threads.sleep();
    }
}
