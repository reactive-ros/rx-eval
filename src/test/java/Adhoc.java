import org.junit.Test;
import org.reactive_ros.Stream;
import rx_eval.RxjavaEvaluationStrategy;
import test_data.utilities.Threads;

/**
 * @author Orestis Melkonian
 */
public class Adhoc {

    @Test
    public void test() {
        Stream.setEvaluationStrategy(new RxjavaEvaluationStrategy(true));

        Threads.sleep();
    }
}
