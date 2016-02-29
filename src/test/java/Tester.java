import org.junit.Test;
import org.reactive_ros.Stream;
import rx_eval.RxjavaEvaluationStrategy;
import test_data.TestData;
import test_data.TestInfo;
import test_data.utilities.Colors;

/**
 * @author Orestis Melkonian
 */
public class Tester {
    @Test
    public void single_threaded() {
        Stream.setEvaluationStrategy(new RxjavaEvaluationStrategy());

        for (TestInfo test : TestData.tests()) {
            System.out.print(test.name + ": ");
            if (test.equality())
                Colors.print(Colors.GREEN, "Passed");
            else {
                Colors.print(Colors.RED, "Failed");
                System.out.println(test.q1 + "\n\t!=\n" + test.q2);
            }
        }
    }

    @Test
    public void multi_threaded() {
        Stream.setEvaluationStrategy(new RxjavaEvaluationStrategy(true));

        for (TestInfo test : TestData.tests()) {
            System.out.print(test.name + ": ");
            if (test.equality())
                Colors.print(Colors.GREEN, "Passed");
            else {
                Colors.print(Colors.RED, "Failed");
                System.out.println(test.q1 + " != " + test.q2);
            }
        }
    }
}
