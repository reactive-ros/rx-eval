import static org.junit.Assert.*;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.reactive_ros.Stream;
import org.reactive_ros.util.functions.Func0;
import rx_eval.RxjavaEvaluationStrategy;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Orestis Melkonian
 */
@RunWith(value = Parameterized.class)
public class Tester {
    private String testName;
    private Stream streamA;
    private Stream streamB;

    //parameters pass via this constructor
    public Tester(String testName, Stream streamA, Stream streamB) {
        this.testName = testName;
        this.streamA = streamA;
        this.streamB = streamB;
    }

    //Declares parameters here
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        Streams s = new Streams();
        setUp(s);
        return s.streams;
    }

    @Before
    public void initialize() {
       Stream.setEvaluationStrategy(new RxjavaEvaluationStrategy());
    }


    @Test
    public void test() {
        Object[] array1 = streamA.toBlocking().toList().toArray();
        Object[] array2 = streamB.toBlocking().toList().toArray();

        assertArrayEquals(array1, array2);
    }

    static void setUp(Streams pairs) {
        pairs.put("from",
                Stream.from(Arrays.asList(1, 2, 3, 4, 5)),
                Stream.just(1,2,3,4,5)
        );
        pairs.put("defer",
                Stream.defer(() -> Stream.just(1, 2, 3, 4, 5)),
                Stream.just(1,2,3,4,5)
        );
        pairs.put("interval_take_cast",
                Stream.interval(100, TimeUnit.MILLISECONDS).take(5).map(Long::intValue),
                Stream.just(0,1,2,3,4)
        );
        pairs.put("map",
                Stream.just(0,1,2,3,4).map(i -> i * i),
                Stream.just(0,1,4,9,16)
        );
        pairs.put("scan",
                Stream.just(1,2,3,4).scan(0, (i1, i2) -> i1 + i2),
                Stream.just(0,1,3,6,10)
        );
        pairs.put("skip_take_range",
                Stream.range(0,5).takeWhile(i -> i < 2).takeLast(2).skip(1),
                Stream.just(1)
        );
        pairs.put("amb_exists",
                Stream.amb(Stream.just(0), Stream.just(1)).exists(i -> i == 0 || i == 1),
                Stream.just(true)
        );
        pairs.put("all",
                Stream.just(1, 2, 3, 4, 5).all(i -> i < 6),
                Stream.just(true)
        );
        pairs.put("distinct",
                Stream.just(1,2,2,3,3,3,3,4,3,5,2,5,5,6,6,7,8,9,10,10,1,2,3,4,5,6,7,8,9,10,10,9,8,7,6,5,4,3,2,1,0).distinct(),
                Stream.just(1,2,3,4,5,6,7,8,9,10,0)
        );
        pairs.put("repeat",
                Stream.just(1,2,3).repeat(3),
                Stream.just(1,2,3,1,2,3,1,2,3)
        );
        pairs.put("buffer",
                Stream.range(0,6).buffer(2),
                Stream.just(Arrays.asList(0,1),Arrays.asList(2,3),Arrays.asList(4,5))
        );
        pairs.put("window",
                Stream.range(0,6).window(3),
                Stream.just(Stream.just(0,1,2),Stream.just(3,4,5))
        );
        pairs.put("cache",
                Stream.range(0,10).cache().cache(),
                Stream.range(0, 10)
        );
        pairs.put("delay",
                Stream.range(0,6).delay(1, TimeUnit.SECONDS),
                Stream.range(0,6)
        );
        pairs.put("merge",
                Stream.merge(Stream.just(0,2,4), Stream.just(6,8,10)).exists(i -> i % 2 != 0),
                Stream.just(false)
        );
        pairs.put("zip",
                Stream.zip(Stream.just(1,2,3), Stream.just("One", "Two", "Three"), (i, str) -> str + "[" + i + "]"),
                Stream.just("One[1]", "Two[2]", "Three[3]")
        );
        pairs.put("concat",
                Stream.concat(Stream.just(1,2,3), Stream.just(4,5,6), Stream.just(7,8,9,10)),
                Stream.range(1,10)
        );
        /*pairs.put("onErrorResume_Return",
                Stream.range(0,21535).flatMap(i -> (i==0) ? Stream.just(0) : Stream.error(null)).onErrorResumeNext(Stream.just(10,1000)),
//                Stream.just(0,10,20).flatMap(i -> (i < 20) ? Stream.just(i) : Stream.error(null)).onErrorReturn(t -> 1000),
                Stream.just(0,10,1000)
        );*/


        pairs.put("test1",
                Stream.just(6,7,8,9,10),
                Stream.just(6,7,8,9,10)
        );
        pairs.put("test2",
                Stream.from(1,2,3,4).map(i -> i * i).filter(i -> i % 2 != 0),
                Stream.from(1,9)
        );
        pairs.put("test3",
                Stream.from(0,1,2,3,4,5,6,7,8,9,10).take(5).repeat(2),
                Stream.from(0,1,2,3,4,0,1,2,3,4)
        );
        pairs.put("test4",
                Stream.just(6f,7f,8f,9f,10f).cache().reduce("", (s, f) -> s + Float.toString(f)),
                Stream.just("6.07.08.09.010.0")
        );
        pairs.put("test5",
                Stream.just(3.0).repeat(10).distinct().filter(d -> d > 1).map(d -> d * 5).flatMap(d -> Stream.just(2.0 * d - 5.0)),
                Stream.just(25.0)
        );
        pairs.put("test6",
                Stream.just(new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5))).repeat(2).doOnNext(l -> l.removeIf(e -> e % 2 == 0)).buffer(2).take(1).map(l -> l.get(0)).map(l -> l.get(0) + l.get(1)).repeat(4),
                Stream.just(4,4,4,4)
        );
        pairs.put("test7",
                Stream.just(1, 2, 3, 4, 5).collect((Func0<HashMap<Integer, String>>) HashMap::new, (m, i) -> {
                    if (i % 2 == 0) m.put(i, Integer.toString(i));
                }).map(h -> new HashSet<>(h.values())).map(c -> {
                    String ret = "";
                    for (String s : c) ret += s;
                    return ret;
                }),
                Stream.just("24")
        );
        pairs.put("test8",
                Stream.interval(100, TimeUnit.MILLISECONDS).takeWhile(i -> i < 10).map(i -> Long.toString(i)).map(String::length).reduce(0, (i1, i2) -> i1 + i2).cast(Integer.class),
                Stream.just(10)
        );
        pairs.put("test9",
                Stream.just(Arrays.asList(1,2,3,4,5)).repeat(2).take(1).map(l -> {
                    int sum = 0;
                    for (int i : l) sum += i;
                    return sum;
                }),
                Stream.just(15)
        );
    }
    static class Streams {
        public Queue<Object[]> streams = new LinkedList<>();

        public void put(String name, Stream s1, Stream s2) {
            streams.add(new Object[] {name, s1, s2});
        }
    }
}
