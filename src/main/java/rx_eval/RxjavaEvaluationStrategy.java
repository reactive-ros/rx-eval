package rx_eval;

import org.rhea_core.evaluation.EvaluationStrategy;
import org.rhea_core.Stream;
import org.rhea_core.internal.expressions.feedback.EntryPointExpr;
import org.rhea_core.internal.expressions.feedback.ExitPointExpr;
import org.rhea_core.internal.graph.FlowGraph;
import org.rhea_core.internal.notifications.Notification;
import org.rhea_core.internal.output.*;
import org.rhea_core.internal.expressions.*;
import org.rhea_core.internal.expressions.conditional_boolean.*;
import org.rhea_core.internal.expressions.creation.*;
import org.rhea_core.internal.expressions.transformational.*;
import org.rhea_core.internal.expressions.combining.*;
import org.rhea_core.internal.expressions.filtering.*;
import org.rhea_core.internal.expressions.backpressure.*;
import org.rhea_core.internal.expressions.error_handling.*;
import org.rhea_core.internal.expressions.utility.*;

import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

/**
 * Evaluates a dataflow graph using built-in rxjava operators.
 * Streams are implemented as {@link rx.Observable}.
 * <p> Can also enable multithreading with a particular {@link Scheduler}. </p>
 * @author Orestis Melkonian
 */
@SuppressWarnings("unchecked")
public class RxjavaEvaluationStrategy implements EvaluationStrategy {

    private boolean parallel = false;
    private Scheduler scheduler = Schedulers.computation();
    private Deque<FlowGraph> graphs = new ConcurrentLinkedDeque<>();

    private FlowGraph currentGraph() {
        return graphs.peekFirst();
    }

    // Needed for graph contains cycles (i.e. feedback loops)
    private Map<ExitPointExpr, Subject> evaluated = new ConcurrentHashMap<>();
    private Deque<Pair<Observable, Subject>> connections = new ConcurrentLinkedDeque<>();
//    private ConcurrentSkipListSet<Pair<Observable, Subject>> toDisconnect = new ConcurrentSkipListSet<>();


    /**
     * Default constructor: Single-threaded execution.
     */
    public RxjavaEvaluationStrategy() {
    }

    /**
     * Constructor specifying if concurrency is enabled.
     * @param parallel set to true, if you want multithreaded execution using the default {@link Scheduler}
     */
    public RxjavaEvaluationStrategy(boolean parallel) {
        this.parallel = parallel;
    }

    /**
     * Constructor specifying concurrency and also scheduler.
     * @param parallel set to true, if you want multithreaded execution using the given {@link Scheduler}
     * @param scheduler the {@link Scheduler} to use
     */
    public RxjavaEvaluationStrategy(boolean parallel, Scheduler scheduler) {
        this.parallel = parallel;
        this.scheduler = scheduler;
    }

    /**
     * Evaluates the given {@link Stream} by first evaluating its inner {@link FlowGraph} and then handling
     * the resulting stream as specified by the given {@link Output}.
     * @param stream the {@link Stream} to be evaluated
     */
    @Override
    public <T> void evaluate(Stream<T> stream, Output output) {
        // Evaluate pipeline
        Observable<T> result = evaluate(stream);
        graphs.push(stream.getGraph());
        // Apply output
        try {
            output(output, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Realize all awaiting connections
        while (!connections.isEmpty()) {
            Pair<Observable, Subject> pair = connections.pop();
            pair.fst.subscribe(pair.snd);
        }
        graphs.pop();
    }

    /**
     * ======================================== OUTPUT ========================================
     */
    private <T> void output(Output output, Observable<T> stream) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method evaluator = getClass().getDeclaredMethod("output", output.getClass(), Observable.class);
        evaluator.setAccessible(true);
        try {
            evaluator.invoke(this, output, stream);
        } catch (Exception e) {
            throw e;
        }
    }

    private <T> void output(NoopOutput output, Observable<T> stream) {
        stream.subscribe();
    }
    private <T> void output(SinkOutput<T> output, Observable<T> stream) {
        stream.subscribe(Converter.toObserver(output.getSink()));
    }
    private <T> void output(ActionOutput<T> output, Observable<T> stream) {
        if (output.getCompleteAction() != null)
            stream.subscribe(output.getAction()::call, output.getErrorAction()::call, output.getCompleteAction()::call);
        else if (output.getErrorAction() != null)
            stream.subscribe(output.getAction()::call, output.getErrorAction()::call);
        else
            stream.subscribe(output.getAction()::call);
    }
    private <T> void output(MultipleOutput output, Observable<T> stream) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        for (Output out : output.getOutputs())
            output(out, stream);
    }

    /**
     * ======================================== EVALUATION ========================================
     */

    private <T> Observable<T> evaluate(Stream<T> stream) {
        graphs.push(stream.getGraph());
        Observable<T> ret = evaluate(stream.getToConnect());
        graphs.pop();
        return ret;
    }

    /**
     * Evaluates the given {@link Transformer} by using reflection to determine the Transformer's runtime type
     * and then call the appropriate method.
     * @param expr the {@link Transformer} to be evaluated
     * @return the resulting {@link rx.Observable}
     */
    private <T> Observable<T> evaluate(Transformer<T> expr) {
        // Use reflection to choose suitable method (instead of multiple 'if-else instanceof')
        Observable <T> ret;
        try {
            Method evaluator = getClass().getDeclaredMethod("evaluate", expr.getClass());
            evaluator.setAccessible(true);
            ret = (Observable<T>) evaluator.invoke(this, expr);
        } catch (Exception e) {
            ret = Observable.error(e.getCause());
        }

        // Propagate onComplete to feedback loop's exit point
        if (expr instanceof SingleInputExpr && inner(expr) instanceof ExitPointExpr) {
            Subject subject = evaluated.get(inner(expr));
            ret = ret.doOnCompleted(subject::onCompleted)
                    .doOnUnsubscribe(subject::onCompleted)
                    .doOnTerminate(subject::onCompleted);
        }
        if (expr instanceof MultipleInputExpr) {
            List<Subject> exits = predecessors(expr).stream()
                    .filter(ex -> ex instanceof ExitPointExpr).map(ex -> evaluated.get((ExitPointExpr) ex)).collect(Collectors.toList());
            for (Subject subject : exits)
                ret = ret.doOnCompleted(subject::onCompleted)
                        .doOnUnsubscribe(subject::onCompleted)
                        .doOnTerminate(subject::onCompleted);

        }

        // Observe on multi-threaded scheduler
        if (parallel)
            ret = ret.observeOn(scheduler);

        return ret;
    }

    private <T> Observable<T> evaluate(Observable<T> stream) {
        return stream;
    }

    private <T> Transformer<T> inner(Transformer<T> main) {
        List<Transformer> pred = currentGraph().predecessors(main);
        if (pred.isEmpty())
            throw new RuntimeException("No predecessors in graph for " + main);
        else
            return pred.get(0);
    }
    private <T> List<Transformer> predecessors(Transformer<T> main) {
        return currentGraph().predecessors(main);
    }

    /* ===========================
     * No Input
     * ===========================*/
    private <T> Observable<T> evaluate(FromExpr<T> expr) {
        return Observable.from(expr.getCollection());
    }
    private <T> Observable<T> evaluate(FromSource<T> expr) {
        return Converter.<T>toObservable(expr.getSource());
    }
    private <T> Observable<T> evaluate(FromListener<T> expr) {
        return Observable.create(subscriber -> expr.getSource().register(subscriber::onNext));
    }
    private <T> Observable<T> evaluate(DeferExpr<T> expr) {
        return Observable.defer(() -> evaluate(expr.getStreamFactory().call()));
    }
    private Observable<Long> evaluate(IntervalExpr expr) {
        return Observable.interval(expr.getInterval(), expr.getUnit());
    }
    private <T> Observable<T> evaluate(ErrorExpr<T> expr) {
        return Observable.error(expr.getT());
    }
    private <T> Observable<T> evaluate(NeverExpr<T> expr) {
        return Observable.never();
    }
    private <T> Observable<T> evaluate(EmptyExpr<T> expr) {
        return Observable.empty();
    }
    private <T,Resource> Observable<T> evaluate(UsingExpr<T, Resource> expr) {
        return Observable.using(
                expr.getResourceFactory()::call,
                (resource) -> evaluate(expr.getStreamFactory().call(resource)),
                expr.getDisposeAction()::call);
    }

    /* ===========================
     * Single Input
     * ===========================*/
    private <T> Observable<T> evaluate(EntryPointExpr<T> expr) {
        return evaluate(inner(expr));
    }
    private <T> Observable<T> evaluate(ExitPointExpr<T> expr) {
        if (evaluated.containsKey(expr))
            return evaluated.get(expr);

        // Create Subject to be connected later
        Subject<T, T> toConnect = PublishSubject.create();
        toConnect = toConnect.toSerialized();

        evaluated.put(expr, toConnect);

        // Evaluate predecessor
        Observable<T> inner = evaluate(inner(expr));

        // Stack all connections to be realized when Output has subscribed
        connections.push(new Pair<>(inner, toConnect));

        return toConnect;
    }
    private <T> Observable<T> evaluate(FilterExpr<T> expr) {
        return evaluate(inner(expr)).filter(expr.getPredicate()::call);
    }
    private <T, U> Observable<T> evaluate(TakeUntilExpr<T, U> expr) {
        return evaluate(inner(expr)).takeUntil(evaluate(expr.getOther()));
    }
    private <T> Observable<T> evaluate(TakeWhileExpr<T> expr) {
        return evaluate(inner(expr)).takeWhile(expr.getPredicate()::call);
    }
    private <T, U> Observable<T> evaluate(SkipUntilExpr<T, U> expr) {
        return evaluate(inner(expr)).skipUntil(evaluate(expr.getOther()));
    }
    private <T> Observable<T> evaluate(SkipWhileExpr<T> expr) {
        return evaluate(inner(expr)).skipWhile(expr.getPredicate()::call);
    }
    private <T> Observable<Boolean> evaluate(ExistsExpr<T> expr) {
        return ((Observable<T>) evaluate(inner(expr))).exists(expr.getPredicate()::call);
    }
    private <T> Observable<T> evaluate(OnErrorResumeExpr<T> expr) {
        return evaluate(inner(expr)).onErrorResumeNext(evaluate(expr.getResumeStream()));
    }
    private <T> Observable<T> evaluate(OnErrorReturnExpr<T> expr) {
        return evaluate(inner(expr)).onErrorReturn(expr.getResumeFunction()::call);
    }
    private <T> Observable<T> evaluate(RetryExpr<T> expr) {
        Observable<T> inner = evaluate(inner(expr));
        return (expr.getCount() < 0) ? inner.retry() : inner.retry(expr.getCount());
    }
    private <T> Observable<T> evaluate(TakeExpr<T> expr) {
        int count = expr.getCount();
        Observable<T> inner = evaluate(inner(expr));
        if (count > 0)
            return inner.take(count);
        else if (count < 0)
            return inner.takeLast(-count);
        else
            return Observable.empty();
    }
    private <T> Observable<T> evaluate(SkipExpr<T> expr) {
        int count = expr.getCount();
        Observable<T> inner = evaluate(inner(expr));
        if (count > 0)
            return inner.skip(count);
        else if (count < 0)
            return inner.skipLast(-count);
        else
            return inner;
    }
    private <T> Observable<T> evaluate(DistinctExpr<T> expr) {
        return evaluate(inner(expr)).distinct();
    }
    private <T> Observable<T> evaluate(RepeatExpr<T> expr) {
        Observable<T> inner = evaluate(inner(expr));
        return (expr.getCount() < 0) ? inner.cache().repeat() : inner.cache().repeat(expr.getCount());
    }
    private <T> Observable<T> evaluate(SerializeExpr<T> expr) {
        return evaluate(inner(expr)).serialize();
    }
    private <T> Observable<T> evaluate(CacheExpr<T> expr) {
        return evaluate(inner(expr)).cache();
    }
    private <T> Observable<T> evaluate(Action0Expr<T> expr) {
        Observable<T> inner = evaluate(inner(expr));
        switch (expr.getWhen()) {
            case "onComplete": return inner.doOnCompleted(expr.getAction()::call);
            case "finally": return inner.finallyDo(expr.getAction()::call);
            case "onTerminate": return inner.doOnTerminate(expr.getAction()::call);
            case "onSubscribe": return inner.doOnSubscribe(expr.getAction()::call);
            case "onUnsubscribe": return inner.doOnUnsubscribe(expr.getAction()::call);
            default: return inner;
        }
    }
    private <T> Observable<T> evaluate(Action1Expr<T> expr) {
        Observable<T> inner = evaluate(inner(expr));
        if (expr.getOnErrorAction() != null) return inner.doOnError(expr.getOnErrorAction()::call);
        if (expr.getOnNextAction() != null) return inner.doOnNext(expr.getOnNextAction()::call);
        if (expr.getOnRequestAction() != null) return inner.doOnRequest(expr.getOnRequestAction()::call);
        return inner;
    }
    private <T> Observable<T> evaluate(DelayExpr<T> expr) {
        return evaluate(inner(expr)).delay(expr.getDelay(), expr.getUnit());
    }
    private <T> Observable<T> evaluate(SampleExpr<T> expr) {
        return evaluate(inner(expr)).sample(expr.getTime(), expr.getTimeUnit());
    }
    private <T> Observable<T> evaluate(TimeoutExpr<T> expr) {
        return evaluate(inner(expr)).timeout(expr.getTime(), expr.getTimeUnit());
    }
    private <T> Observable<T> evaluate(BackpressureBufferExpr<T> expr) {
        Observable<T> inner = evaluate(inner(expr));
        if (expr.getCapacity() < 0) return inner.onBackpressureBuffer();
        if (expr.getOnOverflow() == null) return inner.onBackpressureBuffer(expr.getCapacity());
        else return inner.onBackpressureBuffer(expr.getCapacity(), expr.getOnOverflow()::call);
    }
    private <T> Observable<T> evaluate(BackpressureDropExpr<T> expr) {
        Observable<T> inner = evaluate(inner(expr));
        return (expr.getOnDrop() == null) ? evaluate(inner(expr)).onBackpressureDrop() : evaluate(inner(expr)).onBackpressureDrop(expr.getOnDrop()::call);
    }
    private <T> Observable<T> evaluate(BackpressureLatestExpr<T> expr) {
        return evaluate(inner(expr)).onBackpressureLatest();
    }
    private <T> Observable<List<T>> evaluate(BufferExpr<T> expr) {
        Observable<T> inner = ((Observable<T>) evaluate(inner(expr)));
        return (expr.getCount() >= 0) ? inner.buffer(expr.getCount()) : inner.buffer(expr.getTimespan(), expr.getUnit());
    }
    private <T, R> Observable<R> evaluate(MapExpr<T, R> expr) {
        return ((Observable<T>) evaluate(inner(expr))).map(expr.getMapper()::call);
    }
    private <T, R> Observable<R> evaluate(ScanExpr<T, R> expr) {
        return ((Observable<T>) evaluate(inner(expr))).scan(expr.getSeed(), expr.getAccumulator()::call);
    }
    private <T> Observable<T> evaluate(SimpleScanExpr<T> expr) {
        return ((Observable<T>) evaluate(inner(expr))).scan(expr.getAccumulator()::call);
    }
    private <T> Observable<Notification<T>> evaluate(MaterializeExpr<T> expr) {
        return ((Observable<T>) evaluate(inner(expr)))
                .materialize()
                .map(Converter::fromRxNotification);
    }
    private <T> Observable<T> evaluate(DematerializeExpr<T> expr) {
        return ((Observable<Notification<T>>) evaluate(inner(expr)))
                .map(Converter::fromNotification)
                .dematerialize();
    }

    /* ===========================
     * Multiple Input
     * ===========================*/
    private <T> Observable<T> evaluate(MergeExpr<T> expr) {
        Observable<Stream<T>> inner = evaluate(predecessors(expr).get(0));
        Observable<Observable<T>> evaluated = inner.map(this::evaluate);
        return Observable.merge(evaluated);
    }
    private <T> Observable<T> evaluate(MergeMultiExpr<T> expr) {
        List<Observable<T>> sources = new ArrayList<>();
        predecessors(expr).iterator().forEachRemaining(st -> sources.add(evaluate(st)));
        return Observable.merge(sources);
    }
    private <T> Observable<T> evaluate(AmbExpr<T> expr) {
        List<Observable<T>> sources = new ArrayList<>();
        predecessors(expr).iterator().forEachRemaining(st -> sources.add(evaluate(st)));
        return Observable.amb(sources);
    }
    private <T> Observable<T> evaluate(ConcatExpr<T> expr) {
        Observable<Stream<T>> inner = evaluate(predecessors(expr).get(0));
        Observable<Observable<T>> evaluated = inner.map(this::evaluate);
        return Observable.concat(evaluated);
    }
    private <T> Observable<T> evaluate(ConcatMultiExpr<T> expr) {
        List<Observable<T>> sources = new ArrayList<>();
        predecessors(expr).iterator().forEachRemaining(st -> sources.add(evaluate(st)));
        switch (sources.size()) {
            case 0: case 1: throw new RuntimeException("Cannot concat less than 2 streams");
            case 2: return Observable.concat(sources.get(0), sources.get(1));
            case 3: return Observable.concat(sources.get(0), sources.get(1), sources.get(2));
            case 4: return Observable.concat(sources.get(0), sources.get(1), sources.get(2), sources.get(3));
            case 5: return Observable.concat(sources.get(0), sources.get(1), sources.get(2), sources.get(3), sources.get(4));
            case 6: return Observable.concat(sources.get(0), sources.get(1), sources.get(2), sources.get(3), sources.get(4),
                    sources.get(5));
            case 7: return Observable.concat(sources.get(0), sources.get(1), sources.get(2), sources.get(3), sources.get(4),
                    sources.get(5), sources.get(6));
            case 8: return Observable.concat(sources.get(0), sources.get(1), sources.get(2), sources.get(3), sources.get(4),
                    sources.get(5), sources.get(6), sources.get(7));
            case 9: return Observable.concat(sources.get(0), sources.get(1), sources.get(2), sources.get(3), sources.get(4),
                    sources.get(5), sources.get(6), sources.get(7), sources.get(8));
            default: return Observable.concat(Observable.from(sources));
        }
    }
    private <T1,T2,T3,T4,T5,T6,T7,T8,T9,R> Observable<R> evaluate(ZipExpr<T1,T2,T3,T4,T5,T6,T7,T8,T9,R> c) {
        List<Transformer> p = predecessors(c);
        int size = p.size();
        for (int i=0; i<9-size; i++)
            p.add(null);
        Transformer p1 = p.get(0); Transformer p2 = p.get(1); Transformer p3 = p.get(2); Transformer p4 = p.get(3); Transformer p5 = p.get(4);
        Transformer p6 = p.get(5); Transformer p7 = p.get(6); Transformer p8 = p.get(7); Transformer p9 = p.get(8);
        if (c.combiner9 != null) {
            if (Objects.equals(c.type, "zip"))
                return Observable.zip(evaluate(p.get(0)), evaluate(p2), evaluate(p3), evaluate(p4), evaluate(p5), evaluate(p6), evaluate(p7), evaluate(p8), evaluate(p9), c.combiner9::call);
            else
                return Observable.combineLatest(evaluate(p1), evaluate(p2), evaluate(p3), evaluate(p4), evaluate(p5), evaluate(p6), evaluate(p7), evaluate(p8), evaluate(p9), c.combiner9::call);
        }
        if (c.combiner8 != null) {
            if (Objects.equals(c.type, "zip"))
                return Observable.zip(evaluate(p1), evaluate(p2), evaluate(p3), evaluate(p4), evaluate(p5), evaluate(p6), evaluate(p7), evaluate(p8), c.combiner8::call);
            else
                return Observable.combineLatest(evaluate(p1), evaluate(p2), evaluate(p3), evaluate(p4), evaluate(p5), evaluate(p6), evaluate(p7), evaluate(p8), c.combiner8::call);
        }
        if (c.combiner7 != null) {
            if (Objects.equals(c.type, "zip"))
                return Observable.zip(evaluate(p1), evaluate(p2), evaluate(p3), evaluate(p4), evaluate(p5), evaluate(p6), evaluate(p7), c.combiner7::call);
            else
                return Observable.combineLatest(evaluate(p1), evaluate(p2), evaluate(p3), evaluate(p4), evaluate(p5), evaluate(p6), evaluate(p7), c.combiner7::call);
        }
        if (c.combiner6 != null) {
            if (Objects.equals(c.type, "zip"))
                return Observable.zip(evaluate(p1), evaluate(p2), evaluate(p3), evaluate(p4), evaluate(p5), evaluate(p6), c.combiner6::call);
            else
                return Observable.combineLatest(evaluate(p1), evaluate(p2), evaluate(p3), evaluate(p4), evaluate(p5), evaluate(p6), c.combiner6::call);
        }
        if (c.combiner5 != null) {
            if (Objects.equals(c.type, "zip"))
                return Observable.zip(evaluate(p1), evaluate(p2), evaluate(p3), evaluate(p4), evaluate(p5), c.combiner5::call);
            else
                return Observable.combineLatest(evaluate(p1), evaluate(p2), evaluate(p3), evaluate(p4), evaluate(p5), c.combiner5::call);
        }
        if (c.combiner4 != null) {
            if (Objects.equals(c.type, "zip"))
                return Observable.zip(evaluate(p1), evaluate(p2), evaluate(p3), evaluate(p4), c.combiner4::call);
            else
                return Observable.combineLatest(evaluate(p1), evaluate(p2), evaluate(p3), evaluate(p4), c.combiner4::call);
        }
        if (c.combiner3 != null) {
            if (Objects.equals(c.type, "zip"))
                return Observable.zip(evaluate(p1), evaluate(p2), evaluate(p3), c.combiner3::call);
            else
                return Observable.combineLatest(evaluate(p1), evaluate(p2), evaluate(p3), c.combiner3::call);
        }
        if (Objects.equals(c.type, "zip"))
            return Observable.zip(evaluate(p1), evaluate(p2), c.combiner2::call);
        else
            return Observable.combineLatest(evaluate(p1), evaluate(p2), c.combiner2::call);
    }

    private class Pair<T1, T2> {
        public T1 fst;
        public T2 snd;

        public Pair(T1 fst, T2 snd) {
            this.fst = fst;
            this.snd = snd;
        }
    }
}

