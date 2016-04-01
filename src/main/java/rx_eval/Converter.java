package rx_eval;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.rhea_core.internal.Notification;
import rx.Observable;
import rx.Observer;
import rx.RxReactiveStreams;

/**
 * @author Orestis Melkonian
 */
public abstract class Converter {
    public static <T> Observable<T> toObservable(Publisher<T> publisher) {
        return RxReactiveStreams.toObservable(publisher);
    }

    public static <T> Publisher<T> fromObservable(Observable<T> observable) {
        return RxReactiveStreams.toPublisher(observable);
    }

    public static <T> Observer<T> toObserver(Subscriber<T> subscriber) { return new ObserverAdapter<>(subscriber); }

    public static <T> Subscriber<T> fromObserver(Observer<T> observer) {
        return new SubscriberAdapter<>(observer);
    }

    private static class ObserverAdapter<T> implements Observer<T> {
        Subscriber<T> subscriber;

        public ObserverAdapter(Subscriber<T> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void onCompleted() {
            subscriber.onComplete();
        }

        @Override
        public void onError(Throwable e) {
            subscriber.onError(e);
        }

        @Override
        public void onNext(T o) {
            subscriber.onNext(o);
        }
    }

    private static class SubscriberAdapter<T> implements Subscriber<T> {
        Observer<T> observer;

        public SubscriberAdapter(Observer<T> observer) {
            this.observer = observer;
        }

        @Override
        public void onSubscribe(Subscription s) {}

        @Override
        public void onNext(T o) {
            observer.onNext(o);
        }

        @Override
        public void onError(Throwable t) {
            observer.onError(t);
        }

        @Override
        public void onComplete() {
            observer.onCompleted();
        }
    }

    public static <T> Notification<T> fromRxNotification(rx.Notification<T> not) {
        rx.Notification.Kind kind = not.getKind();
        if (kind == rx.Notification.Kind.OnNext)
            return Notification.createOnNext(not.getValue());
        else if (kind == rx.Notification.Kind.OnError)
            return Notification.createOnError(not.getThrowable());
        else
            return Notification.createOnCompleted();
    }
    public static <T> rx.Notification<T> fromNotification(Notification<T> not) {
        Notification.Kind kind = not.getKind();
        if (kind == Notification.Kind.OnNext)
            return rx.Notification.createOnNext(not.getValue());
        else if (kind == Notification.Kind.OnError)
            return rx.Notification.createOnError(not.getThrowable());
        else
            return rx.Notification.createOnCompleted();
    }
}
