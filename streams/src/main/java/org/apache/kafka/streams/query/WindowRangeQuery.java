package org.apache.kafka.streams.query;

import java.util.Optional;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;

public class WindowRangeQuery<K, V> implements Query<KeyValueIterator<Windowed<K>, V>> {

  private final K lower;
  private final Optional<K> upper;
  private final V windowLower;
  private final V windowUpper;

  private WindowRangeQuery(final K lower, final Optional<K> upper, final V windowLower, final V windowUpper) {
    this.lower = lower;
    this.upper = upper;
    this.windowLower = windowLower;
    this.windowUpper = windowUpper;
  }

  public static <U,T> WindowRangeQuery<U,T> fromKey(final U key, final T windowLower, final T windowUpper){
    return new WindowRangeQuery<U,T>(key, Optional.empty(), windowLower, windowUpper);
  }

  public static <U,T> WindowRangeQuery<U,T> fromRange(final U lower, U upper, final T windowLower, final T windowUpper){
    return new WindowRangeQuery<U,T>(lower, Optional.of(upper), windowLower, windowUpper);
  }

  public K getLowerBound() {
    return lower;
  }

  public Optional<K> getUpperBound() {
    return upper;
  }

  public V getWindowLowerBound() {
    return windowLower;
  }

  public V getWindowUpperBound() {
    return windowUpper;
  }
}