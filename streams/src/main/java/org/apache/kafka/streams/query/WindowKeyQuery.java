package org.apache.kafka.streams.query;

import java.util.Optional;

public class WindowKeyQuery<K, V> implements Query<V> {

  private final K key;
  private final V windowLower;
  private final Optional<V> windowUpper;

  private WindowKeyQuery(final K key, final V windowLower, Optional<V> windowUpper) {
    this.key = key;
    this.windowLower = windowLower;
    this.windowUpper = windowUpper;
  }

  public static <K, V> WindowKeyQuery<K, V> withKey(final K key, final V windowLower) {
    return new WindowKeyQuery<>(key, windowLower, Optional.empty());
  }
  public static <K, V> WindowKeyQuery<K, V> withKeyWindow(final K key, final V windowLower, final V windowUpper) {
    return new WindowKeyQuery<>(key, windowLower, Optional.of(windowUpper));
  }

  public K getKey() {
    return key;
  }

  public V getWindowLower(){
    return windowLower;
  }

  public Optional<V> getWindowUpper(){
    return windowUpper;
  }
}
