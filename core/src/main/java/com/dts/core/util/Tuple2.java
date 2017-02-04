package com.dts.core.util;

import com.google.common.base.Objects;

/**
 * @author zhangxin
 */
public class Tuple2<T1, T2> {

  public final T1 _1;
  public final T2 _2;

  public Tuple2(T1 t1, T2 t2) {
    this._1 = t1;
    this._2 = t2;
  }

  public T1 get_1() {
    return _1;
  }

  public T2 get_2() {
    return _2;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(_1, _2);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Tuple2) {
      Tuple2 o = (Tuple2)other;
      return _1.equals(o._1) && _2.equals(o._2);
    }
    return false;
  }
}
