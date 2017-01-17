package com.dts.rpc.util;

import org.apache.commons.lang3.tuple.Pair;

/**
 * @author zhangxin
 */
public class TestPair<L, R> extends Pair {

  private L left;
  private R right;

  public TestPair(L left, R right) {
    this.left = left;
    this.right = right;
  }

  @Override public L getLeft() {
    return left;
  }

  @Override public R getRight() {
    return right;
  }

  @Override public int compareTo(Pair o) {
    throw new RuntimeException("TestPair.compareTo method is not implement");
  }

  @Override public Object setValue(Object value) {
    throw new RuntimeException("TestPair.setValue method is not implement");
  }
}
