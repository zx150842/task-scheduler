package com.dts.core.rpc;

import com.google.common.collect.Lists;

import com.dts.core.rpc.RpcAddress;
import com.dts.core.rpc.RpcCallContext;
import com.dts.core.rpc.RpcEndpoint;
import com.dts.core.util.Tuple2;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author zhangxin
 */
public class TestRpcEndpoint extends RpcEndpoint {

  private List<Object> receiveMessages = Lists.newArrayList();
  private List<Object> receiveAndReplyMessages = Lists.newArrayList();
  private List<RpcAddress> onConnectedMessages = Lists.newArrayList();
  private List<RpcAddress> onDisconnectedMessages = Lists.newArrayList();
  private List<Tuple2<Throwable, RpcAddress>> onNetworkErrorMessages = Lists.newArrayList();
  private boolean started = false;
  private boolean stopped = false;

  public TestRpcEndpoint() {
    super(null);
  }

  @Override public void receive(Object o, RpcAddress senderAddress) {
    receiveMessages.add(o);
  }

  @Override public void receiveAndReply(Object o, RpcCallContext context) {
    receiveAndReplyMessages.add(o);
  }

  @Override
  public void onConnected(RpcAddress remoteAddress) {
    onConnectedMessages.add(remoteAddress);
  }

  @Override
  public void onNetworkError(Throwable cause, RpcAddress remoteAddress) {
    onNetworkErrorMessages.add(new Tuple2<>(cause, remoteAddress));
  }

  @Override
  public void onDisconnected(RpcAddress remoteAddress) {
    onDisconnectedMessages.add(remoteAddress);
  }

  public int numReceiveMessages = receiveMessages.size();

  @Override
  public void onStart() {
    started = true;
  }

  @Override
  public void onStop() {
    stopped = true;
  }

  public void verifyStarted() {
    assertTrue("RpcEndpoint is not started", started);
  }

  public void verifyStopped() {
    assertTrue("RpcEndpoint is not stopped", stopped);
  }

  public void verifyReceiveMessages(List<Object> expected) {
    assertEquals(receiveMessages, expected);
  }

  public void verifySingleReceiveMessage(Object message) {
    verifyReceiveMessages(Lists.newArrayList(message));
  }

  public void verifyReceiveAndReplyMessages(List<Object> expected) {
    assertEquals(receiveAndReplyMessages, expected);
  }

  public void verifySingleReceiveAndReplyMessage(Object message) {
    verifyReceiveAndReplyMessages(Lists.newArrayList(message));
  }

  public void verifySingleOnConnectedMessage(RpcAddress remoteAddress) {
    verifyOnConnectedMessages(Lists.newArrayList(remoteAddress));
  }

  public void verifyOnConnectedMessages(List<RpcAddress> expected) {
    assertEquals(onConnectedMessages, expected);
  }

  public void verifySingleOnDisconnectedMessage(RpcAddress remoteAddress) {
    verifyOnDisconnectedMessages(Lists.newArrayList(remoteAddress));
  }

  public void verifyOnDisconnectedMessages(List<RpcAddress> expected) {
    assertEquals(onDisconnectedMessages, expected);
  }

  public void verifySingleOnNetworkErrorMessage(Throwable cause, RpcAddress remoteAddress) {
    verifyOnNetworkErrorMessages(Lists.newArrayList(new Tuple2<>(cause, remoteAddress)));
  }

  public void verifyOnNetworkErrorMessages(List<Tuple2<Throwable, RpcAddress>> expected) {
    assertEquals(onNetworkErrorMessages, expected);
  }
}
