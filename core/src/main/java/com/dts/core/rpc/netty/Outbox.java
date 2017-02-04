package com.dts.core.rpc.netty;

import com.google.common.collect.Lists;

import com.dts.core.exception.DTSException;
import com.dts.core.rpc.RpcAddress;
import com.dts.core.rpc.network.client.TransportClient;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * @author zhangxin
 */
public class Outbox {
  private final NettyRpcEnv nettyRpcEnv;
  private final RpcAddress rpcAddress;

  private final LinkedList<OutboxMessage> messages = Lists.newLinkedList();
  private TransportClient client = null;
  private boolean stopped = false;
  private Future connectFuture = null;
  private boolean draining = false;

  public Outbox(NettyRpcEnv nettyRpcEnv, RpcAddress rpcAddress) {
    this.nettyRpcEnv = nettyRpcEnv;
    this.rpcAddress = rpcAddress;
  }

  public void send(OutboxMessage message) {
    boolean dropped;
    synchronized (this) {
      if (stopped) {
        dropped = true;
      } else {
        messages.add(message);
        dropped = false;
      }
    }
    if (dropped) {
      message.onFailure(new DTSException("Message is dropped because Outbox is stopped"));
    } else {
      drainOutbox();
    }
  }

  private void drainOutbox() {
    OutboxMessage message;
    synchronized (this) {
      if (stopped) {
        return;
      }
      if (connectFuture != null) {
        return;
      }
      if (client == null) {
        launchConnectTask();
        return;
      }
      if (draining) {
        return;
      }
      message = messages.poll();
      if (message == null) {
        return;
      }
      draining = true;
    }
    while (true) {
      try {
        synchronized (client) {
          if (client != null) {
            message.sendWith(client);
          } else {
            assert stopped = true;
          }
        }
      } catch (Throwable e) {
        handleNetworkFailure(e);
        return;
      }
      synchronized (this) {
        if (stopped) {
          return;
        }
        message = messages.poll();
        if (message == null) {
          draining = false;
          return;
        }
      }
    }
  }

  private void launchConnectTask() {
    Outbox outbox = this;
    connectFuture = nettyRpcEnv.clientConnectionExecutor().submit(new Callable<Void>() {
      @Override
      public Void call() {
        try {
          TransportClient client = nettyRpcEnv.createClient(rpcAddress);
          synchronized (this) {
            outbox.client = client;
            if (stopped) {
              closeClient();
            }
          }
        } catch (IOException e) {
          synchronized (this) {
            connectFuture = null;
          }
          handleNetworkFailure(e);
          return null;
        }
        synchronized (this) {
          connectFuture = null;
        }
        drainOutbox();
        return null;
      }
    });
  }

  private void handleNetworkFailure(Throwable e) {
    synchronized (this) {
      assert connectFuture == null;
      if (stopped) {
        return;
      }
      stopped = true;
      closeClient();
    }
    nettyRpcEnv.removeOutbox(rpcAddress);
    OutboxMessage message;
    do {
      message = messages.poll();
      message.onFailure(e);
    } while (message != null);
    assert messages.isEmpty();
  }

  public void stop() {
    synchronized (this) {
      if (stopped) {
        return;
      }
      stopped = true;
      if (connectFuture != null) {
        connectFuture.cancel(true);
        closeClient();
      }
    }
    OutboxMessage message = messages.poll();
    while (message != null) {
      message.onFailure(new DTSException("Message is dropped because Outbox is stopped"));
      message = messages.poll();
    }
  }

  private synchronized void closeClient() {
    client = null;
  }
}
