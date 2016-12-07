package com.dts.rpc.netty;

import com.dts.rpc.RpcAddress;
import com.dts.rpc.network.client.TransportClient;
import com.google.common.collect.Lists;

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
  private Outbox outbox;
  private boolean draining = false;

  public Outbox(NettyRpcEnv nettyRpcEnv, RpcAddress rpcAddress) {
    this.nettyRpcEnv = nettyRpcEnv;
    this.rpcAddress = rpcAddress;
    this.outbox = this;
  }

  public void send(OutboxMessage message) {
    boolean dropped;
    synchronized (outbox) {
      if (stopped) {
        dropped = true;
      } else {
        messages.add(message);
        dropped = false;
      }
    }
    if (dropped) {
      message.onFailure(new RuntimeException("Message is dropped because Outbox is stopped"));
    } else {
      drainOutbox();
    }
  }

  private void drainOutbox() {
    synchronized (outbox) {
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
      draining = true;
    }
    do {
      OutboxMessage message = messages.poll();
      if (message == null) {
        return;
      }
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
      synchronized (outbox) {
        if (stopped) {
          return;
        }
      }
    } while (true);
  }

  private void launchConnectTask() {
    connectFuture = nettyRpcEnv.clientConnectionExecutor().submit(new Callable<Void>() {
      @Override
      public Void call() {
        try {
          TransportClient client = nettyRpcEnv.createClient(rpcAddress);
          synchronized (outbox) {
            outbox.client = client;
            if (stopped) {
              closeClient();
            }
          }
        } catch (IOException e) {
          synchronized (outbox) {
            connectFuture = null;
          }
          handleNetworkFailure(e);
          return null;
        }
        synchronized (outbox) {
          connectFuture = null;
        }
        drainOutbox();
        return null;
      }
    });
  }

  private void handleNetworkFailure(Throwable e) {
    synchronized (outbox) {
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
    synchronized (outbox) {
      if (stopped) {
        return;
      }
      stopped = true;
      if (connectFuture != null) {
        connectFuture.cancel(true);
        closeClient();
      }
    }
    OutboxMessage message;
    do {
      message = messages.poll();
      message.onFailure(new RuntimeException("Message is dropped because Outbox is stopped"));
    } while (message != null);
  }

  private synchronized void closeClient() {
    client = null;
  }
}
