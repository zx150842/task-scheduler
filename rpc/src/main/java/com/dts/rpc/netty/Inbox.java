package com.dts.rpc.netty;

import com.dts.rpc.RpcEndpoint;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * @author zhangxin
 */
public class Inbox {
  private final Logger logger = LoggerFactory.getLogger(Inbox.class);

  private final NettyRpcEndpointRef endpointRef;
  private final RpcEndpoint endpoint;

  private final LinkedList<InboxMessage> messages = Lists.newLinkedList();
  private boolean stopped = false;
  private int numActiveThreads = 0;

  private boolean enableConcurrent = false;

  public Inbox(NettyRpcEndpointRef endpointRef, RpcEndpoint endpoint) {
    this.endpointRef = endpointRef;
    this.endpoint = endpoint;

    messages.add(new OnStart());
  }

  public void process(Dispatcher dispatcher) {

    InboxMessage message;
    synchronized (this) {
      if (!enableConcurrent && numActiveThreads != 0) {
        return;
      }
      message = messages.poll();
      if (message != null) {
        numActiveThreads++;
      } else {
        return;
      }
    }
    while (true) {
      try {
        if (message instanceof RpcInboxMessage) {
          RpcInboxMessage msg = (RpcInboxMessage)message;
          try {
            endpoint.receiveAndReply(msg.content, msg.context);
          } catch (Throwable e) {
            msg.context.sendFailure(e);
            throw e;
          }
        } else if (message instanceof OneWayInboxMessage) {
          OneWayInboxMessage msg = (OneWayInboxMessage)message;
          endpoint.receive(msg.content);
        } else if (message instanceof OnStart) {
          endpoint.onStart();
          synchronized (this) {
            if (!stopped) {
              enableConcurrent = true;
            }
          }
        } else if (message instanceof OnStop) {
          int activeThreads;
          synchronized (this) {
            activeThreads = numActiveThreads;
          }
          assert activeThreads == 1;
          dispatcher.removeRpcEndpointRef(endpoint);
          endpoint.onStop();
          assert isEmpty();
        } else if (message instanceof RemoteProcessConnected) {
          RemoteProcessConnected msg = (RemoteProcessConnected)message;
          endpoint.onConnected(msg.remoteAddress);
        } else if (message instanceof RemoteProcessDisconnected) {
          RemoteProcessDisconnected msg = (RemoteProcessDisconnected)message;
          endpoint.onDisconnected(msg.remoteAddress);
        } else if (message instanceof RemoteProcessConnectionError) {
          RemoteProcessConnectionError msg = (RemoteProcessConnectionError)message;
          endpoint.onNetworkError(msg.cause, msg.remoteAddress);
        }
      } catch (Throwable e) {
        try {
          endpoint.onError(e);
        } catch (Throwable ee) {
          logger.error("Ignoring error", ee);
        }
      }
      synchronized (this) {
        if (!enableConcurrent && numActiveThreads != 1) {
          numActiveThreads--;
          return;
        }
        message = messages.poll();
        if (message == null) {
          numActiveThreads--;
          return;
        }
      }
    }
  }

  public synchronized void stop() {
    if (!stopped) {
      enableConcurrent = false;
      stopped = true;
      messages.add(new OnStop());
    }
  }

  public synchronized boolean isEmpty() {
    return messages.isEmpty();
  }

  public synchronized void post(InboxMessage message) {
    if (stopped) {
      onDrop(message);
    } else {
      messages.add(message);
    }
  }

  protected void onDrop(InboxMessage message) {
    logger.warn("Drop {} because {} is stopped", message, endpointRef);
  }
}
