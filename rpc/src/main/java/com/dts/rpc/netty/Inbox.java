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

  public Inbox(NettyRpcEndpointRef endpointRef, RpcEndpoint endpoint) {
    this.endpointRef = endpointRef;
    this.endpoint = endpoint;
  }

  public void process(Dispatcher dispatcher) {
    do {
      InboxMessage message;
      synchronized (this) {
        message = messages.poll();
        if (message != null) {
          numActiveThreads++;
        } else {
          return;
        }
      }
      try {
        if (message instanceof RpcInboxMessage) {
          RpcInboxMessage msg = (RpcInboxMessage)message;
          try {
            endpoint.receiveAndReply(msg.context);
          } catch (Throwable e) {
            msg.context.sendFailure(e);
            throw e;
          }
        } else if (message instanceof OneWayInboxMessage) {
          OneWayInboxMessage msg = (OneWayInboxMessage)message;
          endpoint.receive(msg.content);
        } else if (message instanceof OnStart) {
          endpoint.onStart();
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
    } while(true);
  }

  public synchronized void stop() {
    if (!stopped) {
      stopped = true;
      messages.add(new OnStop());
    }
  }

  public synchronized boolean isEmpty() {
    return messages.isEmpty();
  }

  public synchronized void post(InboxMessage message) {
    if (stopped) {
      logger.warn("Drop {} because {} is stopped", message, endpointRef);
    } else {
      messages.add(message);
    }
  }
}
