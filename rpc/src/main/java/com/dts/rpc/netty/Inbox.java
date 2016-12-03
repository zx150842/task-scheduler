package com.dts.rpc.netty;

import com.dts.rpc.RpcEndpoint;
import com.dts.rpc.netty.message.InboxMessage;
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
        if (message instanceof InboxMessage.AskReplyInboxMessage) {
          InboxMessage.AskReplyInboxMessage msg = (InboxMessage.AskReplyInboxMessage)message;
          try {
            endpoint.receiveAndReply(msg.context);
          } catch (Throwable e) {
            msg.context.sendFailure(e);
            throw e;
          }
        } else if (message instanceof InboxMessage.AskInboxMessage) {
          InboxMessage.AskInboxMessage msg = (InboxMessage.AskInboxMessage)message;
          endpoint.receive(msg.content);
        } else if (message instanceof InboxMessage.OnStart) {
          endpoint.onStart();
        } else if (message instanceof InboxMessage.OnStop) {
          int activeThreads;
          synchronized (this) {
            activeThreads = numActiveThreads;
          }
          assert activeThreads == 1;
          dispatcher.removeRpcEndpointRef(endpoint);
          endpoint.onStop();
          assert isEmpty();
        } else if (message instanceof InboxMessage.RemoteProcessConnected) {
          InboxMessage.RemoteProcessConnected msg = (InboxMessage.RemoteProcessConnected)message;
          endpoint.onConnected(msg.remoteAddress);
        } else if (message instanceof InboxMessage.RemoteProcessDisconnected) {
          InboxMessage.RemoteProcessDisconnected msg = (InboxMessage.RemoteProcessDisconnected)message;
          endpoint.onDisconnected(msg.remoteAddress);
        } else if (message instanceof InboxMessage.RemoteProcessConnectionError) {
          InboxMessage.RemoteProcessConnectionError msg = (InboxMessage.RemoteProcessConnectionError)message;
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
      messages.add(new InboxMessage.OnStop());
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
