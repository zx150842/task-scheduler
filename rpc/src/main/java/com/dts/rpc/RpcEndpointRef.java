package com.dts.rpc;

import com.dts.rpc.netty.NettyRpcEnv;
import org.slf4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author zhangxin
 */
public abstract class RpcEndpointRef {

  protected final int maxRetries;
  protected final long retryWaitMs;
  protected long defaultAskTimeoutMs;

  public RpcEndpointRef(DTSConf conf) {
    this.maxRetries = conf.getInt("master.rpc.maxRetries", 3);
    this.retryWaitMs = conf.getLong("master.rpc.maxWaitMs", 3000);
    this.defaultAskTimeoutMs = conf.getLong("master.rpc.askTimeoutMs", 120 * 1000);
  }

  public abstract RpcAddress address();
  public abstract String name();

  public abstract void send(Object message);

  public abstract <T> Future<T> ask(T message, long timeoutMs);

  protected abstract Logger logger();

  public <T> Object askWithRetry(T message, long timeoutMs) {
    int attempts = 0;
    Exception lastException = null;
    while (attempts < maxRetries) {
      ++attempts;
      try {
        Future<T> future = ask(message, timeoutMs);
        T result = future.get(timeoutMs, TimeUnit.MILLISECONDS);
        if (result == null) {
          throw new RuntimeException("RpcEndpoint returned null");
        }
        return result;
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException | TimeoutException e) {
        lastException = e;
        logger().warn("Error sending message {} in {} attempts", message, attempts, e);
      }
      if (attempts < maxRetries) {
        try {
          Thread.sleep(retryWaitMs);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    throw new RuntimeException("Error sending message " + message, lastException);
  }
}
