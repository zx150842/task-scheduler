package com.dts.rpc.netty;

import com.dts.rpc.DTSConf;
import com.dts.rpc.RpcEnv;
import com.dts.rpc.RpcEnvConfig;
import com.dts.rpc.RpcEnvFactory;
import com.dts.rpc.exception.DTSException;
import com.dts.rpc.util.SerializerInstance;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;

/**
 * @author zhangxin
 */
public class NettyRpcEnvFactory implements RpcEnvFactory {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Override public RpcEnv create(RpcEnvConfig rpcEnvConfig) {
    DTSConf conf = rpcEnvConfig.conf;
    SerializerInstance serializerInstance = new SerializerInstance(conf);
    NettyRpcEnv nettyRpcEnv = new NettyRpcEnv(conf, serializerInstance, rpcEnvConfig.host);
    if (!rpcEnvConfig.clientMode) {
      try {
        startServiceOnPort(rpcEnvConfig.port, rpcEnvConfig.name, conf, nettyRpcEnv);
      } catch (Throwable e) {
        nettyRpcEnv.shutdown();
        throw new DTSException(e);
      }
    }
    return nettyRpcEnv;
  }

  private int startServiceOnPort(int startPort, String serviceName, DTSConf conf, NettyRpcEnv nettyRpcEnv)
    throws Exception {
    Preconditions.checkArgument(startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65536 (inclusive), or 0 for a random free port.");

    String serviceString = serviceName.isEmpty() ? "" : serviceName;

    int maxRetries = portMaxRetries(conf);
    for (int offset = 0; offset < maxRetries; ++offset) {
      int tryPort = startPort == 0 ? startPort : ((startPort + offset - 1024) % (65536 - 1024) + 1024);
      try {
        nettyRpcEnv.startServer(tryPort);
        logger.info("Successfully started service {} on port {}", serviceString, tryPort);
        return tryPort;
      } catch (Exception e) {
        if (e instanceof BindException) {
          if (offset >= maxRetries) {
            String exceptionMessage = String.format("%s: Service %s failed after "
              + "%s retries! Consider explicitly setting the appropriate port for the "
              + "service %s to an available port or increasing dts.port.maxRetries.",
              e.getMessage(), serviceString, maxRetries, serviceString);
            Exception exception = new BindException(exceptionMessage);
            exception.setStackTrace(e.getStackTrace());
            throw exception;
          }
          logger.warn("Service {} could not bind on port {}. Attempting port {}",
            serviceString, tryPort, tryPort + 1);
        }
      }
    }
    throw new DTSException(String.format("Failed to start service {} on port {}", serviceString, startPort));
  }

  private int portMaxRetries(DTSConf conf) {
    int maxRetries = conf.getInt("dts.port.maxRetries", 16);
    return maxRetries;
  }
}
