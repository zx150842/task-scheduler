package com.dts.rpc.netty;

import com.dts.rpc.DTSConf;
import com.dts.rpc.RpcAddress;
import com.dts.rpc.network.TransportContext;
import com.dts.rpc.network.client.TransportClient;
import com.dts.rpc.network.client.TransportClientFactory;
import com.dts.rpc.network.server.TransportServer;
import com.dts.rpc.network.util.NettyUtils;
import com.dts.rpc.network.util.TransportConf;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.ThreadUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhangxin
 */
public class NettyRpcEnv {

  private final String host;
  private final TransportConf transportConf;
  private final Dispatcher dispatcher;
  private final TransportContext transportContext;
  private final TransportClientFactory clientFactory;

  private final DTSConf conf;

  private TransportServer server;

  private final ThreadPoolExecutor clientConnectionExecutor;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private final Map<RpcAddress, Outbox> outboxes = Maps.newConcurrentMap();

  NettyRpcEnv(DTSConf conf, String host) {
    this.host = host;
    this.conf = conf;
    this.transportConf = conf.getTransportConf("rpc");
    this.dispatcher = new Dispatcher(this);
    this.transportContext =
        new TransportContext(transportConf, new NettyRpcHandler(dispatcher, this));
    this.clientConnectionExecutor = NettyUtils.newDaemonCachedThreadPool("netty-rpc-connection",
        conf.getInt("dts.rpc.connect.threads", 64), 60);
    this.clientFactory = transportContext.createClientFactory();
  }

  public void startServer(int port) {
    server = transportContext.createServer(host, port);
    // TODO register to dispatcher
  }

  public DTSConf conf() {
    return conf;
  }

  public RpcAddress address() {
    if (server != null) {
      return new RpcAddress(host, server.getPort());
    }
    return null;
  }

  public ThreadPoolExecutor clientConnectionExecutor() { return clientConnectionExecutor; }

  public TransportClient createClient(RpcAddress address) throws IOException {
    return clientFactory.createClient(new InetSocketAddress(address.getHost(), address.getPort()));
  }

  public void removeOutbox(RpcAddress address) {
    Outbox outbox = outboxes.remove(address);
    if (outbox != null) {
      outbox.stop();
    }
  }
}
