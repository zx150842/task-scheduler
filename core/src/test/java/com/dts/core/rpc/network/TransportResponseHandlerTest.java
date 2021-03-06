package com.dts.core.rpc.network;

import com.dts.core.rpc.network.buffer.NioManagedBuffer;
import com.dts.core.rpc.network.client.RpcResponseCallback;
import com.dts.core.rpc.network.client.TransportResponseHandler;
import com.dts.core.rpc.network.protocol.RpcFailure;
import com.dts.core.rpc.network.protocol.RpcResponse;

import org.junit.Test;

import java.nio.ByteBuffer;

import io.netty.channel.local.LocalChannel;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author zhangxin
 */
public class TransportResponseHandlerTest {

  @Test
  public void handleSuccessfulRPC() throws Exception {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    handler.addRpcRequest(12345, callback);
    assert 1 == handler.numOutstandingRequests();

    handler.handle(new RpcResponse(54321, new NioManagedBuffer(ByteBuffer.allocate(7))));
    assert 1 == handler.numOutstandingRequests();

    ByteBuffer resp = ByteBuffer.allocate(10);
    handler.handle(new RpcResponse(12345, new NioManagedBuffer(resp)));
    verify(callback, times(1)).onSuccess(eq(ByteBuffer.allocate(10)));
    assert 0 == handler.numOutstandingRequests();
  }

  @Test
  public void handleFailedRPC() throws Exception {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    handler.addRpcRequest(12345, callback);
    assert 1 == handler.numOutstandingRequests();

    handler.handle(new RpcFailure(54321, "uh-oh"));
    assert 1 == handler.numOutstandingRequests();

    handler.handle(new RpcFailure(12345, "oh no"));
    verify(callback, times(1)).onFailure((Throwable) any());
    assert 0 == handler.numOutstandingRequests();
  }
}
