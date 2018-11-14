/*
 * Copyright 2018 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.servlet;

import io.grpc.InternalChannelz.SocketStats;
import io.grpc.InternalInstrumented;
import io.grpc.ServerStreamTracer.Factory;
import io.grpc.internal.ClientTransportFactory;
import io.grpc.internal.FakeClock;
import io.grpc.internal.InternalServer;
import io.grpc.internal.ManagedClientTransport;
import io.grpc.internal.ServerListener;
import io.grpc.internal.ServerTransportListener;
import io.grpc.internal.testing.AbstractTransportTest;
import io.grpc.netty.InternalNettyTestAccessor;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.servlet.ServletServerBuilder.ServerTransportImpl;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Filter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.apache.coyote.http2.Http2Protocol;
import org.junit.Before;

/**
 * Transport test for Tomcat server and Netty client.
 */
public class TomcatTransportTest extends AbstractTransportTest {
  private static final String HOST = "localhost";
  private static final String MYAPP = "/service";

  private final FakeClock fakeClock = new FakeClock();

  private Tomcat tomcat;
  private int port;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    Logger.getLogger("").setLevel(Level.FINEST);
    for (Handler handler : LogManager.getLogManager().getLogger("").getHandlers()) {
      handler.setFilter(new Filter() {
        @Override
        public boolean isLoggable(LogRecord logRecord) {
          return true;
        }
      });
      handler.setFormatter(new SimpleFormatter());
      handler.setLevel(Level.FINEST);
    }
  }

  @Override
  protected InternalServer newServer(List<Factory> streamTracerFactories) {
    return new InternalServer() {
      final InternalServer delegate =
          new ServletServerBuilder().buildTransportServer(streamTracerFactories);

      @Override
      public void start(ServerListener listener) throws IOException {
        delegate.start(listener);
        ScheduledExecutorService scheduler = fakeClock.getScheduledExecutorService();
        ServerTransportListener serverTransportListener =
            listener.transportCreated(new ServerTransportImpl(scheduler, true));
        ServletAdapter adapter =
            new ServletAdapter(serverTransportListener, streamTracerFactories);
        GrpcServlet grpcServlet = new GrpcServlet(adapter);

        tomcat = new Tomcat();
        tomcat.setPort(0);
        Context ctx = tomcat.addContext(MYAPP, new File("build/tmp").getAbsolutePath());
        Tomcat.addServlet(ctx, "TomcatTransportTest", grpcServlet)
            .setAsyncSupported(true);
        ctx.addServletMappingDecoded("/*", "TomcatTransportTest");
        tomcat.getConnector().addUpgradeProtocol(new Http2Protocol());
        try {
          tomcat.start();
        } catch (LifecycleException e) {
          throw new RuntimeException(e);
        }

        port = tomcat.getConnector().getLocalPort();
      }

      @Override
      public void shutdown() {
        delegate.shutdown();
      }

      @Override
      public int getPort() {
        return delegate.getPort();
      }

      @Override
      public List<InternalInstrumented<SocketStats>> getListenSockets() {
        return delegate.getListenSockets();
      }
    };
  }

  @Override
  protected InternalServer newServer(InternalServer server, List<Factory> streamTracerFactories) {
    return null;
  }

  @Override
  protected ManagedClientTransport newClientTransport(InternalServer server) {
    NettyChannelBuilder nettyChannelBuilder = NettyChannelBuilder
        // Although specified here, address is ignored because we never call build.
        .forAddress("localhost", 0)
        .flowControlWindow(65 * 1024)
        .negotiationType(NegotiationType.PLAINTEXT);
    InternalNettyTestAccessor
        .setTransportTracerFactory(nettyChannelBuilder, fakeClockTransportTracer);
    ClientTransportFactory clientFactory =
        InternalNettyTestAccessor.buildTransportFactory(nettyChannelBuilder);
    return clientFactory.newClientTransport(
        new InetSocketAddress("localhost", port),
        new ClientTransportFactory.ClientTransportOptions()
            .setAuthority(testAuthority(server)));
  }

  @Override
  protected String testAuthority(InternalServer server) {
    return "localhost:" + port;
  }

  @Override
  protected void advanceClock(long offset, TimeUnit unit) {
    fakeClock.forwardNanos(unit.toNanos(offset));
  }

  @Override
  protected long fakeCurrentTimeNanos() {
    return fakeClock.getTicker().read();
  }

}
