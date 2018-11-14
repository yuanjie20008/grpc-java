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

import com.google.common.util.concurrent.SettableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.internal.AbstractManagedChannelImplBuilder;
import io.grpc.internal.AbstractServerImplBuilder;
import io.grpc.testing.integration.AbstractInteropTest;
import java.io.File;
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
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Interop test for Tomcat server and Netty client.
 */
public class TomcatInteropTest extends AbstractInteropTest {

  public static final class RetryRule implements TestRule {
    private int retryCount;

    public RetryRule(int retryCount) {
      this.retryCount = retryCount;
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          for (int i = 0; i < retryCount; i++) {
            SettableFuture<Void> future = SettableFuture.create();
            new Thread(
                    () -> {
                      try {
                        base.evaluate();
                        future.set(null);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        future.setException(new RuntimeException(e));
                      } catch (Throwable t) {
                        future.setException(t);
                      }
                    })
                .start();
            try {
              future.get(5, TimeUnit.SECONDS);
              return;
            } catch (Throwable t) {
              System.err.println(i + "-th attempt failed");
              t.printStackTrace();
              if (i == retryCount - 1) {
                throw t;
              }
            }

          }
        }
      };
    }
  }

  // Embedded Tomcat is flaky
  //@Rule
  //public final RetryRule retry = new RetryRule(3);

  private static final String HOST = "localhost";
  private static final String MYAPP = "/grpc.testing.TestService";
  private int port;
  private Tomcat tomcat;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    // Logger.getLogger("").setLevel(Level.FINEST);
    // for (Handler handler : LogManager.getLogManager().getLogger("").getHandlers()) {
    //   handler.setFilter(new Filter() {
    //     @Override
    //     public boolean isLoggable(LogRecord logRecord) {
    //       return true;
    //     }
    //   });
    //   handler.setFormatter(new SimpleFormatter());
    //   handler.setLevel(Level.FINEST);
    // }
  }

  @After
  @Override
  public void tearDown() {
    super.tearDown();
    try {
      tomcat.stop();
    } catch (LifecycleException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    FileUtils.deleteDirectory(new File("tomcat.0"));
  }

  @Override
  protected AbstractServerImplBuilder<?> getServerBuilder() {
    return new ServletServerBuilder();
  }

  @Override
  protected void startServer(AbstractServerImplBuilder<?> builer) {
    tomcat = new Tomcat();
    tomcat.setPort(0);
    Context ctx = tomcat.addContext(MYAPP, new File("build/tmp").getAbsolutePath());
    Tomcat.addServlet(ctx, "TomcatInteropTest", new GrpcServlet((ServletServerBuilder) builer))
        .setAsyncSupported(true);
    ctx.addServletMappingDecoded("/*", "TomcatInteropTest");
    tomcat.getConnector().addUpgradeProtocol(new Http2Protocol());
    try {
      tomcat.start();
    } catch (LifecycleException e) {
      throw new RuntimeException(e);
    }

    port = tomcat.getConnector().getLocalPort();
    System.err.println("port " + port);

  }

  @Override
  protected ManagedChannel createChannel() {
    AbstractManagedChannelImplBuilder<?> builder =
        (AbstractManagedChannelImplBuilder<?>) ManagedChannelBuilder.forAddress(HOST, port)
            .usePlaintext()
            .maxInboundMessageSize(AbstractInteropTest.MAX_MESSAGE_SIZE);
    io.grpc.internal.TestingAccessor.setStatsImplementation(
        builder, createClientCensusStatsModule());
    return builder.build();
  }

  @Override
  protected boolean metricsExpected() {
    return false; // otherwise re-test will not work
  }

  // FIXME
  @Override
  @Ignore("Tomcat is broken on client GOAWAY")
  @Test
  public void gracefulShutdown() {}

  // FIXME
  @Override
  @Ignore("Tomcat is not able to send trailer only")
  @Test
  public void specialStatusMessage() {}

  // FIXME
  @Override
  @Ignore("Tomcat is not able to send trailer only")
  @Test
  public void unimplementedMethod() {}

  // FIXME
  @Override
  @Ignore("Tomcat is not able to send trailer only")
  @Test
  public void statusCodeAndMessage() {}
}
