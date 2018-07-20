package io.grpc.servlet.helloworld;

import io.grpc.examples.helloworld.HelloWorldServer;
import io.grpc.servlet.ServerBuilder;
import io.grpc.servlet.ServletAdapter;
import io.grpc.testing.integration.TestServiceImpl;
import java.util.concurrent.Executors;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class ServletAdapterProvider {
  @Produces
  ServletAdapter getServletAdapter() {
    return Provider.servletAdapter;
  }

  private static final class Provider {
    static final ServletAdapter servletAdapter =
        ServletAdapter.Factory.create(
            new ServerBuilder()
                .addService(new HelloWorldServer.GreeterImpl())
                .addService(new TestServiceImpl(Executors.newScheduledThreadPool(2))));
  }
}
