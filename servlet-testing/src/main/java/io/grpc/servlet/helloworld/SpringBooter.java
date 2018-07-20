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

package io.grpc.servlet.helloworld;

import io.grpc.examples.helloworld.HelloWorldServer;
import io.grpc.servlet.ServerBuilder;
import io.grpc.servlet.ServletAdapter;
import io.grpc.testing.integration.TestServiceImpl;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@ServletComponentScan
public class SpringBooter {
  public static void main(String[] args) {
    SpringApplication.run(SpringBooter.class, args);
  }

  @Bean
  ServletAdapter getServletAdapter() {
    return ServletAdapterProvider.servletAdapter;
  }

  private static final class ServletAdapterProvider {
    static final ServletAdapter servletAdapter =
        ServletAdapter.Factory.create(
            new ServerBuilder()
                .addService(new HelloWorldServer.GreeterImpl())
                .addService(new TestServiceImpl(Executors.newScheduledThreadPool(2))));
  }
}
