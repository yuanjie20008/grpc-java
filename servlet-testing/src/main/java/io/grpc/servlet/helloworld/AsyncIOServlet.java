package io.grpc.servlet.helloworld;

import java.io.IOException;
import javax.servlet.AsyncContext;
import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(urlPatterns={"/asyncioservlet"}, asyncSupported=true)
public class AsyncIOServlet extends HttpServlet {
  @Override
  public void doPost(HttpServletRequest request,
      HttpServletResponse response)
      throws IOException {
    final AsyncContext acontext = request.startAsync();
    final ServletInputStream input = request.getInputStream();



    input.setReadListener(new ReadListener() {
      byte buffer[] = new byte[4*1024];
      StringBuilder sbuilder = new StringBuilder();
      @Override
      public void onDataAvailable() {
        try {
          do {
            System.out.println("input.isReady():" + input.isReady());
            System.out.println("input.isFinished():" + input.isFinished());
            int length = input.read(buffer);
            sbuilder.append(new String(buffer, 0, length));
          } while(input.isReady());
        } catch (IOException ex) {

        } catch (RuntimeException e) {
          e.printStackTrace();
        }

      }
      @Override
      public void onAllDataRead() {
        try {
          acontext.getResponse().getWriter()
              .write("...the response...");
        } catch (IOException ex) { }
        acontext.complete();
      }
      @Override
      public void onError(Throwable t) {  }
    });

    System.out.println("listener set, input.isReady():" + input.isReady());
    System.out.println("listener set, input.isFinished():" + input.isFinished());
  }
}