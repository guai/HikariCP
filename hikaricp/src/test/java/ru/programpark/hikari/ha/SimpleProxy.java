package ru.programpark.hikari.ha;

import lombok.SneakyThrows;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayDeque;

/**
 * This class implements a simple single-threaded proxy server.
 **/
public class SimpleProxy extends Thread
{

   private final String host;
   private final int remoteport;
   private final int localport;
   private final ArrayDeque<Closeable> sockets = new ArrayDeque<>();

   public SimpleProxy(String host, int remoteport, int localport, boolean daemon)
   {
      this.host = host;
      this.remoteport = remoteport;
      this.localport = localport;
      setName("Proxy main thread host = " + host + ", remoteport = " + remoteport + ", localport = " + localport);
      setDaemon(daemon);
   }

   public SimpleProxy(String host, int remoteport, int localport)
   {
      this(host, remoteport, localport, true);
   }

   @Override
   @SneakyThrows
   public void run()
   {
      try(ServerSocket listeningSocket = new ServerSocket(localport)) {

         while (true) {
            try {
               final Socket client = listeningSocket.accept();

               sockets.add(client);

               final InputStream from_client = client.getInputStream();
               final OutputStream to_client = client.getOutputStream();

               final Socket server = new Socket(host, remoteport);

               final InputStream from_server = server.getInputStream();
               final OutputStream to_server = server.getOutputStream();

               sockets.add(server);

               final Thread t1 = new Thread("Proxy worker")
               {
                  @SneakyThrows public void run()
                  {
                     final byte[] buffer = new byte[4096];
                     int bytes_read;
                     try {
                        while (true) {
                           bytes_read = from_client.read(buffer);
                           to_server.write(buffer, 0, bytes_read);
                           to_server.flush();
                        }
                     }
                     catch (IOException e) {
                        System.err.println(e.getMessage());
                     }
                  }
               };
               t1.setDaemon(true);
               t1.start();

               final Thread t2 = new Thread("Proxy worker")
               {
                  @SneakyThrows public void run()
                  {
                     byte[] buffer = new byte[4096];
                     int bytes_read;
                     try {
                        while (true) {
                           bytes_read = from_server.read(buffer);
                           to_client.write(buffer, 0, bytes_read);
                           to_client.flush();
                           Thread.sleep(0);
                        }
                     }
                     catch (IOException e) {
                        System.err.println(e.getMessage());
                     }
                  }
               };
               t2.setDaemon(true);
               t2.start();

            }

            catch (IOException e) {
               e.printStackTrace(System.err);
            }
         }
      } finally {
      }
   }

   SimpleProxy started()
   {
      start();
      return this;
   }

   @Override
   public void interrupt()
   {
      for (Closeable socket : sockets)
         try {
            socket.close();
         }
         catch (IOException e) {
            System.err.println(e.getMessage());
         }
      super.interrupt();
   }

   public static void main(String... args) {
      if(args.length != 3) {
         System.out.println("params: host port local_port");
         return;
      }
      String host = args[0];
      int port = Integer.parseInt(args[1]);
      int localPort = Integer.parseInt(args[2]);
      new SimpleProxy(host, port, localPort, false).started();
   }

}
