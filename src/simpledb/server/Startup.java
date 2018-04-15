package simpledb.server;

import simpledb.buffer.BasicBufferMgr;
import simpledb.remote.*;
import java.rmi.registry.*;

public class Startup {

   public static void main(String args[]) throws Exception {
      // configure and initialize the database
      System.out.println(args[1]);

      //=================================CS4432-Project1================
      //Added a program argument to select between clock, lru, and if
      //further revisions call for it, another replacement policy
      SimpleDB.init(args[0], args[1]);
      
      // create a registry specific for the server on the default port
      Registry reg = LocateRegistry.createRegistry(1099);
      
      // and post the server entry in it
      RemoteDriver d = new RemoteDriverImpl();
      reg.rebind("simpledb", d);
      
      System.out.println("database server ready");
   }
}
