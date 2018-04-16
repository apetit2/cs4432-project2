package simpledb.testing;

import org.junit.jupiter.api.Test;
import simpledb.index.extensiblehash.GlobalIndex;
import simpledb.query.Constant;
import simpledb.query.IntConstant;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class GlobalIndexTests {

    @Test
    public void globalIndexTests() {
        //analogous to the driver
        SimpleDB.init("GlobalIndexDB", "LRU");

        //analogous to the connection
        Transaction tx = new Transaction();

        tx.commit();
        GlobalIndex gIndex = new GlobalIndex("test", null, tx);

        //should return empty list because we have closed the tablescan
        System.out.println(gIndex.toString());

        Constant constant = new IntConstant(9);
        gIndex.beforeFirst(constant);

        //should return a string with a local index value of 1 because we still have
        //a global depth of 1
        System.out.println(gIndex.toString());

        constant = new IntConstant(2);
        gIndex.beforeFirst(constant);

        //should return a string with a local index value of 0 because we still have
        //a global depth of 1
        System.out.println(gIndex.toString());

    }
}
