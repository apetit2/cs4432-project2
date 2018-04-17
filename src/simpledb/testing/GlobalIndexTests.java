package simpledb.testing;

import org.junit.jupiter.api.Test;
import simpledb.index.extensiblehash.GlobalIndex;
import simpledb.query.Constant;
import simpledb.query.IntConstant;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import static java.sql.Types.INTEGER;

public class GlobalIndexTests {

    @Test
    public void globalIndexTests() {
        //analogous to the driver
        SimpleDB.init("GlobalIndexDB", "LRU");

        //analogous to the connection
        Transaction tx = new Transaction();

        tx.commit();

        Schema sch = new Schema();
        sch.addIntField("block");
        sch.addIntField("id");
        sch.addField("dataval", INTEGER, 0);

        GlobalIndex gIndex = new GlobalIndex("test", sch, tx);

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

        constant = new IntConstant(12);
        RID rid1 = new RID(0, 0);
        gIndex.insert(constant, rid1);

        constant = new IntConstant(14);
        RID rid2 = new RID(1, 1);
        gIndex.insert(constant, rid2);

        constant = new IntConstant(16);
        RID rid3 = new RID(2, 2 );
        gIndex.insert(constant, rid3);

    }
}
