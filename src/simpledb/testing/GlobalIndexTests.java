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
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GlobalIndexTests {
    //================================================NOTE==============================================
    //THIS IS INTENDED FOR USE WITH A LOCAL INDEX VALUE OF 3 ONLY!!!!! IF YOU MODIFY THE LOCAL INDEX VALUE
    //THIS UNIT TEST WILL NO LONGER BE APPLICABLE -- IT IS USED ONLY TO PROVE FUNCTIONALITY
    @Test
    public void globalIndexTests() {
        //analogous to the driver -- so we can run offline
        SimpleDB.init("GlobalIndexDB", "LRU");

        //analogous to the connection
        Transaction tx = new Transaction();

        tx.commit();

        //create a new schema for the local index of an extensible hash
        Schema sch = new Schema();
        sch.addIntField("block");
        sch.addIntField("id");
        sch.addField("dataval", INTEGER, 0);

        //generate a new global index
        GlobalIndex gIndex = new GlobalIndex("test", sch, tx);

        //should return a global index that has two values that point to two empty buckets in the local index
        String expectedString = "[{GlobalIndex: 0, LocalIndex: 0, LocalDepth: 1, ArrayIndex: 0, BucketValues: []}, {GlobalIndex: 1, LocalIndex: 1, LocalDepth: 1, ArrayIndex: 1, BucketValues: []}]";
        assertEquals(expectedString, gIndex.toString());

        //print-out because that is a requirement for the documentation
        System.out.println(gIndex.toString());

        //insert the constant 12 into the global index and check to see it correctly added to the global index spot at 0 since we have global depth of 1
        Constant constant = new IntConstant(12);
        RID rid1 = new RID(0, 0);
        gIndex.insert(constant, rid1);

        expectedString = "[{GlobalIndex: 0, LocalIndex: 0, LocalDepth: 1, ArrayIndex: 0, BucketValues: [{Block: 0, ID: 0}]}, {GlobalIndex: 1, LocalIndex: 1, LocalDepth: 1, ArrayIndex: 1, BucketValues: []}]";
        assertEquals(expectedString, gIndex.toString());

        //print-out because that is a requirement for the documentation
        System.out.println(gIndex.toString());

        //insert the constant 14 into the global index and check to see it correctly added to the global index spot at 0 since we have global depth of 1
        constant = new IntConstant(14);
        RID rid2 = new RID(1, 1);
        gIndex.insert(constant, rid2);

        expectedString = "[{GlobalIndex: 0, LocalIndex: 0, LocalDepth: 1, ArrayIndex: 0, BucketValues: [{Block: 0, ID: 0}, {Block: 1, ID: 1}]}, {GlobalIndex: 1, LocalIndex: 1, LocalDepth: 1, ArrayIndex: 1, BucketValues: []}]";
        assertEquals(expectedString, gIndex.toString());

        //print-out because that is a requirement for the documentation
        System.out.println(gIndex.toString());

        //insert the constant 16 into the global index and ccheck to see it correctly added to the global index spot at 0 since we have global depth of 1
        //this should fill the bucket that the global index value of 0 points to
        constant = new IntConstant(16);
        RID rid3 = new RID(2, 2 );
        gIndex.insert(constant, rid3);

        expectedString = "[{GlobalIndex: 0, LocalIndex: 0, LocalDepth: 1, ArrayIndex: 0, BucketValues: [{Block: 0, ID: 0}, {Block: 1, ID: 1}, {Block: 2, ID: 2}]}, {GlobalIndex: 1, LocalIndex: 1, LocalDepth: 1, ArrayIndex: 1, BucketValues: []}]";
        assertEquals(expectedString, gIndex.toString());

        //print-out because that is a requirement for the documentation
        System.out.println(gIndex.toString());

        //insert the constant 20 into the global index and check to see if it correctly added to the global index spot of 0. This overfills the bucket that
        //the global index value of 0 points to so we should have seen an increase in the number of global index values and chances in the structure based upon the spliting
        //strategy discussed in class
        constant = new IntConstant(20);
        RID rid4 = new RID(3, 3);
        gIndex.insert(constant, rid4);

        expectedString = "[{GlobalIndex: 0, LocalIndex: 0, LocalDepth: 2, ArrayIndex: 0, BucketValues: [{Block: 0, ID: 0}, {Block: 2, ID: 2}, {Block: 3, ID: 3}]}, {GlobalIndex: 1, LocalIndex: 1, LocalDepth: 1, ArrayIndex: 1, BucketValues: []}, {GlobalIndex: 10, LocalIndex: 10, LocalDepth: 2, ArrayIndex: 2, BucketValues: [{Block: 1, ID: 1}]}, {GlobalIndex: 11, LocalIndex: 1, LocalDepth: 1, ArrayIndex: 1, BucketValues: []}]";
        assertEquals(expectedString, gIndex.toString());

        //print-out because that is a requirement for the documentation
        System.out.println(gIndex.toString());

        //we should have only one bucket for the 1 values that are pointed to by several global index values, check to make sure this is true
        constant = new IntConstant(11);
        RID rid0 = new RID(3, 3);
        gIndex.insert(constant, rid0);

        expectedString = "[{GlobalIndex: 0, LocalIndex: 0, LocalDepth: 2, ArrayIndex: 0, BucketValues: [{Block: 0, ID: 0}, {Block: 2, ID: 2}, {Block: 3, ID: 3}]}, {GlobalIndex: 1, LocalIndex: 1, LocalDepth: 1, ArrayIndex: 1, BucketValues: [{Block: 3, ID: 3}]}, {GlobalIndex: 10, LocalIndex: 10, LocalDepth: 2, ArrayIndex: 2, BucketValues: [{Block: 1, ID: 1}]}, {GlobalIndex: 11, LocalIndex: 1, LocalDepth: 1, ArrayIndex: 1, BucketValues: [{Block: 3, ID: 3}]}]";
        assertEquals(expectedString, gIndex.toString());

        //print-out because that is a requirement for the documentation
        System.out.println(gIndex.toString());

        //keep filling this global index bucket and make sure that when it is fully filled we don't extend the global index, we just add another bucket to the different global index values
        constant = new IntConstant(13);
        RID rid5 = new RID(4, 4);
        gIndex.insert(constant, rid5);

        expectedString = "[{GlobalIndex: 0, LocalIndex: 0, LocalDepth: 2, ArrayIndex: 0, BucketValues: [{Block: 0, ID: 0}, {Block: 2, ID: 2}, {Block: 3, ID: 3}]}, {GlobalIndex: 1, LocalIndex: 1, LocalDepth: 1, ArrayIndex: 1, BucketValues: [{Block: 3, ID: 3}, {Block: 4, ID: 4}]}, {GlobalIndex: 10, LocalIndex: 10, LocalDepth: 2, ArrayIndex: 2, BucketValues: [{Block: 1, ID: 1}]}, {GlobalIndex: 11, LocalIndex: 1, LocalDepth: 1, ArrayIndex: 1, BucketValues: [{Block: 3, ID: 3}, {Block: 4, ID: 4}]}]";
        assertEquals(expectedString, gIndex.toString());

        //print-out because that is a requirement for the documentation
        System.out.println(gIndex.toString());

        //this will fill up the global index with local index value of 1
        constant = new IntConstant(15);
        RID rid6 = new RID(5, 5);
        gIndex.insert(constant, rid6);

        expectedString = "[{GlobalIndex: 0, LocalIndex: 0, LocalDepth: 2, ArrayIndex: 0, BucketValues: [{Block: 0, ID: 0}, {Block: 2, ID: 2}, {Block: 3, ID: 3}]}, {GlobalIndex: 1, LocalIndex: 1, LocalDepth: 1, ArrayIndex: 1, BucketValues: [{Block: 3, ID: 3}, {Block: 4, ID: 4}, {Block: 5, ID: 5}]}, {GlobalIndex: 10, LocalIndex: 10, LocalDepth: 2, ArrayIndex: 2, BucketValues: [{Block: 1, ID: 1}]}, {GlobalIndex: 11, LocalIndex: 1, LocalDepth: 1, ArrayIndex: 1, BucketValues: [{Block: 3, ID: 3}, {Block: 4, ID: 4}, {Block: 5, ID: 5}]}]";
        assertEquals(expectedString, gIndex.toString());

        //print-out because that is a requirement for the documentation
        System.out.println(gIndex.toString());

        //ensure that we did not add the global depth again
        constant = new IntConstant(19);
        RID rid7 = new RID(6, 6);
        gIndex.insert(constant, rid7);

        expectedString = "[{GlobalIndex: 0, LocalIndex: 0, LocalDepth: 2, ArrayIndex: 0, BucketValues: [{Block: 0, ID: 0}, {Block: 2, ID: 2}, {Block: 3, ID: 3}]}, {GlobalIndex: 1, LocalIndex: 1, LocalDepth: 2, ArrayIndex: 1, BucketValues: [{Block: 4, ID: 4}]}, {GlobalIndex: 10, LocalIndex: 10, LocalDepth: 2, ArrayIndex: 2, BucketValues: [{Block: 1, ID: 1}]}, {GlobalIndex: 11, LocalIndex: 11, LocalDepth: 2, ArrayIndex: 3, BucketValues: [{Block: 3, ID: 3}, {Block: 5, ID: 5}, {Block: 6, ID: 6}]}]";
        assertEquals(expectedString, gIndex.toString());

        //print-out because that is a requirement for the documentation
        System.out.println(gIndex.toString());

        //finally test the delete function
        gIndex.delete(constant, rid7);

        expectedString = "[{GlobalIndex: 0, LocalIndex: 0, LocalDepth: 2, ArrayIndex: 0, BucketValues: [{Block: 0, ID: 0}, {Block: 2, ID: 2}, {Block: 3, ID: 3}]}, {GlobalIndex: 1, LocalIndex: 1, LocalDepth: 2, ArrayIndex: 1, BucketValues: [{Block: 4, ID: 4}]}, {GlobalIndex: 10, LocalIndex: 10, LocalDepth: 2, ArrayIndex: 2, BucketValues: [{Block: 1, ID: 1}]}, {GlobalIndex: 11, LocalIndex: 11, LocalDepth: 2, ArrayIndex: 3, BucketValues: [{Block: 3, ID: 3}, {Block: 5, ID: 5}]}]";
        assertEquals(expectedString, gIndex.toString());

        //print-out because that is a requirement for the documentation
        System.out.println(gIndex.toString());

        //try deleting a RID that has no other elements in it
        constant = new IntConstant(13);
        gIndex.delete(constant, rid5);

        expectedString = "[{GlobalIndex: 0, LocalIndex: 0, LocalDepth: 2, ArrayIndex: 0, BucketValues: [{Block: 0, ID: 0}, {Block: 2, ID: 2}, {Block: 3, ID: 3}]}, {GlobalIndex: 1, LocalIndex: 1, LocalDepth: 2, ArrayIndex: 1, BucketValues: []}, {GlobalIndex: 10, LocalIndex: 10, LocalDepth: 2, ArrayIndex: 2, BucketValues: [{Block: 1, ID: 1}]}, {GlobalIndex: 11, LocalIndex: 11, LocalDepth: 2, ArrayIndex: 3, BucketValues: [{Block: 3, ID: 3}, {Block: 5, ID: 5}]}]";
        assertEquals(expectedString, gIndex.toString());

        //print-out because that is a requirement for the documentation
        System.out.println(gIndex.toString());

    }
}
