package simpledb.testing;

import org.junit.jupiter.api.Test;
import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

//=================================CS4432-Project1================
public class LRUTests {

    @Test
    public void lruPolicyTests() throws RemoteException {

        Connection conn = null;
        try {

            //using the drivers for simpleDB create a connection
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();

            //create a cars table with car id, car model, car make, and car year
            String s = "create table CARS(CId int, CModel varchar(10), CMake varchar(10), CYear int)";

            stmt.executeUpdate(s);

            SimpleDB.initFileLogAndBufferMgr("lruDB", "LRU");

            //get the buffer manager
            BufferMgr bm = SimpleDB.bufferMgr();
            Block block1 = new Block("cars.tbl", 0);
            Block block2 = new Block("cars.tbl", 1);
            Block block3 = new Block("cars.tbl", 2);
            Block block4 = new Block("cars.tbl", 3);
            Block block5 = new Block("cars.tbl", 4);
            Block block6 = new Block("cars.tbl", 5);
            Block block7 = new Block("cars.tbl", 6);
            Block block8 = new Block("cars.tbl", 7);
            Block block9 = new Block("cars.tbl", 8);
            Block block10 = new Block("cars.tbl", 9);
            Block block11 = new Block("cars.tbl", 10);

            //pin the blocks to fill the buffer
            Buffer buff1 = bm.pin(block1);
            Buffer buff2 = bm.pin(block2);
            Buffer buff3 = bm.pin(block3);
            Buffer buff4 = bm.pin(block4);
            Buffer buff5 = bm.pin(block5);
            Buffer buff6 = bm.pin(block6);
            Buffer buff7 = bm.pin(block7);
            Buffer buff8 = bm.pin(block8);

            //unpin a few of them to test the policy
            bm.unpin(buff2);
            bm.unpin(buff5);
            bm.unpin(buff7);

            System.out.println(bm.toString());

            //ensure that the bufferpool has three available buffers
            assertEquals(3, bm.available());

            //try to add another buff
            Buffer buff9 = bm.pin(block9);
            System.out.println(bm.toString());

            //check to see if the buffer is correct...
            Buffer[] expectedBufferPool = new Buffer[8];
            expectedBufferPool[0] = buff1;
            expectedBufferPool[1] = buff9;
            expectedBufferPool[2] = buff3;
            expectedBufferPool[3] = buff4;
            expectedBufferPool[4] = buff5;
            expectedBufferPool[5] = buff6;
            expectedBufferPool[6] = buff7;
            expectedBufferPool[7] = buff8;

            assertEquals(Arrays.toString(expectedBufferPool), bm.toString());

            //if we pin and unpin buffer 5 it should no longer be the least recently used, so we
            //should move onto the least recently used buffer which would be buffer 7
            bm.pin(block5);
            System.out.println(bm.toString());
            bm.unpin(buff5);
            System.out.println(bm.toString());

            Buffer buff10 = bm.pin(block10);
            System.out.println(bm.toString());

            expectedBufferPool[6] = buff10;

            assertEquals(Arrays.toString(expectedBufferPool), bm.toString());

            //if we unpin another frame however, 5 will be the least recently used, so we should use that one
            bm.unpin(buff1);
            System.out.println(bm.toString());

            Buffer buff11 = bm.pin(block11);
            System.out.println(bm.toString());

            expectedBufferPool[4] = buff11;

            assertEquals(Arrays.toString(expectedBufferPool), bm.toString());

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            //close the connection to the database
            try {
                if (conn != null){
                    conn.close();
                }
            } catch (SQLException e){
                e.printStackTrace();
            }
        }
    }
}
