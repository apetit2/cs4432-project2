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

import static junit.framework.TestCase.assertEquals;

//=================================CS4432-Project1================
public class ClockPolicyTests {

    @Test
    public void clockPolicyTest() throws RemoteException {

        Connection conn = null;
        try {

            //using the drivers for simpleDB create a connection
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();

            //create a cars table with car id, car model, car make, and car year
            String s = "create table CARS(CId int, CModel varchar(10), CMake varchar(10), CYear int)";

            stmt.executeUpdate(s);

            SimpleDB.initFileLogAndBufferMgr("ClockTestDB", "CLOCK");

            //get the buffer manager
            BufferMgr bm = SimpleDB.bufferMgr();

            //initialize some blocks to add to the buffer-pool to test the clock policy
            Block block1 = new Block("cars.tbl", 0);
            Block block2 = new Block("cars.tbl", 1);
            Block block3 = new Block("cars.tbl", 2);
            Block block4 = new Block("cars.tbl", 3);
            Block block5 = new Block("cars.tbl", 4);
            Block block6 = new Block("cars.tb1", 5);
            Block block7 = new Block("cars.tbl", 6);
            Block block8 = new Block("cars.tbl", 7);
            Block block9 = new Block("cars.tbl", 8);
            Block block10 = new Block("cars.tbl", 9);
            Block block11 = new Block("cars.tbl", 10);
            Block block12 = new Block("cars.tbl", 11);
            Block block13 = new Block("cars.tbl", 12);

            //Fill the bufferpool
            Buffer buff1 = bm.pin(block1);
            Buffer buff2 = bm.pin(block2);
            Buffer buff3 = bm.pin(block3);
            Buffer buff4 = bm.pin(block4);
            Buffer buff5 = bm.pin(block5);
            Buffer buff6 = bm.pin(block6);
            Buffer buff7 = bm.pin(block7);
            Buffer buff8 = bm.pin(block8);

            //unpin some of the buffers
            bm.unpin(buff2);
            bm.unpin(buff5);
            bm.unpin(buff7);

            System.out.println(bm.toString());

            //should have a three buffers available
            assertEquals(3, bm.available());

            //pin another bin to overfill the bufferpool
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

            //continue to check if clock policy was correct
            Buffer buff10 = bm.pin(block10);
            System.out.println(bm.toString());

            expectedBufferPool[4] = buff10;

            assertEquals(Arrays.toString(expectedBufferPool), bm.toString());

            //continue to check correctness
            Buffer buff11 = bm.pin(block11);
            System.out.println(bm.toString());

            expectedBufferPool[6] = buff11;

            assertEquals(Arrays.toString(expectedBufferPool), bm.toString());

            //make sure it rotates back to the start of the buffer at the end
            bm.unpin(buff1);
            System.out.println(bm.toString());

            Buffer buff12 = bm.pin(block12);
            System.out.println(bm.toString());

            expectedBufferPool[0] = buff12;
            assertEquals(Arrays.toString(expectedBufferPool), bm.toString());
        } catch (SQLException e){
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
