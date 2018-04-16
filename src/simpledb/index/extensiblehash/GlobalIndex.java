package simpledb.index.extensiblehash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GlobalIndex implements Index {
    private String idxname;
    private Schema generalSch;
    private Schema globalSchema;
    private Transaction tx;
    private Constant searchKey = null;
    private TableScan ts = null;
    private int globalDepth;

    /**
     * Constructor
     */
    public GlobalIndex(String idxname, Schema sch, Transaction tx){
        this.idxname = idxname;
        this.generalSch = sch; //schema is for local index...
        this.globalSchema = new Schema();
        this.globalSchema.addStringField("GlobalIndex", 25); //for the purposes of this class - I highly doubt we would need to go above 2^25 possible combinations
        this.globalSchema.addStringField("LocalIndex", 25); //for the purposes of this class - I highly doubt we would need to go above 2^25 possible combinations
        this.globalSchema.addIntField("LocalDepth");
        this.tx = tx;

        //the first entry in the global index should be zero in binary
        String zeroInBinary = Integer.toBinaryString(0);
        //the second entry in the global index should be one in binary
        String oneInBinary = Integer.toBinaryString(1);

        globalDepth = 1; //the initial depth should only be 1

        TableInfo ti = new TableInfo(zeroInBinary, globalSchema);
        ts = new TableScan(ti, tx);
        ts.insert();
        ts.setString("GlobalIndex", zeroInBinary);
        ts.setString("LocalIndex", zeroInBinary);
        ts.setInt("LocalDepth", 1); //initial depth should be 1
        ts.close();

        ti = new TableInfo(oneInBinary, globalSchema);
        ts = new TableScan(ti, tx);
        ts.insert();
        ts.setString("GlobalIndex", oneInBinary);
        ts.setString("LocalIndex", oneInBinary);
        ts.setInt("LocalDepth", 1);

        //no need to keep either open after this...
    }

    @Override
    public void beforeFirst(Constant searchkey) {
        close();
        this.searchKey = searchkey;
        //this will be used for the key
        String binaryFormat = Integer.toBinaryString(searchkey.hashCode());

        System.out.println(binaryFormat);
        //use the global depth to determine what we should table-scan for...
        //check to make sure the string is actually of a length where we can substring
        String whatToScanFor = binaryFormat;

        //we should only check so many digits of the binary formatted string
        //if it exceeds the global depth of the hash index
        if (binaryFormat.length() >= globalDepth) {
            whatToScanFor = binaryFormat.substring(binaryFormat.length() - globalDepth);
        }

        TableInfo ti = new TableInfo(whatToScanFor, globalSchema);
        ts = new TableScan(ti, tx);
    }

    @Override
    public boolean next() {
        //this does exist here, but we will instead look up a local index and then run the next method on that local index to
        //determine if the key is there -- if not we go to the next local index that the bucket points to
        //returns false if there are no values in the local index that exist


        return false;
    }

    @Override
    public RID getDataRid() {
        //this should basically look up from the table scan of the current local index
        //the local index should have the same exact method as found in the hash index

        return null;
    }

    @Override
    public void insert(Constant dataval, RID datarid) {

        //how we insert into the global index

    }

    @Override
    public void delete(Constant dataval, RID datarid) {

        //how we delete from the global index

    }

    @Override
    public void close() {
        if (ts != null){
            ts.close();
        }
    }

    @Override
    public String toString() {
        //just print out the current table scan
        //that helps enough

        List<String> records = new ArrayList<>();

        while(ts.next()) {
            records.add("{GlobalIndex: " + ts.getString("GlobalIndex")
                    + ", LocalIndex: " + ts.getString("LocalIndex")
                    + ", LocalDepth: " + ts.getInt("LocalDepth") + "}");
        }

        return Arrays.toString(records.toArray());
    }
}
