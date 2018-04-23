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
import java.util.LinkedList;
import java.util.List;

import static sun.misc.Version.println;

public class GlobalIndex implements Index {
    private String idxname;
    private Schema generalSch;
    private Schema globalSchema;
    private Transaction tx;
    private Constant searchKey = null;
    private TableScan ts = null;
    private int globalDepth;

    //this is probably pointless, could use a single schema on the local index -- just point to the correct bucket!
    private List<LocalIndex> localIndices;

    /**
     * Constructor
     */
    public GlobalIndex(String idxname, Schema sch, Transaction tx){
        this.idxname = idxname;
        this.generalSch = sch; //schema is for local index...
        this.globalSchema = new Schema();
        this.globalSchema.addIntField("LocalDepth");
        this.globalSchema.addIntField("ArrayIndex");
        this.globalSchema.addStringField("GlobalIndex", 25); //for the purposes of this class - I highly doubt we would need to go above 2^25 possible combinations
        this.globalSchema.addStringField("LocalIndex", 25); //for the purposes of this class - I highly doubt we would need to go above 2^25 possible combinations
        this.tx = tx;
        this.localIndices = new LinkedList<>();

        //the first entry in the global index should be zero in binary
        String zeroInBinary = Integer.toBinaryString(0);
        //the second entry in the global index should be one in binary
        String oneInBinary = Integer.toBinaryString(1);

        LocalIndex localIndex = new LocalIndex(idxname, sch, tx);
        LocalIndex localIndex2 = new LocalIndex(idxname, sch, tx);

        //add two new local indices to the list
        localIndices.add(localIndex);
        localIndices.add(localIndex2);

        globalDepth = 1; //the initial depth should only be 1

        TableInfo ti = new TableInfo(zeroInBinary, globalSchema);
        ts = new TableScan(ti, tx);
        ts.insert();
        ts.setString("GlobalIndex", zeroInBinary);
        ts.setString("LocalIndex", zeroInBinary);
        ts.setInt("LocalDepth", 1); //initial depth should be 1
        ts.setInt("ArrayIndex", 0);

        ts.close();

        ti = new TableInfo(oneInBinary, globalSchema);
        ts = new TableScan(ti, tx);
        ts.insert();
        ts.setString("GlobalIndex", oneInBinary);
        ts.setString("LocalIndex", oneInBinary);
        ts.setInt("LocalDepth", 1);
        ts.setInt("ArrayIndex", 1);

        //no need to keep either open after this...
        ts.close();
    }

    @Override
    public void beforeFirst(Constant searchkey) {
        close();
        this.searchKey = searchkey;
        //this will be used for the key
        String binaryFormat = Integer.toBinaryString(searchkey.hashCode());


        //System.out.println(binaryFormat);
        //use the global depth to determine what we should table-scan for...
        //check to make sure the string is actually of a length where we can substring
        String whatToScanFor = binaryFormat;

        //we should only check so many digits of the binary formatted string
        //if it exceeds the global depth of the hash index
        if (binaryFormat.length() >= globalDepth) {
            whatToScanFor = binaryFormat.substring(binaryFormat.length() - globalDepth);
        }

        char[] digits = whatToScanFor.toCharArray();

        if (digits[0] == '0' && digits.length != 1){
            for (int i = 0; i < digits.length - 1; i++){
                if(digits[i] == '1'){
                    break;
                } else {
                    whatToScanFor = whatToScanFor.substring(1);
                }
            }
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

        //first get us where we should insert into the global index
        beforeFirst(dataval);

        //what index is our LocalIndex at?
        int index = -1;
        int localSize = -1;
        String binaryGlobalIndex = "";

        //realistically, this should only execute once
        while(ts.next()){
            System.out.println("ArrayIndex: " + ts.getInt("ArrayIndex"));
            index = ts.getInt("ArrayIndex");
            System.out.println("GlobalIndex: " + ts.getString("GlobalIndex"));
            binaryGlobalIndex = ts.getString("GlobalIndex");
            System.out.println("LocalDepth: " + ts.getInt("LocalDepth"));
            localSize = ts.getInt("LocalDepth");
        }

        //THIS SHOULD NEVER EXECUTE
        //IF WE DO, just break out
        if (index == -1){
            return;
        }

        //System.out.println("This is the index: " + index);
        System.out.println("localindices size: " + localIndices.get(index).isFull());

        if (localIndices.get(index).isFull()) {
            //we need to split the local index up to accommodate the excess records

            //we may need to increment the global index schema
            //check to see if this is the case...
            //if(localSize > globalDepth){ //because they are equivalent we need to increment global depth -- since we will need to increment local size -- that's the problem
                globalDepth++;

                //how many #s we will need is directly correspondent to 2 raised to the new global depth
                int numGlobalIndices = (int) Math.pow(2, globalDepth);

                int numPreviousGlobalIndices = (int) Math.pow(2, globalDepth - 1);

                //we need to now insert these files into the global index

                //this is not completely correct... //actually pretty incorrect...
                for(int i = numPreviousGlobalIndices; i < numGlobalIndices; i++) {
                    close();

                    String inBinaryFormat = Integer.toBinaryString(i);

                    //open up a new table scan and figure out if we should point the new global index value to a new bucket or an empty/partially empty created bucket
                    String previousBinary = inBinaryFormat.substring(1);
                    if(previousBinary.equals(binaryGlobalIndex)) {
                        //generate a new local index
                        LocalIndex localIndex = new LocalIndex(idxname, generalSch, tx);
                        localIndices.add(localIndex);

                        //insert the new index in
                        TableInfo tableInfo = new TableInfo(inBinaryFormat, globalSchema);
                        TableScan tableScan2 = new TableScan(tableInfo, tx);
                        tableScan2.insert();

                        tableScan2.setString("GlobalIndex", inBinaryFormat);
                        tableScan2.setString("LocalIndex", inBinaryFormat);
                        tableScan2.setInt("LocalDepth", globalDepth);
                        tableScan2.setInt("ArrayIndex", localIndices.size() - 1);

                        System.out.println("GlobalIndex:");
                        System.out.println(tableScan2.getString("GlobalIndex"));
                        System.out.println("LocalIndex;");
                        System.out.println(tableScan2.getString("LocalIndex"));
                    } else {
                        close();

                        //look up the right index
                        TableInfo tInfo = new TableInfo(previousBinary, globalSchema);
                        TableScan tScan = new TableScan(tInfo, tx);
                        int previousIndex = -1;
                        while (tScan.next()) {
                            previousIndex = tScan.getInt("ArrayIndex");
                        }

                        TableInfo tableInfo = new TableInfo (inBinaryFormat, globalSchema);
                        TableScan tableScan2 = new TableScan (tableInfo, tx);
                        tableScan2.insert();

                        tableScan2.setString("GlobalIndex", inBinaryFormat);
                        tableScan2.setString("LocalIndex", previousBinary);
                        tableScan2.setInt("LocalDepth", localSize);
                        tableScan2.setInt("ArrayIndex", previousIndex);

                        System.out.println("GlobalIndex (else case):");
                        System.out.println(tableScan2.getString("GlobalIndex"));
                        System.out.println("LocalIndex (else case):");
                        System.out.println(tableScan2.getString("LocalIndex"));

                    }
                }
           // }

            //copy the values from the local index, delete them, then reinsert
            List<Constant> searchKeys = new LinkedList<>(localIndices.get(index).getSearchKeys());
            System.out.println("search keys: " + searchKeys);
            List<RID> ridValues = new LinkedList<>(localIndices.get(index).getRidValues());

            System.out.println("RID block values: ");
            ridValues.forEach(rid->System.out.println(rid.blockNumber()));

            localIndices.get(index).setSize(0);
            //delete the values from the local index
            for (int i = 0; i <= searchKeys.size() - 1; i++){

                //delete from the local index
                localIndices.get(index).mergeDelete(searchKeys.get(i), ridValues.get(i));

                //temporary values
                Constant tempCons = searchKeys.get(i);
                RID tempRID = ridValues.get(i);

                //remove the item from the index and decrement the size
                localIndices.get(index).clearLists();

                System.out.println("TempCons: " + tempCons);

                //reinsert into the global table
                insert(tempCons, tempRID);
            }

            while (ts.next()) {
                System.out.println("aparently we can");
                if(localIndices.get(index).isFull()) {
                    ts.setInt("LocalDepth", globalDepth);
                }
            }

            //now insert the new value if we can
            insert(dataval, datarid);

        } else {
            //no need to split, we can simply insert into this local index
            localIndices.get(index).insert(dataval, datarid);
            localIndices.get(index).incrementSize();
        }
    }

    public void insertHelper(){

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
        List<String> records = new ArrayList<>();
        for (int i = 0; i < (int) Math.pow(2, globalDepth); i++){
            close();
            String binary = Integer.toBinaryString(i);

            TableInfo ti = new TableInfo(binary, globalSchema);
            ts = new TableScan(ti, tx);

            while(ts.next()){
                List<Constant> dataVals = localIndices.get(ts.getInt("ArrayIndex")).getSearchKeys();
                records.add("{GlobalIndex: " + ts.getString("GlobalIndex")
                        + ", LocalIndex: " + ts.getString("LocalIndex")
                        + ", LocalDepth: " + ts.getInt("LocalDepth")
                        + ", ArrayIndex: " + ts.getInt("ArrayIndex") + "}"
                        + ", ArrayValues: " + localIndices.get(ts.getInt("ArrayIndex")).toString(dataVals) + "}");
            }
        }

        return Arrays.toString(records.toArray());
    }
}
