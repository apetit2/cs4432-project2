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


public class GlobalIndex implements Index {
    //the idxname
    private String idxname;
    //the local schema
    private Schema generalSch;
    //the global schema
    private Schema globalSchema;
    //the transaction
    private Transaction tx;
    //the current search key
    private Constant searchKey = null;
    //the current table scan
    private TableScan ts = null;
    //the depth of the global index
    private int globalDepth;
    //a list of all local indices
    private List<LocalIndex> localIndices;

    /**
     * Constructor
     * @param idxname
     * @param sch
     * @param tx
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

        //use the global depth to determine what we should table-scan for...
        //check to make sure the string is actually of a length where we can substring
        String whatToScanFor = binaryFormat;

        //we should only check so many digits of the binary formatted string
        //if it exceeds the global depth of the hash index
        if (binaryFormat.length() >= globalDepth) {
            whatToScanFor = binaryFormat.substring(binaryFormat.length() - globalDepth);
        }

        char[] digits = whatToScanFor.toCharArray();

        //this is because we will have values like 0001 and this should be recognized as 1
        if (digits[0] == '0' && digits.length != 1){
            for (int i = 0; i < digits.length - 1; i++){
                if(digits[i] == '1'){
                    break;
                } else {
                    whatToScanFor = whatToScanFor.substring(1);
                }
            }
        }

        //return the table scan
        TableInfo ti = new TableInfo(whatToScanFor, globalSchema);
        ts = new TableScan(ti, tx);

    }

    @Override
    public boolean next() {
        //Don't use this here...
        return false;
    }

    @Override
    public RID getDataRid() {
        //Don't use this here...
        return null;
    }

    /**
     * How we insert a value into the global index
     * @param dataval the dataval in the new index record.
     * @param datarid the dataRID in the new index record.
     */
    @Override
    public void insert(Constant dataval, RID datarid) {
        //first get us where we should insert into the global index
        beforeFirst(dataval);

        //what index is our LocalIndex at in the array of local indices?
        int index = -1;
        //what is the local depth of this global index value
        int localSize = -1;
        //what is the global index value in binary
        String binaryGlobalIndex = "";
        //what is the local index value in binary
        String binaryLocalIndex = "";

        //realistically, this should only execute once because we should only have one binary value per spot in the global index
        while(ts.next()){
            //setting the index value
            index = ts.getInt("ArrayIndex");
            //setting the global index binary value
            binaryGlobalIndex = ts.getString("GlobalIndex");
            //setting the local depth value
            localSize = ts.getInt("LocalDepth");
            //setting the local index value
            binaryLocalIndex = ts.getString("LocalIndex");
        }

        //THIS SHOULD NEVER EXECUTE UNLESS WE EXCEED THE NUMBER OF BUFFERS!!!!!
        //IF WE DO, just break out
        if (index == -1){
            return;
        }

        //if we have filled the current local index we need to do a couple of things to
        //insert the dataval into the global index
        if (localIndices.get(index).isFull()) {
            // we need to determine if we can simply remove and re-add elements
            if (localSize < globalDepth){
                //we don't need to insert any new values into the global index,
                //but we do need to associate a new local index with global index values that point to the same local index
                for (int i = 0; i < (int) Math.pow(2, globalDepth); i++){
                    //close ts, becuase we need to look up a new value in the schema
                    close();
                    //look up the i value in the schema
                    String inBinaryFormat = Integer.toBinaryString(i);
                    TableInfo tableInfo = new TableInfo(inBinaryFormat, globalSchema);
                    TableScan tableScan = new TableScan(tableInfo, tx);
                    while (tableScan.next()) {
                        //if the local index == the local index from the first scan, and the global index does not equal the local index (this is the one index spot that we
                        //do not need to associate a new local index value with"
                        if((tableScan.getString("LocalIndex").equals(binaryLocalIndex)) && !(tableScan.getString("GlobalIndex").equals(binaryLocalIndex))){
                            LocalIndex localIndex = new LocalIndex(idxname, generalSch, tx);
                            localIndices.add(localIndex);

                            tableScan.setInt("ArrayIndex", localIndices.size() - 1);
                            tableScan.setInt("LocalDepth", localSize + 1);

                            String newLocal = binaryGlobalIndex.substring(binaryGlobalIndex.length() - (localSize + 1), binaryGlobalIndex.length());
                            tableScan.setString("LocalIndex", newLocal);
                        }
                    }
                }

                //up the local-depth of the current local index
                TableInfo tableInfo = new TableInfo(binaryLocalIndex, globalSchema);
                TableScan tableScan = new TableScan(tableInfo, tx);
                while (tableScan.next()) {
                    tableScan.setInt("LocalDepth", localSize + 1);
                }

            } else {
                //this is the case where we do need to increment the global depth

                //get our ts value
                beforeFirst(dataval);

                //increment the local depth
                while (ts.next()) {
                    ts.setInt("LocalDepth", localSize + 1);
                }

                //we need to split the local index up to accommodate the excess records
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
                    }
                }
            }


            //copy the values from the local index, delete them, then reinsert
            List<Constant> searchKeys = new LinkedList<>(localIndices.get(index).getSearchKeys());
            List<RID> ridValues = new LinkedList<>(localIndices.get(index).getRidValues());

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

                //reinsert into the global table
                insert(tempCons, tempRID);
            }

            //now insert the new value if we can
            insert(dataval, datarid);

        } else {
            localIndices.get(index).insert(dataval, datarid);
            localIndices.get(index).incrementSize();
        }
    }

    /**
     * How we delete from the global index
     * @param dataval the dataval of the deleted index record
     * @param datarid the dataRID of the deleted index record
     */
    @Override
    public void delete(Constant dataval, RID datarid) {
        //how we delete from the global index

        //prepare our tablescan
        beforeFirst(dataval);

        int index = -1;
        //go through the values
        while (ts.next()) {
            //we only need to look up the array value because we will delete from this
            index = ts.getInt("ArrayIndex");
        }

        //THE ONLY TIME WE SHOULD REALLY EVER NOT GET HERE IS IF WE RUN OUT OF BUFFERS!!!!
        if(index != -1) {
            localIndices.get(index).delete(dataval, datarid);
        }

    }

    /**
     * Close the connection to the current rendition of the tablescan
     */
    @Override
    public void close() {
        //if ts is currently set to a rendition of a table scan we should close now
        if (ts != null){
            ts.close();
        }
    }

    /**
     * Prints out the schema of the global table
     * @return {String} global schema in string format
     */
    @Override
    public String toString() {
        //temporary list of all records in the schema
        List<String> records = new ArrayList<>();

        //go through all the records in the schema and record the information about them
        for (int i = 0; i < (int) Math.pow(2, globalDepth); i++){

            //the ith record in the schema can be obtained by looking at it's binary value
            String binary = Integer.toBinaryString(i);

            //scan the schema looking for this binary value
            TableInfo ti = new TableInfo(binary, globalSchema);
            TableScan topScan = new TableScan(ti, tx);

            //this should only execute once, because we should only have one value per each global table spot
            while(topScan.next()){
                records.add("{GlobalIndex: " + topScan.getString("GlobalIndex")
                        + ", LocalIndex: " + topScan.getString("LocalIndex")
                        + ", LocalDepth: " + topScan.getInt("LocalDepth")
                        + ", ArrayIndex: " + topScan.getInt("ArrayIndex") + "}"
                        + ", ArrayValues: " + localIndices.get(topScan.getInt("ArrayIndex")).toString() + "}");
            }

            //close the scan each time
            topScan.close();
        }

        //return the array of records as a string
        return Arrays.toString(records.toArray());
    }
}
