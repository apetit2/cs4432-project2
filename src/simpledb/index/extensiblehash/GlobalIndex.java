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
        String binaryLocalIndex = "";

        //realistically, this should only execute once
        while(ts.next()){
            System.out.println("ArrayIndex: " + ts.getInt("ArrayIndex"));
            index = ts.getInt("ArrayIndex");
            System.out.println("GlobalIndex: " + ts.getString("GlobalIndex"));
            binaryGlobalIndex = ts.getString("GlobalIndex");
            System.out.println("LocalDepth: " + ts.getInt("LocalDepth"));
            localSize = ts.getInt("LocalDepth");
            System.out.println("LocalIndex: " + ts.getString("LocalIndex"));
            binaryLocalIndex = ts.getString("LocalIndex");

            System.out.println(ts.getInt("LocalDepth"));
        }

        //THIS SHOULD NEVER EXECUTE
        //IF WE DO, just break out
        if (index == -1){
            return;
        }

        //System.out.println("This is the index: " + index);
        System.out.println("localindices size: " + localIndices.get(index).isFull());

        if (localIndices.get(index).isFull()) {

            System.out.println("Global depth: " + globalDepth);
            // we need to determine if we can simply remove and re-add elements
            if (localSize < globalDepth){
                System.out.println("localSize: " + localSize);

                for (int i = 0; i < (int) Math.pow(2, globalDepth); i++){
                    System.out.println("Not corrupt yet");
                    System.out.println(toString());

                    close();
                    String inBinaryFormat = Integer.toBinaryString(i);
                    TableInfo tableInfo = new TableInfo(inBinaryFormat, globalSchema);
                    TableScan tableScan = new TableScan(tableInfo, tx);
                    while (tableScan.next()) {
                        if((tableScan.getString("LocalIndex").equals(binaryLocalIndex)) && !(tableScan.getString("GlobalIndex").equals(binaryLocalIndex))){
                            LocalIndex localIndex = new LocalIndex(idxname, generalSch, tx);
                            localIndices.add(localIndex);

                            tableScan.setInt("ArrayIndex", localIndices.size() - 1);
                            tableScan.setInt("LocalDepth", localSize + 1);

                            String newLocal = binaryGlobalIndex.substring(binaryGlobalIndex.length() - (localSize + 1), binaryGlobalIndex.length());
                            System.out.println(newLocal);
                            tableScan.setString("LocalIndex", newLocal);
                        }
                    }
                }

                System.out.println("Not corrupt yet");
                System.out.println(toString());

                TableInfo tableInfo = new TableInfo(binaryLocalIndex, globalSchema);
                TableScan tableScan = new TableScan(tableInfo, tx);
                while (tableScan.next()) {
                    tableScan.setInt("LocalDepth", localSize + 1);
                }

                System.out.println("Not corrupt yet");
                System.out.println(toString());

                //copy the values from the local index, delete them, then reinsert
                List<Constant> searchKeys = new LinkedList<>(localIndices.get(index).getSearchKeys());
                System.out.println("search keys: " + searchKeys);
                List<RID> ridValues = new LinkedList<>(localIndices.get(index).getRidValues());

                System.out.println("RID block values: ");
                ridValues.forEach(rid->System.out.println(rid.blockNumber()));

                localIndices.get(index).setSize(0);

                System.out.println("Not corrupt yet -- AFTER RID VALUES");
                System.out.println(toString());
                //delete the values from the local index
                for (int i = 0; i <= searchKeys.size() - 1; i++){
                    //delete from the local index
                    System.out.println("Not corrupt yet -- BEFORE MERGE DELETE");
                    System.out.println(toString());
                    localIndices.get(index).mergeDelete(searchKeys.get(i), ridValues.get(i));

                    System.out.println("Not corrupt yet -- AFTER MERGE DELETE");
                    System.out.println(toString());

                    //temporary values
                    Constant tempCons = searchKeys.get(i);
                    RID tempRID = ridValues.get(i);

                    //remove the item from the index and decrement the size
                    localIndices.get(index).clearLists();

                    System.out.println("TempCons: " + tempCons);

                    //reinsert into the global table
                    insert(tempCons, tempRID);

                    System.out.println("got here");
                }

                TableInfo tinfo = new TableInfo("0", globalSchema);
                TableScan tscan = new TableScan(tinfo, tx);

                while (tscan.next()) {
                    System.out.println(tscan.getInt("LocalDepth"));
                    int i = tscan.getInt("ArrayIndex");
                    System.out.println(Arrays.toString(localIndices.get(i).getSearchKeys().toArray()));
                }

                //System.out.println(toString());

                //now insert the new value if we can
                insert(dataval, datarid);

                return;

            } else {
                beforeFirst(dataval);
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
            }


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

            //now insert the new value if we can
            insert(dataval, datarid);

        } else {
            System.out.println("NOT CORRUPT --- JUST BEFORE INSERT OF 11: ");
            System.out.println(toString());
            //no need to split, we can simply insert into this local index
            System.out.println(index);
            System.out.println(localIndices.get(index).isFull());

            System.out.println("SEARCH KEYS: ");
            System.out.println(localIndices.get(index).getSearchKeys());

            System.out.println("DATAVAL: " + dataval);
            System.out.println("DATARID: " + datarid);


            System.out.println(toString());

            localIndices.get(index).insert(dataval, datarid);

            System.out.println("NOT CORRUPT --- JUST AFTER INSERT OF 11: ");
            System.out.println(toString());

            System.out.println(localIndices.get(index).getSearchKeys());
            localIndices.get(index).incrementSize();
        }
    }

    @Override
    public void delete(Constant dataval, RID datarid) {

        //how we delete from the global index

    }

    @Override
    public void close() {
        if (ts != null){
            System.out.println("trying to close");
            ts.close();
        }
    }

    @Override
    public String toString() {
        List<String> records = new ArrayList<>();
        for (int i = 1; i < (int) Math.pow(2, globalDepth); i++){
            String binary = Integer.toBinaryString(i);

            System.out.println(localIndices.get(0).getSearchKeys());
            System.out.println(localIndices.get(0).getRidValues());

            TableInfo ti = new TableInfo(binary, globalSchema);
            TableScan topScan = new TableScan(ti, tx);

            while(topScan.next()){
                List<Constant> dataVals = localIndices.get(topScan.getInt("ArrayIndex")).getSearchKeys();
                records.add("{GlobalIndex: " + topScan.getString("GlobalIndex")
                        + ", LocalIndex: " + topScan.getString("LocalIndex")
                        + ", LocalDepth: " + topScan.getInt("LocalDepth")
                        + ", ArrayIndex: " + topScan.getInt("ArrayIndex") + "}"
                        + ", ArrayValues: " + localIndices.get(topScan.getInt("ArrayIndex")).toString(dataVals) + "}");
            }

            topScan.close();
        }

        return Arrays.toString(records.toArray());
    }
}
