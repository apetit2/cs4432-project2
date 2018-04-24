package simpledb.index.extensiblehash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class LocalIndex implements Index {
    //the idxname
    private String idxName;
    //the schema
    private Schema sch;
    //the transaction
    private Transaction tx;
    //the current search key
    private Constant searchKey = null;
    //the current table scan
    private TableScan ts = null;
    final static int MAX_SIZE = 3; //Max size of the buffer
    //the current size of the buffer
    private int size = 0;
    //a list of all the search keys on this local index
    private List<Constant> searchKeys = new LinkedList<>();
    //a list of all RID values on this local index
    private List<RID> ridValues = new LinkedList<>();

    /**
     * Constructor
     * @param idxName
     * @param sch
     * @param tx
     */
    public LocalIndex(String idxName, Schema sch, Transaction tx){
        this.idxName = idxName;
        this.sch = sch;
        this.tx = tx;
    }

    /**
     * Prepares us a table scan that we will use to make transactions with the schema -- basically the same as that found on hash index
     * @param searchkey the search key value.
     */
    @Override
    public void beforeFirst(Constant searchkey) {
        close();

        this.searchKey = searchkey;
        String binaryFormat = Integer.toBinaryString(searchkey.hashCode());

        String tblname = idxName + binaryFormat;

        //add the search key to the list of search keys
        searchKeys.add(searchkey);

        TableInfo ti = new TableInfo(tblname, sch);
        ts = new TableScan(ti, tx);
    }

    /**
     * Tells us whether or not there is another value in the local index
     * @return
     */
    @Override
    public boolean next() {
        while (ts.next())
            if (ts.getVal("dataval").equals(searchKey))
                return true;
        return false;
    }

    /**
     * Same as on the hash index -- gets us a data rid
     * @return
     */
    @Override
    public RID getDataRid() {
        int blknum = ts.getInt("block");
        int id = ts.getInt("id");
        return new RID(blknum, id);
    }

    /**
     * We are simply inserting a new record into the local index
     * @param constant
     * @param rid
     */
    @Override
    public void insert(Constant constant, RID rid) {
        beforeFirst(constant);

        //add the rid value to the list that exists on this local index
        ridValues.add(rid);

        ts.insert();

        ts.setInt("block", rid.blockNumber());
        ts.setInt("id", rid.id());
        ts.setVal("dataval", constant);
    }

    /**
     * This is essentially merge delete, but we want to remove dataval and datarid unlike mergedelete
     * @param dataval the dataval of the deleted index record
     * @param datarid the dataRID of the deleted index record
     */
    @Override
    public void delete(Constant dataval, RID datarid) {
        beforeFirst(dataval);

        while(next())
            if (getDataRid().equals(datarid)) {
                searchKeys.remove(dataval);
                ridValues.remove(datarid);
                ts.delete();
                return;
            }
    }

    /**
     * close the table scan of this local index
     */
    @Override
    public void close() {
        if (ts != null){
            ts.close();
        }
    }

    /**
     * Returns a boolean stating whether or not this local index is filled
     * @return {boolean} true or false value indicating whether or not this local index is full
     */
    public boolean isFull() {
        if (size == MAX_SIZE){
            return true;
        }
        return false;
    }

    /**
     * Allows us to increment size in an encapsulated manner
     */
    public void incrementSize(){
        this.size++;
    }

    /**
     * Allows us to set the tablescan for this local index
     * @param tblName
     */
    public void setTs(String tblName){
        TableInfo ti = new TableInfo(tblName, sch);
        this.ts = new TableScan(ti, tx);

    }

    /**
     * Basically a toString method, we print out all the blocks and ids in the schema
     * @return
     */
    @Override
    public String toString() {
        //just print out the current table scan
        //that helps enough
        //not really sure how to print out the entire schema :/

        List<String> records = new LinkedList<>();

        for(Constant searchKey : this.searchKeys){
            close();
            this.searchKey = searchKey;
            String binaryFormat = Integer.toBinaryString(searchKey.hashCode());
            String tblname = idxName + binaryFormat;

            TableInfo ti = new TableInfo(tblname, sch);
            ts = new TableScan(ti, tx);

            while(ts.next()) {
                records.add("{Block: " + ts.getInt("block")
                        + ", ID: " + ts.getInt("id") + "}");
            }
        }

        return Arrays.toString(records.toArray());
    }

    /**
     * Gets a list of search keys stored on this local index
     * @return {List<Constant>} a list of all search keys on this index
     */
    public List<Constant> getSearchKeys(){
        return this.searchKeys;
    }

    /**
     * Just gets us a list of the RID values stored on this local index
     * @return {List<RID>} a list of all the RID values stored on this local index
     */
    public List<RID> getRidValues(){
        return this.ridValues;
    }

    /**
     * All this does is clear the linked lists that we have created
     */
    public void clearLists() {
        this.searchKeys.remove(0);
        this.ridValues.remove(0);
    }

    /**
     * This is necessary to set how many records are actually in this index
     * @param size
     */
    public void setSize(int size) {
        this.size = size;
    }

    /**
     * Basically delete, but we use this in the insert method, when we want to move around
     * records
     * @param searchKey
     * @param rid
     */
    public void mergeDelete(Constant searchKey, RID rid){
        close();
        this.searchKey = searchKey;

        String binaryFormat = Integer.toBinaryString(searchKey.hashCode());
        String tblname = idxName + binaryFormat;

        TableInfo ti = new TableInfo(tblname, sch);
        ts = new TableScan(ti, tx);

        while(next())
            if (getDataRid().equals(rid)) {
                ts.delete();

                return;
            }
    }
}
