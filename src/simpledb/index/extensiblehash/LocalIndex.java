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

public class LocalIndex implements Index {
    private String idxName;
    private Schema sch;
    private Transaction tx;
    private Constant searchKey = null;
    private TableScan ts = null;
    final static int MAX_SIZE = 2; //Max size of the buffer plus 1
    private int size = 0;
    private List<Constant> searchKeys = new LinkedList<>();
    private List<RID> ridValues = new LinkedList<>();

    public LocalIndex(String idxName, Schema sch, Transaction tx){
        this.idxName = idxName;
        this.sch = sch;
        this.tx = tx;
    }

    @Override
    public void beforeFirst(Constant searchkey) {
        close();
        this.searchKey = searchkey;
        String binaryFormat = Integer.toBinaryString(searchkey.hashCode());
        String tblname = idxName + binaryFormat;

        searchKeys.add(searchkey);

        TableInfo ti = new TableInfo(tblname, sch);
        ts = new TableScan(ti, tx);
    }

    @Override
    public boolean next() {
        while (ts.next())
            if (ts.getVal("dataval").equals(searchKey))
                return true;
        return false;
    }

    @Override
    public RID getDataRid() {
        int blknum = ts.getInt("block");
        int id = ts.getInt("id");
        return new RID(blknum, id);
    }

    @Override
    public void insert(Constant constant, RID rid) {
        beforeFirst(constant);

        ridValues.add(rid);

        ts.insert();
        ts.setInt("block", rid.blockNumber());
        ts.setInt("id", rid.id());
        ts.setVal("dataval", constant);
    }

    @Override
    public void delete(Constant dataval, RID datarid) {
        beforeFirst(dataval);
        while(next())
            if (getDataRid().equals(datarid)) {
                ts.delete();
                return;
            }
    }

    @Override
    public void close() {
        if (ts != null){
            ts.close();
        }
    }

    /**
     *
     * @return
     */
    public boolean isFull() {
        if (size == MAX_SIZE){
            return true;
        }
        return false;
    }

    public void incrementSize(){
        this.size++;
    }

    public void setTs(String tblName){
        TableInfo ti = new TableInfo(tblName, sch);
        this.ts = new TableScan(ti, tx);

    }

    public String toString(List<Constant> dataVals) {
        //just print out the current table scan
        //that helps enough
        //not really sure how to print out the entire schema :/

        List<String> records = new LinkedList<>();

        for(Constant searchKey : dataVals){
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

    public List<Constant> getSearchKeys(){
        return this.searchKeys;
    }

    public List<RID> getRidValues(){
        return this.ridValues;
    }

    public void clearLists() {
        this.searchKeys.remove(0);
        this.ridValues.remove(0);
    }

    public void setSize(int size) {
        this.size = size;
    }

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

        System.out.println(toString());
    }

}
