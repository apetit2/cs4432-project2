package simpledb.index.extensiblehash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class LocalIndex implements Index {
    private String idxName;
    private Schema sch;
    private Transaction tx;
    private Constant searchKey = null;
    private TableScan ts = null;
    private int locakDepth;
    final static int MAX_SIZE = 2;
    private int size = 0;

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
        TableInfo ti = new TableInfo(tblname, sch);
        ts = new TableScan(ti, tx);
    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public RID getDataRid() {
        return null;
    }

    @Override
    public void insert(Constant val, RID rid) {
        beforeFirst(val);
        ts.insert();
        ts.setInt("block", rid.blockNumber());
        ts.setInt("id", rid.id());
        ts.setVal("dataval", val);
    }

    @Override
    public void delete(Constant dataval, RID datarid) {

    }

    @Override
    public void close() {

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

}
