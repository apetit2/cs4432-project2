package simpledb.index.extensiblehash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class GlobalIndex implements Index {
    private String idxname;
    private Schema sch;
    private Transaction tx;
    private Constant searchKey = null;
    private TableScan ts = null;

    /**
     * Constructor
     */
    public GlobalIndex(String idxname, Schema sch, Transaction tx){
        this.idxname = idxname;
        this.sch = sch;
        this.tx = tx;
    }

    @Override
    public void beforeFirst(Constant searchkey) {
        close();
        this.searchKey = searchkey;
        //this will be used for the key
        String binaryFormat = Integer.toBinaryString(searchkey.hashCode());
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
    public void insert(Constant dataval, RID datarid) {

    }

    @Override
    public void delete(Constant dataval, RID datarid) {

    }

    @Override
    public void close() {
        if (ts != null){
            ts.close();
        }
    }
}
