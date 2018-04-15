package simpledb.index.extensiblehash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.RID;

public class LocalIndex implements Index {
    @Override
    public void beforeFirst(Constant searchkey) {
        
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

    }
}
