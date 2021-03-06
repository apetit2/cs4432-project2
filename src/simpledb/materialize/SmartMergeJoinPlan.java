package simpledb.materialize;

import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.Arrays;
import java.util.List;

public class SmartMergeJoinPlan implements Plan {
    private Plan p1, p2;
    private String fldname1, fldname2;
    private Schema sch = new Schema();
    /**
     * Creates a mergejoin plan for the two specified queries.
     * The RHS must be materialized after it is sorted,
     * in order to deal with possible duplicates.
     *      * Creates a mergejoin plan for the two specified table plans
     * (***Does not work on regular plans)
     * TODO Fill In
     * @param p1 the LHS query plan
     * @param p2 the RHS query plan
     * @param fldname1 the LHS join field
     * @param fldname2 the RHS join field
     * @param tx the calling transaction
     */
    public SmartMergeJoinPlan(TablePlan p1, TablePlan p2, String fldname1, String fldname2, Transaction tx) {
        this.fldname1 = fldname1;
        List<String> sortlist1 = Arrays.asList(fldname1);
        this.p1 = new SmartSortPlan(p1, sortlist1, tx);

        this.fldname2 = fldname2;
        List<String> sortlist2 = Arrays.asList(fldname2);
        this.p2 = new SmartSortPlan(p2, sortlist2, tx);

        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
    }

    /** The method first sorts its two underlying scans
     * on their join field. It then returns a mergejoin scan
     * of the two sorted table scans.
     * @see simpledb.query.Plan#open()
     */
    public Scan open() {
//        System.out.println(String.format("p1 class: %s\np2 class: %s\n", p1.getClass(), p2.getClass()));
        Scan s1 = p1.open();
        SortScan s2 = (SortScan) p2.open();
        return new MergeJoinScan(s1, s2, fldname1, fldname2);
    }


    /**
     * Returns the number of block acceses required to
     * mergejoin the sorted tables.
     * Since a mergejoin can be preformed with a single
     * pass through each table, the method returns
     * the sum of the block accesses of the
     * materialized sorted tables.
     * It does <i>not</i> include the one-time cost
     * of materializing and sorting the records.
     * @see simpledb.query.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return p1.blocksAccessed() + p2.blocksAccessed();
    }

    /**
     * Returns the number of records in the join.
     * Assuming uniform distribution, the formula is:
     * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
     * @see simpledb.query.Plan#recordsOutput()
     */
    public int recordsOutput() {
        int maxvals = Math.max(p1.distinctValues(fldname1),
                p2.distinctValues(fldname2));
        return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
    }

    /**
     * Estimates the distinct number of field values in the join.
     * Since the join does not increase or decrease field values,
     * the estimate is the same as in the appropriate underlying query.
     * @see simpledb.query.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        if (p1.schema().hasField(fldname))
            return p1.distinctValues(fldname);
        else
            return p2.distinctValues(fldname);
    }

    /**
     * Returns the schema of the join,
     * which is the union of the schemas of the underlying queries.
     * @see simpledb.query.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }
}
