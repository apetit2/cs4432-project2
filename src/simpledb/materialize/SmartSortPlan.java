package simpledb.materialize;

import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.tx.Transaction;

import java.util.List;

public class SmartSortPlan extends SortPlan {
    /**
     * Creates a sort plan for the specified query.
     *
     * @param p          the plan for the underlying query
     * @param sortfields the fields to sort by
     * @param tx         the calling transaction
     */
    public SmartSortPlan(TablePlan p, List<String> sortfields, Transaction tx) {
        super(p, sortfields, tx);
    }

    /**
     *
     * @see simpledb.materialize.SortPlan#open()
     */
    public Scan open() {
        TableScan src = (TableScan) p.open();
        List<TempTable> runs = splitIntoRuns(src);
        while (runs.size() > 2)
            runs = doAMergeIteration(runs);
        return new SmartSortScan(runs, comp, src);
    }
}
