package simpledb.materialize;

import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.query.UpdateScan;

import java.util.List;

public class SmartSortScan extends SortScan {

    private final TableScan sourceTableScan;
    /**
     * Creates a sort scan, given a list of 1 or 2 runs.
     * If there is only 1 run, then s2 will be null and
     * hasmore2 will be false.
     *
     * NEW: As this scan is iterated over, the source table is
     * replaced record-by-record with the currentscan's record,
     * that is, by the time this scan is iterated over
     * the source table will be sorted
     *
     * @param runs the list of runs
     * @param comp the record comparator
     * @param associatedTablePlan the source table plan
     */
    public SmartSortScan(List<TempTable> runs, RecordComparator comp, TablePlan associatedTablePlan) {
        super(runs, comp);
        this.sourceTableScan = (TableScan) associatedTablePlan.open();
        sourceTableScan.beforeFirst();
        sourceTableScan.next();
    }

    /**
     * Moves to the next record in sorted order.
     * First, the current scan is moved to the next record.
     * Then the lowest record of the two scans is found, and that
     * scan is chosen to be the new current scan.
     *
     * NEW: The current record in the current record is then
     * set as the next record in the source scan
     * @see simpledb.materialize.SortScan#next()
     */
    public boolean next() {
        if (currentscan != null) {
            for (String fldname : sourceTableScan.getFields())
                sourceTableScan.setVal(fldname, currentscan.getVal(fldname));
            sourceTableScan.next();
            if (currentscan == s1)
                hasmore1 = s1.next();
            else if (currentscan == s2)
                hasmore2 = s2.next();
        }

        if (!hasmore1 && !hasmore2)
            return false;
        else if (hasmore1 && hasmore2) {
            if (comp.compare(s1, s2) < 0)
                currentscan = s1;
            else
                currentscan = s2;
        }
        else if (hasmore1)
            currentscan = s1;
        else if (hasmore2)
            currentscan = s2;

        return true;
    }

    /**
     * Closes the two underlying scans.
     * @see SortScan#close()
     */
    public void close() {
        super.close();
        sourceTableScan.close();
    }
}
