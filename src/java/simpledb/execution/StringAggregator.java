package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int gbFieldIndex, aFieldIndex;

    private Type gbfieldtype;

    private Op what;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFieldIndex = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.aFieldIndex = afield;
        this.what = what;
        aggHandler = new CountHandler();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField;
        if(gbFieldIndex == NO_GROUPING) {
            gbField = null;
        } else {
            gbField = tup.getField(gbFieldIndex);
        }
        Field aField = tup.getField(aFieldIndex);
        aggHandler.handler(gbField);
    }

    private abstract class AggHandler {
        HashMap<Field, Integer> agg;

        public AggHandler() {
            agg = new HashMap<>();
        }

        public HashMap<Field, Integer> get() {
            return agg;
        }

        abstract void handler(Field gbField);
    }

    AggHandler aggHandler;

    public class CountHandler extends AggHandler {
        @Override
        void handler(Field gbField) {
            if(agg.containsKey(gbField)) {
                agg.put(gbField, agg.get(gbField) + 1);
            } else {
                agg.put(gbField, 1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        HashMap<Field, Integer> agg = aggHandler.get();
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc tupleDesc;
        if(gbFieldIndex == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"AggValue"});
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(agg.get(null)));
            tuples.add(tuple);
        } else {
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupByValue", "AggValue"});
            for(Map.Entry<Field, Integer> entry : agg.entrySet()) {
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
