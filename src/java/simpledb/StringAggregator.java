package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	
	private static final long serialVersionUID = 1L;
	
	private int gbField;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private HashMap<Field, Integer> groupResult;
	
	/**
	 * Aggregate constructor
	 *
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */
	
	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbField = gbField;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		this.groupResult = new HashMap<>();
	}
	
	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the constructor
	 *
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		// some code goes here
		
		Field groupByField = null;
		if (this.gbField != Aggregator.NO_GROUPING)
			groupByField = tup.getField(this.gbField);
		
		//Initialize new key value if it does not exist in hashmap
		if (!this.groupResult.containsKey(groupByField)) {
			this.groupResult.put(groupByField, 0);
		}
		
		int curVal = this.groupResult.get(groupByField);
		this.groupResult.put(groupByField, curVal + 1);
	}
	
	/**
	 * Create a DbIterator over group aggregate results.
	 *
	 * @return a DbIterator whose tuples are the pair (groupVal,
	 * aggregateVal) if using group, or a single (aggregateVal) if no
	 * grouping. The aggregateVal is determined by the type of
	 * aggregate specified in the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		if (!(this.what == Op.COUNT))
			throw new UnsupportedOperationException("please implement me for lab3");
		
		//---------- Create the new group tuple descriptor ----------
		String[] groupNames;
		Type[] groupTypes;
		if (this.gbField == Aggregator.NO_GROUPING) {
			groupNames = new String[]{"aggregateVal"};
			groupTypes = new Type[]{Type.INT_TYPE};
		}
		else {
			groupNames = new String[]{"groupVale", "aggregateVal"};
			groupTypes = new Type[]{this.gbfieldtype, Type.INT_TYPE};
		}
		
		TupleDesc groupTd = new TupleDesc(groupTypes, groupNames);
		
		//---------- Create list of aggregates tuples ----------
		ArrayList<Tuple> groupedTuples = new ArrayList<>();
		for (Field groupKey : this.groupResult.keySet()) {
			int aggraval = this.groupResult.get(groupKey);
			
			Tuple groupedTuple = new Tuple(groupTd);
			if (this.gbField == Aggregator.NO_GROUPING)
				groupedTuple.setField(0, new IntField(aggraval));
			else {
				groupedTuple.setField(0, groupKey);
				groupedTuple.setField(1, new IntField(aggraval));
			}
			groupedTuples.add(groupedTuple);
		}
		
		return new TupleIterator(groupTd, groupedTuples);
	}
	
}
