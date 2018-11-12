package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
	private DbIterator it;
	private int aggrField;
	private int groupField;
	private Aggregator.Op aggrOperator;
	private Aggregator aggregator;
	private DbIterator aggrIt;
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * Constructor.
	 * <p>
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an {@link IntAggregator} or {@link StringAggregator} to help
	 * you with your implementation of readNext().
	 *
	 * @param child  The DbIterator that is feeding us tuples.
	 * @param afield The column over which we are computing an aggregate.
	 * @param gfield The column over which we are grouping the result, or -1 if
	 *               there is no grouping
	 * @param aop    The aggregation operator to use
	 */
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		// some code goes here
		this.it = child;
		this.aggrField = afield;
		this.groupField = gfield;
		this.aggrOperator = aop;
		
		Type aggrType = this.it.getTupleDesc().getFieldType(this.aggrField);
		System.out.println("aggr type is " + aggrType.toString());
		Type groupType = null;
		if (this.groupField != Aggregator.NO_GROUPING)
			groupType = this.it.getTupleDesc().getFieldType(this.groupField);
		
		if (aggrType == Type.INT_TYPE)
			this.aggregator = new IntegerAggregator(this.groupField, groupType, this.aggrField, this.aggrOperator);
		else
			this.aggregator = new StringAggregator(this.groupField, groupType, this.aggrField, this.aggrOperator);
		
	}
	
	/**
	 * @return If this aggregate is accompanied by a groupby, return the groupby
	 * field index in the <b>INPUT</b> tuples. If not, return
	 * {@link simpledb.Aggregator#NO_GROUPING}
	 */
	public int groupField() {
		// some code goes here
		return this.groupField;
	}
	
	/**
	 * @return If this aggregate is accompanied by a group by, return the name
	 * of the groupby field in the <b>OUTPUT</b> tuples If not, return
	 * null;
	 */
	public String groupFieldName() {
		// some code goes here
		if (this.groupField == Aggregator.NO_GROUPING)
			return null;
		return this.it.getTupleDesc().getFieldName(this.groupField);
		
	}
	
	/**
	 * @return the aggregate field
	 */
	public int aggregateField() {
		// some code goes here
		return this.aggrField;
	}
	
	/**
	 * @return return the name of the aggregate field in the <b>OUTPUT</b>
	 * tuples
	 */
	public String aggregateFieldName() {
		// some code goes here
		return this.it.getTupleDesc().getFieldName(this.aggrField);
	}
	
	/**
	 * @return return the aggregate operator
	 */
	public Aggregator.Op aggregateOp() {
		// some code goes here
		return this.aggrOperator;
	}
	
	public static String nameOfAggregatorOp(Aggregator.Op aop) {
		return aop.toString();
	}
	
	public void open() throws NoSuchElementException, DbException,
			TransactionAbortedException {
		// some code goes here
		super.open();
		this.it.open();
		while (this.it.hasNext())
			this.aggregator.mergeTupleIntoGroup(this.it.next());
		this.aggrIt = this.aggregator.iterator();
		this.aggrIt.open();
	}
	
	/**
	 * Returns the next tuple. If there is a group by field, then the first
	 * field is the field by which we are grouping, and the second field is the
	 * result of computing the aggregate, If there is no group by field, then
	 * the result tuple should contain one field representing the result of the
	 * aggregate. Should return null if there are no more tuples.
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (this.aggrIt.hasNext())
			return this.aggrIt.next();
		return null;
	}
	
	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		this.aggrIt.rewind();
	}
	
	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field,
	 * this will have one field - the aggregate column. If there is a group by
	 * field, the first field will be the group by field, and the second will be
	 * the aggregate value column.
	 * <p>
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
	 * given in the constructor, and child_td is the TupleDesc of the child
	 * iterator.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return this.it.getTupleDesc();
	}
	
	public void close() {
		// some code goes here
		super.close();
		this.it.close();
		this.aggrIt.close();
	}
	
	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		return new DbIterator[]{this.aggrIt};
	}
	
	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
		this.aggrIt = children[0];
	}
	
}
