package simpledb;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {
	
	private static final long serialVersionUID = 1L;
	
	private TransactionId transactionId;
	private DbIterator it;
	private int tid;
	private boolean newTupleInserted = false;
	private TupleDesc td;
	
	/**
	 * Constructor.
	 *
	 * @param t       The transaction running the insert.
	 * @param child   The child operator from which to read tuples to be inserted.
	 * @param tableId The table in which to insert tuples.
	 * @throws DbException if TupleDesc of child differs from table into which we are to
	 *                     insert.
	 */
	public Insert(TransactionId t, DbIterator child, int tableId)
			throws DbException {
		// some code goes here
		this.transactionId = t;
		this.it = child;
		this.tid = tableId;
	}
	
	public TupleDesc getTupleDesc() {
		// some code goes here
		return this.td;
	}
	
	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		super.open();
		this.it.open();
		this.newTupleInserted = false;
	}
	
	public void close() {
		// some code goes here
		super.close();
		this.it.close();
	}
	
	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		this.it.rewind();
	}
	
	/**
	 * Inserts tuples read from child into the tableId specified by the
	 * constructor. It returns a one field tuple containing the number of
	 * inserted records. Inserts should be passed through BufferPool. An
	 * instances of BufferPool is available via Database.getBufferPool(). Note
	 * that insert DOES NOT need check to see if a particular tuple is a
	 * duplicate before inserting it.
	 *
	 * @return A 1-field tuple containing the number of inserted records, or
	 * null if called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (this.newTupleInserted)
			return null;
		int inserted = 0;
		while (this.it.hasNext()) {
			Tuple curTuple = this.it.next();
			
			try {
				Database.getBufferPool().insertTuple(this.transactionId, this.tid, curTuple);
			}
			catch (IOException e) {
				throw new DbException("Insertion failed");
			}
			inserted ++;
		}
		Tuple finalTuple = new Tuple(this.td);
		finalTuple.setField(0, new IntField(inserted));
		this.newTupleInserted = true;
		return finalTuple;
	}
	
	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		return null;
	}
	
	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
	}
}
