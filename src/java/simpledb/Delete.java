package simpledb;

//import javax.xml.bind.DataBindingException;
import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
	
	private TransactionId tid;
	private TupleDesc td;
	private boolean deleted;
	private DbIterator it;
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * Constructor specifying the transaction that this delete belongs to as
	 * well as the child to read from.
	 *
	 * @param t     The transaction this delete runs in
	 * @param child The child operator from which to read tuples for deletion
	 */
	public Delete(TransactionId t, DbIterator child) {
		// some code goes here
		this.tid = t;
		this.it = child;
		this.deleted = false;
		this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Deleted"});
	}
	
	public TupleDesc getTupleDesc() {
		// some code goes here
		return this.td;
	}
	
	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		super.open();
		this.it.open();
		this.deleted = false;
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
	 * Deletes tuples as they are read from the child operator. Deletes are
	 * processed via the buffer pool (which can be accessed via the
	 * Database.getBufferPool() method.
	 *
	 * @return A 1-field tuple containing the number of deleted records.
	 * @see Database#getBufferPool
	 * @see BufferPool#deleteTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (this.deleted)
			return null;
		int count = 0;
		while (this.it.hasNext()) {
			Tuple t = it.next();
			try {
				Database.getBufferPool().deleteTuple(this.tid, t);
			} catch (IOException e) {
				throw new DbException("Deletion failed");
			}
			count += 1;
		}
		Tuple t = new Tuple(this.td);
		t.setField(0, new IntField(count));
		this.deleted = true;
		return t;
	}
	
	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		return new DbIterator[]{this.it};
	}
	
	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
		this.it = children[0];
	}
	
}
