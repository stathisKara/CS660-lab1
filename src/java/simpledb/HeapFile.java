package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File fileDesc;
    private TupleDesc tupleDesc;
    private int uniqueId;
    private int numPages;
    private Vector<Page> pages;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        if (f ==null){
            throw new NullPointerException("You need to specify a name of an existing file\n");
        }
        this.fileDesc = f;
        this.tupleDesc = td;
        this.uniqueId = f.getAbsoluteFile().hashCode();
        this.numPages =0;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.fileDesc;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return this.uniqueId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return 0;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        /**
         * create a new DbFile iterator and implement the methods of the DbFileIterator interface
         */
        DbFileIterator db_it = new DbFileIterator() {
            /**
             * we need to know which page we are on and which tuple we are on,
             * and we want to store the page we are on for easy access.
             */
            private boolean isOpen = false;
            private int page_position;
            private int tuple_position;
            HeapPage p;

            @Override
            /**
             * The open() method is like initializing the iterator,
             * initialize all the objects of the iterator.
             * Throw exceptions if something goes wrong.
             */
            public void open() throws DbException, TransactionAbortedException {
                try {
                    page_position = 0;
                    tuple_position = 0;
                    p = (HeapPage) Database.getBufferPool().getPage(tid, pages.get(page_position), Permissions.READ_ONLY);
                    isOpen = true;
                } catch (DbException e) {
                    e.printStackTrace();
                    throw new DbException("\nThere was a problem opening or accessing the database.");
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                    throw new TransactionAbortedException();
                }
            }

            @Override
            /**
             * get the next tuple on the current page.
             * If the iterator is not open, return false.
             * If we are at the end of the page, check if there is another page.
             * If there is, there is another tuple. If there is not another page,
             * then there are no more tuples.
             */
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!isOpen) {
                    return false;
                }

                if (tuple_position < p.tuples.length) {
                    return  true;
                } else if (page_position < pages.size()) {
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            /**
             * get the next tuple on the current page.
             * If we are at the end of the page and there is another page, get the first
             * tuple on the next page. If we are on the last page, throw a NoSuchElement
             * exception.
             */
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (tuple_position < p.tuples.length) {
                    // get the next tuple
                    Tuple t = p.tuples[tuple_position];

                    // increase the counter
                    tuple_position++;
                    return t;
                } else if (page_position < pages.size()) {
                    // go to the next page
                    page_position++;
                    tuple_position = 0;
                    p = (HeapPage) Database.getBufferPool().getPage(tid, pages.get(page_position).getId(), Permissions.READ_ONLY);

                    // get the first tuple on the next page
                    Tuple t =  p.tuples[tuple_position];

                    // increase the counter
                    tuple_position++;
                    return t;
                } else {
                    // we are at the end of the table
                    throw new NoSuchElementException("\nThere are no more tuples in the table.");
                }
            }

            @Override
            /**
             * Reset the iterator (i.e. do the same thing as if you are opening it for the first time)
             */
            public void rewind() throws DbException, TransactionAbortedException {
                if (!isOpen) {
                    throw new DbException("Rewind is unsupported when an iterator is closed.");
                }

                try {
                    page_position = 0;
                    tuple_position = 0;
                    p = (HeapPage) Database.getBufferPool().getPage(tid, pages.get(page_position), Permissions.READ_ONLY);
                } catch (DbException e) {
                    e.printStackTrace();
                    throw new DbException("\nRewind is unsupported.");
                }
            }

            @Override
            /**
             * Close the iterator, i.e. set isOpen to "false" so that the methods
             * of the iterator cannot run
             */
            public void close() {
                isOpen = false;
            }
        };

        return db_it;
    }

}

