package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
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
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.fileDesc = f;
        this.tupleDesc = td;
        this.uniqueId = f.getAbsoluteFile().hashCode();
        this.numPages = 0;
        this.pages = new Vector<>();
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

    public Vector<Page> getPages(){
        return this.pages;
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
        int pg_size = BufferPool.getPageSize();
        try {
            RandomAccessFile file = new RandomAccessFile(fileDesc, "r");
            try {
                //file.seek(pid.pageNumber() * pg_size);
//                System.out.println("offset is " + pg_size);
                byte data[] = new byte[pg_size];
                file.read(data, pid.pageNumber() * pg_size, pg_size);
                file.close();
                try {
                    numPages++;
                    HeapPage page = new HeapPage((HeapPageId) pid, data);
//                    pages.add(page);
                    //                  System.out.println(numPages);
                    return page;
                } catch (IOException e) {
                    return null;
                }
            } catch (IOException e) {
                System.out.println("Reading file failed");
            }
        } catch (FileNotFoundException e) {
            System.out.println("File does not exist");
        }
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
        if (this.fileDesc.length() % BufferPool.getPageSize() == 0)
            return (int) this.fileDesc.length() / BufferPool.getPageSize();
        else
            return (int) this.fileDesc.length() / BufferPool.getPageSize() + 1;
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

            // parameters of the iterator
            private int currentPageNumber = 0;
            private Page currentPage = null;
            private PageId currentPageId = null;
            private Iterator<Tuple> tuples;

            @Override
            /**
             * The open() method is like initializing the iterator,
             * initialize all the objects of the iterator.
             * Throw exceptions if something goes wrong.
             */
            public void open() throws DbException, TransactionAbortedException {
                int tableId = HeapFile.this.getId();
                this.currentPageId = new HeapPageId(tableId, this.currentPageNumber);
                this.currentPage = Database.getBufferPool().getPage(tid, this.currentPageId, null);
                this.tuples = ((HeapPage) this.currentPage).iterator();
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
                if (this.tuples != null) {
                    // if tuple null, iterator closed
                    if (this.tuples.hasNext()) {
                        return true;
                    } else {
                        if (this.currentPageNumber < HeapFile.this.numPages - 1) {
                            int tableId = HeapFile.this.getId();
                            this.currentPageNumber++;
                            this.currentPageId = new HeapPageId(tableId, this.currentPageNumber);
                            this.currentPage = Database.getBufferPool().getPage(tid, this.currentPageId, null);
                            this.tuples = ((HeapPage) this.currentPage).iterator();
                            return this.hasNext();
                        }
                    }
                }
                return false;
            }

            @Override
            /**
             * get the next tuple on the current page.
             * If we are at the end of the page and there is another page, get the first
             * tuple on the next page. If we are on the last page, throw a NoSuchElement
             * exception.
             */
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (this.tuples != null) {
                    return this.tuples.next();
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            /**
             * just reset everything, like opening again
             */
            public void rewind() throws DbException, TransactionAbortedException {
                int tableId = HeapFile.this.getId();
                this.currentPageNumber = 0;
                this.currentPageId = new HeapPageId(tableId, this.currentPageNumber);
                this.currentPage = Database.getBufferPool().getPage(tid, this.currentPageId, null);
                this.tuples = ((HeapPage) this.currentPage).iterator();
            }

            @Override
            /**
             * make everything invalid,
             * the opposite of open()
             */
            public void close() {
                this.currentPageNumber = 0;
                this.currentPageId = null;
                this.currentPage = null;
                this.tuples = null;
            }
        };

        return db_it;
    }

}

