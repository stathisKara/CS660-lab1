/**
 * Iterates over the fields in a TupleDesc.
 * Called by TupleDesc.
 */
package simpledb;

import java.util.Iterator;

public class TupleDescIterator implements Iterator {
    private TupleDesc td;
    private int position = 0;

    public TupleDescIterator(TupleDesc td) {
        this.td = td;
    }

    @Override
    public boolean hasNext() {
        if (position < td.getItems().length)
            return true;
        else
            return false;
    }

    @Override
    public TupleDesc.TDItem next() {
        if (this.hasNext()) {
            TupleDesc.TDItem x = td.getItems()[position];
            position++;
            return x;
        }
        else
            return null;
    }
}
