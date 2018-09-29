package simpledb;

import java.util.Iterator;

public class FieldsIterator implements Iterator {

    private Tuple t;
    private int position = 0;

    public FieldsIterator(Tuple t) {
        this.t = t;
    }

    @Override
    public boolean hasNext() {
        if (position < t.getAllFields().length)
            return true;
        else
            return false;
    }

    @Override
    public Field next() {
        if (this.hasNext()) {
            Field x = t.getAllFields()[position];
            position++;
            return x;
        } else
            return null;
    }
}
