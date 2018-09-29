package simpledb;

import java.io.Serializable;
import java.util.*;
import java.lang.instrument.Instrumentation;


import static java.lang.System.exit;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * container for TDItems
     * This will be iterated over with the following iterator
     */
    private TDItem items[];

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        //for all items in t
        return new TupleDescIterator(this);
    }



    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */


    /**
     * the constructor of tuledesc takes as arguments
     * an array of types and an array of field names corresponding to that type
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        int typeAr_len = typeAr.length;
        if (typeAr_len <= 0) {
            System.out.println("Table should have at least one field\n");
            exit(1);
        }
        int fieldAr_len = fieldAr.length;

        if (typeAr_len != fieldAr_len) {
            System.out.println("Number of field names should be the same as number of field types\n");
            exit(2);
        }
        this.items = new TDItem[typeAr_len];
        for (int i = 0; i < typeAr_len; i++)
            this.items[i] = new TDItem(typeAr[i], fieldAr[i]);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        int typeAr_len = typeAr.length;
        if (typeAr_len <= 0) {
            System.out.println("Table should have at least one field\n");
            exit(1);
        }
        this.items = new TDItem[typeAr_len];
        for (int i = 0; i < typeAr_len; i++)
            this.items[i] = new TDItem(typeAr[i], null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.items.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        try {
            return this.items[i].fieldName;
        } catch (NoSuchElementException e) {
            System.out.println("The tuple only contains " + this.items.length +
                    "and not " + i);
        }
        return null;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        try {
            return this.items[i].fieldType;
        } catch (NoSuchElementException e) {
            System.out.println("The tuple only contains " + this.items.length +
                    "and not " + i);
        }
        return null;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException();
        }
        int len = this.items.length;
        for (int i = 0; i < len; i++) {
            if (this.items[i].fieldName.equals(name))
                return i;
        }
        System.out.println("Index not found\n");
        throw new NoSuchElementException();
    }

    /**
    Get size of object
     */
    /*public static class ObjectSizeFetcher {
        private static Instrumentation instrumentation;

        public static void premain(String args, Instrumentation inst) {
            instrumentation = inst;
        }

        public static long getObjectSize(Object o) {
            return instrumentation.getObjectSize(o);
        }
    }*/
    public static class InstrumentationAgent {
        private static volatile Instrumentation globalInstrumentation;

        public static void premain(final String agentArgs, final Instrumentation inst) {
            globalInstrumentation = inst;
        }

        public static long getObjectSize(final Object object) {
            if (globalInstrumentation == null) {
                throw new IllegalStateException("Agent not initialized.");
            }
            return globalInstrumentation.getObjectSize(object);
        }
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        /*int size = 0;
        Iterator<TDItem> it = iterator();
        while (it.hasNext()) {
            size += InstrumentationAgent.getObjectSize(it.next().fieldType);
        }
        return size;*/
        return 8;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TupleDescIterator it1 = new TupleDescIterator(td1);
        TupleDescIterator it2 = new TupleDescIterator(td2);
        int combinedLength = td1.numFields() + td2.numFields();
        Type[] combinedTypes = new Type[combinedLength];
        String[] combinedFields = new String[combinedLength];

        int i = 0;
        while (it1.hasNext()){
            combinedTypes[i] = it1.next().fieldType;
            combinedFields[i] = it1.next().fieldName;
            i++;
        }

        while (it2.hasNext()){
            combinedTypes[i] = it2.next().fieldType;
            combinedFields[i] = it2.next().fieldName;
            i++;
        }

        return new TupleDesc(combinedTypes, combinedFields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o == null) return false;
        if (o == this) return true;
        if (!(o instanceof TupleDesc)) return false;
        TupleDesc tempTupD = (TupleDesc)o;
        if (tempTupD.items.length != this.items.length) return false;
        for (int i=0; i <this.items.length; i++) {
            if (tempTupD.items[i].fieldType != this.items[i].fieldType)
                return false;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return Arrays.deepHashCode(this.items);

//        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
//        String result = "" ;
        StringBuilder result = new StringBuilder();
        int len = this.items.length;
        for (int i = 0; i < len; i++) {
            if (i != 0)
                result.append(",");
            result.append(this.items[i].toString());
        }
        return result.toString();
    }

    public TDItem[] getItems() {
        return items;
    }
}
