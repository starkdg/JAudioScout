package org.phash.audioscout.server;

public class TableValue {
    public int id;
    public int pos;

    static public int getSizeInBytes(){
		return 2*(Integer.SIZE/Byte.SIZE);
    }
}
