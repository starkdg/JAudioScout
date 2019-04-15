package org.phash.audioscout.server;

import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ByteOrder;
import krati.io.Serializer;
import krati.io.SerializationException;

public class TableValueSerializer implements Serializer<ArrayList<TableValue>> {

    @Override
    public byte[] serialize(ArrayList<TableValue> list) throws SerializationException {
		int nbytes = TableValue.getSizeInBytes();
		if (list != null && list.size() > 0){
			ByteBuffer buf = ByteBuffer.allocate(nbytes*list.size());
			for (TableValue tblval : list){
				buf.putInt(tblval.id);
				buf.putInt(tblval.pos);
			}
			return buf.array();
		}
		return null;
    }

    @Override
    public ArrayList<TableValue> deserialize(byte[] bytes) throws SerializationException {
		if (bytes == null || bytes.length <= 0) return null;
		ArrayList<TableValue> list = new ArrayList<TableValue>();
		IntBuffer buf = ByteBuffer.wrap(bytes).asIntBuffer();
		while (buf.remaining() >= 2){
			TableValue tblval = new TableValue();
			tblval.id = buf.get();
			tblval.pos = buf.get();
			list.add(tblval);
		}
		return list;
    }
}
