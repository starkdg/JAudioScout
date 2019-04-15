package org.phash.audioscout.server;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;

public class TestKratiMetaDataStore {

    static public int key = 0;
    static public final String valueString = "Happy families are all alike. Unhappy families are unhappy in their own way.";
    static public final String homedir = "ext/au2";
    static public KratiMetaDataStore store;

    @BeforeClass static public void testCreateMetaDataStore(){

	File storeDir = new File(homedir);
	try {
	    store = new KratiMetaDataStore(storeDir, 1000, 5, 64, false);
	} catch (Exception ex){
	    System.out.println("unable to init metadata store - " + ex.getMessage());
	}
	assert(store != null);
	
	try {
	    key = store.storeMetaData(valueString);
	} catch (Exception ex){
	    System.out.println("unable to store metadata - " + ex.getMessage());
	}
	assert(key != 0);
    }

    @Test public void testStoreMetaData(){
	int key2 = 0;
	try {
	    key2 = store.storeMetaData("Another line ... ");
	} catch (Exception ex){
	    System.out.println("unable to store metadata - " + ex.getMessage());
	}
	assert(key2 != 0);
    }

    @Test public void testGetMetaData(){
	String retrievedStr = store.getMetaData(key);
	assert(retrievedStr.compareTo(valueString) == 0);
    }

    @AfterClass static public void testDeleteId(){
	boolean deleted = true;
	try {
	    deleted = store.deleteId(key);
	} catch (Exception ex){
	    System.out.println("unable to delete id" + ex.getMessage());
	    deleted = false;
	}
	assert(deleted);
    }

    @After public void testSync(){
	boolean done = true;
	try {
	    store.sync();
	} catch (Exception ex){
	    System.out.println("unable to sync datastore - " + ex.getMessage());
	    done = false;
	}
	assert(done);
    }
}