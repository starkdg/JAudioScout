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

public class TestKratiAudioHashStore {
    static public final Integer guid   =  4005;
    static public final Integer length = 10000;
    static public final String homedir = "ext/au";
    static public final Float threshold = 0.025f;
    static public final Integer bs = 128;
    static public int[] array;
    static public KratiAudioHashStore hashstore;

    @BeforeClass static public void testCreateAudioHashStore(){
	File storeDir = new File(homedir);
	try {
	    hashstore = new KratiAudioHashStore(storeDir, 100, 500, 64, false);
	} catch (IOException ex){
	    System.out.println("unable to init data store - " + ex.getMessage());
	}	    
	assert(hashstore != null);

	Random rnd = new Random(2823832);
	array = new int[length];

	for (int i=0;i<length;i++){
	    array[i] = rnd.nextInt();
	}

	boolean stored = true;
	try {
	    stored = hashstore.storeAudioHash(guid, array);
	} catch (Exception ex){
	    stored = false;
	    System.out.println("unable to store - " + ex.getMessage());
	}
	assert(stored);
    }


    @Test public void testGetCandidates(){
	System.out.printf("test getCandidates function ...\n");

	int P = 3;
	int nbcands = 1 << P;
	int value = 0x00000000;
	int toggles = 0x00000007;
	int[] candidates = new int[nbcands];

	hashstore.getCandidates(value, toggles, candidates);

	System.out.printf("permutations:\n");
	for (int i=0;i<nbcands;i++){
	    System.out.printf(" %x ", candidates[i]);
	}
	System.out.printf("\n");

	toggles = 0xe0000000;
	hashstore.getCandidates(value, toggles, candidates);
	System.out.printf("permutations:\n");
	for (int i=0;i<nbcands;i++){
	    System.out.printf(" %x ", candidates[i]);
	}
	System.out.printf("\n");

	assert(true);
    }

    @Test public void testStore(){
	int uid = 4000;
	Random rnd = new Random(1234567890);
	int[] array2 = new int[length];

	System.out.println("test store ... ");
	for (int i=0;i<length;i++){
	    array2[i] = rnd.nextInt();
	}

	boolean stored = true;
	try {
	    stored = hashstore.storeAudioHash(uid, array2);
	} catch (Exception ex){
	    stored = false;
	    System.out.println("unable to store - " + ex.getMessage());
	}
	assert(stored);
    }

    @Test public void testLookup(){
	int qlength = length/10;
	int[] qarray = new int[qlength];
	System.arraycopy(array, 0, qarray, 0, qlength);

	ArrayList<KratiAudioHashStore.FoundId> list = hashstore.lookupAudioHash(qarray, null, threshold, bs);
	assert(list.size() > 0);

	KratiAudioHashStore.FoundId result = list.get(0);
	assert(result.id == guid.intValue());

    }

    @Test public void testSync(){
	boolean success = true;
	try {
	    hashstore.sync();
	} catch (IOException ex){
	    System.out.println("unable to sync datastore - " + ex.getMessage());
	    success = false;
	}
	assert(success);
    }


    @AfterClass static public void testClose(){
	boolean success = true;
	try {
	    hashstore.close();
	} catch (IOException ex){
	    System.out.println("unable to close data store - " + ex.getMessage());
	    success = false;
	}
	assert(success);
    }
}