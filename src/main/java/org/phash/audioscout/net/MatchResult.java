package org.phash.audioscout.net;

/**
 * Results to be returned by QuerySender.java
 * Contains name, confidence score, position index of best match
 * and an integer id
 *
 * @author dgs
 **/
public class MatchResult {

    /**
     * identifying information
     **/
    public String name;

    /**
     * confidence score for result
     **/
    public float cs;

    /**
     * position in which signal was found in the indexed original file
     * Note: To convert to no. of seconds into buffer:
     *       no. seconds = 64*position/samplerate
     **/
    public int position;

    /**
     * ID number for result
     **/
    public int id;

	/**
	 * constructor
	 * @param name 
	 * @param cs   confidence score
	 * @param postion position of match
	 * @param id  integer id of match
	 *
	 **/
    public MatchResult(String name, float cs, int position, int id){
		this.name = name;
		this.cs = cs;
		this.position = position;
		this.id = id;
    }
}
