package org.phash.audioscout.hash;


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
     * Note: To convert to no. of seconds in file:
     *       no. seconds = 64*position/samplerate
     **/
    public int position;

    /**
     * id number of entry
     **/
    public int id;

    public MatchResult(String name, float cs, int position, int id){
	this.name = name;
	this.cs = cs;
	this.position = position;
	this.id = id;
    }

}