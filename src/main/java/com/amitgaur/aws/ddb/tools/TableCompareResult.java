package com.amitgaur.aws.ddb.tools;

/**
 * Created by agaur on 11/21/15.
 */
public class TableCompareResult {


    public Long numCompares;
    public Long equals;
    public Long notEquals;


    public String toString(){

        StringBuffer b = new StringBuffer();
        return b.append("TotalCompares:").append(numCompares).append(" equals :").append(equals).append(" notEq").append(notEquals).toString();
    }

}
