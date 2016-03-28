package com.epam.hbase.exceptions;

/**
 * Created by hiken on 3/28/16.
 */
class TableNotFoundException extends Exception{
    public TableNotFoundException(String message) {
        super(message);
    }
}
