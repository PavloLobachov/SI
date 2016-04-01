package com.epam.hbase.logging;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.FileAppender;

public class TimestampFileAppender extends FileAppender{

    private static final String TARGET = "\\{timestamp\\}";

    protected String timestampPattern = null;

    public void setFile(String file) {

        if (timestampPattern != null) {
            super.setFile(file.replaceAll(TARGET, new SimpleDateFormat(timestampPattern).format(Calendar.getInstance().getTime())));
        } else {
            super.setFile(file);
        }
    }

    public void setFile(String fileName, boolean append, boolean bufferedIO, int bufferSize) throws IOException {

        if (timestampPattern != null) {
            super.setFile(fileName.replaceAll(TARGET, new SimpleDateFormat(timestampPattern).format(Calendar.getInstance().getTime())), append, bufferedIO, bufferSize);
        } else {
            super.setFile(fileName, append, bufferedIO, bufferSize);
        }
    }

    public String getTimestampPattern() {
        return timestampPattern;
    }

    public void setTimestampPattern(String timestampPattern) {
        this.timestampPattern = timestampPattern;
    }


}
