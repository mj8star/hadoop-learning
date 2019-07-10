package com.licerlee.hadoop.topn;

import org.apache.hadoop.io.WritableComparator;

public class TopnGroupingComparator extends WritableComparator {

    // TODO
    public TopnGroupingComparator() {
        super(TKey.class, true);
    }
}
