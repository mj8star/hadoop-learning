package com.licerlee.hadoop.topn;

import org.apache.hadoop.io.WritableComparator;


public class TopnSortComparator extends WritableComparator {

    // ?
    public TopnSortComparator() {
        super(TKey.class,true);
    }

    // 实现自定义的大小比较规则
    @Override
    public int compare(Object a, Object b) {

        TKey tKey1 = (TKey) a;
        TKey tKey2 = (TKey) b;

        int c1 = Integer.compare(tKey1.getYear(), tKey2.getYear());
        if (c1 == 0) {
            int c2 = Integer.compare(tKey1.getMonth(), tKey2.getMonth());
            if (c2 == 0) {

                // 温度倒序
                int compare = Integer.compare(tKey1.getTempreture(), tKey2.getTempreture());
                return -compare;
            }
            return c2;
        }
        return c1;
    }
}
