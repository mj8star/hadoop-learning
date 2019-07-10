package com.licerlee.hadoop.topn;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义map key，
 * <br/>须实现序列化、反序列化、比较器接口
 * <br/>大数据中 map key的设计很重要
 *
 * <br/>
 */
public class TKey implements WritableComparable<TKey> {

    private int year;
    private int month;
    private int day;
    private int tempreture;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTempreture() {
        return tempreture;
    }

    public void setTempreture(int tempreture) {
        this.tempreture = tempreture;
    }


    /**
     * 自定义大小比较
     *  <br/> 数据格式：年-月-日-温度
     *  <br/> 通用的大小比较器一般按照时间升序
     *  <br/> 我们这个需要，是要yyyy-MM，温度降序排序，所以这里只实现通用排序，后面在实现sortComparator
     * @param o 待比较对象
     * @return
     */
    public int compareTo(TKey o) {
        int c1 = Integer.compare(this.year, o.getYear());
        if(c1 ==0){
            int c2 = Integer.compare(this.month, o.getMonth());
            if (c2 == 0) {
                return Integer.compare(this.day, o.getDay());
            }
            return c2;
        }
        return c1;
    }

    /**
     * 序列号、须与反序列化顺序保持一致
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year);
        out.writeInt(this.month);
        out.writeInt(this.day);
        out.writeInt(this.tempreture);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.tempreture = in.readInt();
    }
}
