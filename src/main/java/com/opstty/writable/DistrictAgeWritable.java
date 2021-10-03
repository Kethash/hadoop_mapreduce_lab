package com.opstty.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DistrictAgeWritable implements WritableComparable<DistrictAgeWritable> {
    // Pair to store adress and the year of the planting
    private Text address;
    private int value;

    public DistrictAgeWritable() {
    }

    public DistrictAgeWritable(Text address ,int value) {
        this.setAdress(address);
        this.setValue(value);
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    public void setAdress(Text address) {
        this.address = address;
    }

    public Text getAddress() {
        return this.address;
    }


    public void readFields(DataInput in) throws IOException {
        this.value = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.value);
    }

    public boolean equals(Object o) {
        if (!(o instanceof DistrictAgeWritable)) {
            return false;
        } else {
            DistrictAgeWritable other = (DistrictAgeWritable)o;
            return this.value == other.value;
        }
    }

    public int hashCode() {
        return this.value;
    }

    public int compareTo(DistrictAgeWritable o) {
        int thisValue = this.value;
        int thatValue = o.value;
        return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }

    public String toString() {
        return Integer.toString(this.value);
    }

    static {
        WritableComparator.define(DistrictAgeWritable.class, new DistrictAgeWritable.Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(DistrictAgeWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int thisValue = readInt(b1, s1);
            int thatValue = readInt(b2, s2);
            return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
        }
    }
}
