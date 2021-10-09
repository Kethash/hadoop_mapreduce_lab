package com.opstty.writable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DistrictCountWritable implements WritableComparable<DistrictCountWritable> {
    // Pair to store adress and the year of the planting
    private int district;
    private int value;

    public DistrictCountWritable() {
    }

    public DistrictCountWritable(int district ,int value) {
        this.setDistrict(district);
        this.setValue(value);
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    public void setDistrict(int district) {
        this.district = district;
    }

    public int getDistrict() {
        return this.district;
    }


    public void readFields(DataInput in) throws IOException {
        this.district = in.readInt();
        this.value = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.district);
        out.writeInt(this.value);
    }

    public boolean equals(Object o) {
        if (!(o instanceof DistrictCountWritable)) {
            return false;
        } else {
            DistrictCountWritable other = (DistrictCountWritable)o;
            return this.district == other.district;
        }
    }

    public int hashCode() {
        return this.district;
    }

    public int compareTo(DistrictCountWritable o) {
        int thisValue = this.district;
        int thatValue = o.district;
        return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }

    public String toString() {
        return Integer.toString(this.district) + " " + Integer.toString(this.value);
    }

    static {
        WritableComparator.define(DistrictCountWritable.class, new DistrictCountWritable.Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(DistrictCountWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int thisValue = readInt(b1, s1);
            int thatValue = readInt(b2, s2);
            return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
        }
    }
}
