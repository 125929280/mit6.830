package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.List;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */

    private int buckets[];
    private int min, max, ntups;
    private int width;

    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
//        System.out.println((max - min + 1.0) / buckets + " " + ()));
        width = (int) (Math.ceil((max - min + 1.0) / buckets));
        ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if(v < min || v > max) return ;
        int index = (int)((v - min) / width);
        buckets[index] ++;
        ntups ++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        int index = (int)((v - min) / width);
        double sum = 0.0;
        switch (op) {
            case EQUALS:
                if(index < 0 || index >= buckets.length) {
                    return 0.0;
                }
//                System.out.println(buckets[index] + " " +  width + " " + ntups);
                return (1.0 * buckets[index] / width) / ntups;
            case GREATER_THAN:
                if(index < 0) {
                    return 1.0;
                } else if(index >= buckets.length) {
                    return 0.0;
                }
                int b_right = min + (index + 1) * width - 1;
                sum = 1.0 * buckets[index] * (b_right - v) / width;
                for(int i = index+1;i < buckets.length;i ++) {
                    sum += buckets[i];
                }
                return sum / ntups;
            case LESS_THAN:
                if(index < 0) {
                    return 0.0;
                } else if(index >= buckets.length) {
                    return 1.0;
                }
                int b_left = min + index * width;
                sum = 1.0 * buckets[index] * (v - b_left) / width;
                for(int i = 0;i < index;i ++) {
                    sum += buckets[i];
                }
                return sum / ntups;
            case NOT_EQUALS:
                return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v-1);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v+1);
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        int sum = 0;
        for(int bucket : buckets) sum += bucket;
        if(sum == 0) return 0.0;
        return 1.0 * sum / ntups;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String ans = "";
        for(int i = 0;i < buckets.length;i ++) ans += "[" + i + "," + buckets[i] + "] ";
        return ans;
    }
}
