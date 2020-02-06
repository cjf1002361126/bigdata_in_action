package mr.custompartitoner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * @author cjf
 * @description :
 * 自定义partitioner：该功能就是想
 * 让Dear落到0号分区，Bear落到1号分区,River落到2号分区，Car落到3号分区；
 * <p>
 * Partitioner<Text, IntWritable>
 * 其中：<Text, IntWritable> 为 map方法输出的key,value的类型;
 * @create 2020-02-05 22:28
 */
public class CustomPartitioner extends Partitioner<Text, IntWritable> {

    static Map<String, Integer> dict = new HashMap<String, Integer>();

    //定义每个键对应的分区index，使用map数据结构完成
    static {
        dict.put("Dear", 0);
        dict.put("Bear", 1);
        dict.put("River", 2);
        dict.put("Car", 3);
    }

    public static int getPartition1(String key, int numReduceTasks) {
        Integer partitionIndex = dict.get(key.toString());
        if (partitionIndex != null) {
            return partitionIndex;
        }
        return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }

    public static void main(String[] args) {
        System.out.println(getPartition1("Dear", 4));
    }

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        Integer partitionIndex = dict.get(key.toString());
        if (partitionIndex != null) {
            return partitionIndex;
        }
        // 如果取不到，则使用默认分区
        return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
