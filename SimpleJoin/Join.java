import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

public class Join extends Configured implements Tool {
	
	public static class OuterMapper extends MapReduceBase implements Mapper<LongWritable, Text, TextPair, TextPair> {
        private static int joinCol;
        
		public void map(LongWritable key, Text value, OutputCollector<TextPair, TextPair> output, Reporter reporter) throws IOException {
			String[] tuple = value.toString().split(" ");
            StringBuffer attrs = new StringBuffer();
            for (int i = 0; i < tuple.length; i++) {
                if (i != joinCol) {
                    attrs.append(tuple[i] + "\t");
                }
            }
            
            if (attrs.length() > 0) {
                attrs.deleteCharAt(attrs.length()-1);
            }
            
			output.collect(new TextPair(tuple[joinCol], "0"), new TextPair(attrs.toString(), "0"));
		}
                
        public void configure(JobConf conf) {
            joinCol = conf.getInt("OuterJoinColumn", 0);
        }
	}
 	
	public static class InnerMapper extends MapReduceBase implements Mapper<LongWritable, Text, TextPair, TextPair> {
        private static int joinCol;
    
		public void map(LongWritable key, Text value, OutputCollector<TextPair, TextPair> output, Reporter reporter) throws IOException {
			String[] tuple = value.toString().split(" ");
            StringBuffer attrs = new StringBuffer();
            for (int i = 0; i < tuple.length; i++) {
                if (i != joinCol) {
                    attrs.append(tuple[i] + "\t");
                }
            }
            
            if (attrs.length() > 0) {
                attrs.deleteCharAt(attrs.length()-1);
            }
            
			output.collect(new TextPair(tuple[joinCol], "1"), new TextPair(attrs.toString(), "1"));
		}
        
        public void configure(JobConf conf) {
            joinCol = conf.getInt("InnerJoinColumn", 0);
        }
	}
        
    public static class KeyPartitioner implements Partitioner <TextPair, TextPair> {
        @Override
        public int getPartition (TextPair key , TextPair value , int numPartitions ) {
                return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
        
        @Override
        public void configure(JobConf conf) {}
    }

	public static class JoinReducer extends MapReduceBase implements Reducer<TextPair, TextPair, Text, Text> {
		public void reduce(TextPair key, Iterator<TextPair> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            ArrayList<String> buffer = new ArrayList<String>();
            Text tag = key.getSecond();
            TextPair value = null;
            String attrs = null;
            String bAttrs = null;
            
            //System.out.println(key.getFirst().toString() + "\t" + key.getSecond().toString());
            while (values.hasNext()) {
                value = values.next();
                    
                //System.out.println(value.getFirst().toString() + "\t" + value.getSecond().toString());    
                if ((value.getSecond().compareTo(tag) == 0)) {
                    buffer.add(value.getFirst().toString());
                } else {
                    bAttrs = value.getFirst().toString();
                    for (String val : buffer) {
                        if ("".compareTo(val) != 0 && "".compareTo(bAttrs) != 0) {
                            attrs = val + "\t" + bAttrs;
                        } else {
                            attrs = val + bAttrs;
                        }
                        output.collect(key.getFirst(), new Text(attrs));
                    }
                }
            } 
            //System.out.println("----------");
		}
	}
 	
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			System.out.println("USAGE: <prog name> <R input> <R joincol> <S input> <S joincol> <output>");
			return -1;
		}

		JobConf conf = new JobConf(getConf(), getClass());

        //conf.set("mapred.job.tracker", "local");
        //conf.set("fs.default.name", "local");
        
        // Mapper classes & Input files
		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, OuterMapper.class);
		MultipleInputs.addInputPath(conf, new Path(args[2]), TextInputFormat.class, InnerMapper.class);
		
        // Output path
        FileOutputFormat.setOutputPath(conf, new Path(args[4]));

        // Mapper output class
        conf.setMapOutputKeyClass(TextPair.class);
		conf.setMapOutputValueClass(TextPair.class);
        
        // Mapper Value Grouping
		conf.setOutputValueGroupingComparator(TextPair.FirstComparator.class);

        // Partitioner
        conf.setPartitionerClass(KeyPartitioner.class);
        
        // Reducer
        conf.setReducerClass(JoinReducer.class);

        // Reducer output
        conf.setOutputKeyClass(Text.class);
        
        // Set Join columns
        conf.setInt("OuterJoinColumn", Integer.parseInt(args[1]));
        conf.setInt("InnerJoinColumn", Integer.parseInt(args[3]));

        // Run job
		JobClient.runJob(conf);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new Join(), args);
	    System.exit(res);
	 }
}	