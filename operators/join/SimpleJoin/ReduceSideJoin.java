package operators.join.SimpleJoin;

import java.io.IOException;
import java.util.*;

import operators.join.TextPair;
import operators.selection.DateSelectionFilter;
import operators.selection.SelectionFilter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import relations.Relation;

public class ReduceSideJoin {

  /**
   * Outer is the smaller relation, i.e. it's values per key are copied into mem
   * in the reduce phase
   * 
   */
  public static final String PREFIX_JOIN_SMALLER = "smaller_";
  public static final String PREFIX_JOIN_LARGER = "larger_";

  public static final String PARAM_LARGER_NAME = "larger_name";
  public static final String PARAM_SMALLER_NAME = "smaller_name";

  protected static final String COLUMN_SEPARATOR_RE = "\\|";
  protected static final String COLUMN_SEPARATOR = "|";

  private static final boolean IS_LOCAL = false;
  private static final boolean DEBUG = true;

  public static class SmallerRelationMapper extends ReduceSideJoinAbstractMapper {
    public void configure(JobConf conf) {
      relationName = conf.get(PARAM_SMALLER_NAME, "");
      reduceOrder = "0";
      joinCol = conf.getInt("OuterJoinColumn", 0);

      selectionFilter = new SelectionFilter(conf, PREFIX_JOIN_SMALLER);
      configureDateSelection(conf);
    };

  }

  public static class LargerRelationMapper extends ReduceSideJoinAbstractMapper {
    public void configure(JobConf conf) {

      relationName = conf.get(PARAM_LARGER_NAME, "");
      reduceOrder = "1";
      joinCol = conf.getInt("InnerJoinColumn", 0);

      selectionFilter = new SelectionFilter(conf, PREFIX_JOIN_LARGER);
      configureDateSelection(conf);
    }

  }

  public static class ReduceSideJoinAbstractMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, TextPair, TextPair> {
    // TODO: static fields force NO Parallel jobs on same
    // machine!!! Is this really really always the
    // case. In the new API it seems we could use Context object for this... but
    // is this performant?

    // Overridden by child class
    protected String reduceOrder;
    protected int joinCol;

    // TODO: add projection
    protected SelectionFilter selectionFilter;
    protected DateSelectionFilter dateSelectionFilter = null;

    protected String relationName = "";

    protected void configureDateSelection(Configuration conf) {
      dateSelectionFilter = new DateSelectionFilter(conf, relationName);
    }

    public void map(LongWritable key, Text value, OutputCollector<TextPair, TextPair> output,
        Reporter reporter) throws IOException {
      String[] tuple = value.toString().split(COLUMN_SEPARATOR_RE);

      // System.out.println("map in: " + value);

      // filter the rows out that don't pass the selection
      if (!selectionFilter.checkSelection(tuple))
        return;

      if (dateSelectionFilter != null && !dateSelectionFilter.checkSelection(tuple))
        return;

      // if (DEBUG) System.out.println("sel ok: " + value);

      StringBuffer attrs = new StringBuffer();
      for (int i = 0; i < tuple.length; i++) {
        // TODO: for this is simplier and in general it's ok to leave it
        // like this.
        // if (i != joinCol) {
        attrs.append(tuple[i] + COLUMN_SEPARATOR);
        // }
      }

      if (attrs.length() > 0) {
        attrs.deleteCharAt(attrs.length() - 1);
      }

      // try {
      output.collect(new TextPair(tuple[joinCol], reduceOrder), new TextPair(attrs.toString(),
          reduceOrder));
      /*
       * } catch (ArrayIndexOutOfBoundsException e) { // TODO: handle exception
       * e.printStackTrace(); System.out.println("Error at rel=" + relationName
       * + " joinCol=" + joinCol + " for tuple=" + Arrays.toString(tuple)); }
       */
    }
  }

  public static class KeyPartitioner implements Partitioner<TextPair, TextPair> {

    @Override
    public int getPartition(TextPair key, TextPair value, int numPartitions) {
      return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

    @Override
    public void configure(JobConf arg0) {
      // TODO Auto-generated method stub

    }

  }

  /***
   * TODO: behaviour of this may be a bit undefined!!!
   * 
   * @author vidma
   * 
   */
  public static class JoinReducer extends MapReduceBase implements
      Reducer<TextPair, TextPair, NullWritable, Text> {

    private static final Text smallerRelmarker = new Text("0");

    public void reduce(TextPair key, Iterator<TextPair> values,
        OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {

      ArrayList<String> smaller_relation_tuples = new ArrayList<String>();

      TextPair value = null;
      String result_tuple = null;
      String larger_rel_tuple = null;

      while (values.hasNext()) {
        value = values.next();

        if ((value.getSecond().compareTo(smallerRelmarker) == 0)) {

          // add the whole tuple of smaller relation
          smaller_relation_tuples.add(value.getFirst().toString());
        } else {

          // We do INNER join, so if smaller relation not present, we can quit
          // immediately!
          // TODO: check if somewhere we need non inner joins, as it's valid for
          // aggregation to show zero values. Probably we add another
          // param=joinType [inner, left, right, outer] --- preserve empty
          // columns

          if (smaller_relation_tuples.size() == 0) {
            return;
          }

          larger_rel_tuple = value.getFirst().toString();
          // System.out.println("match (k=" + key.getFirst() + ") smaller: " +
          // smaller_relation_tuples + "larger: " + larger_rel_tuple);

          for (String smaller_rel_tuple : smaller_relation_tuples) {

            result_tuple = smaller_rel_tuple + COLUMN_SEPARATOR + larger_rel_tuple;

            // Instead of key.getFirst() we output null for a key, so only the
            // tuple gets written into intermediate file
            output.collect(null, new Text(result_tuple));
          }
        }
      }
    }
  }

  /**
   * Convience method to made definitions shorter
   * 
   * TODO: shall I change string parameters into Path? What about hdfs paths?
   * 
   * TODO: change params inner/outer into smaller/larger -- so it would be more
   * obvious
   * 
   * @param
   * @return
   * @throws IOException
   */
  public static JobConf createJob(Configuration conf_, Relation larger, String largerJoinCol,
      Relation smaller, String smallerJoinCol, Relation outRelation) throws IOException {

    if (IS_LOCAL) {
      conf_.set("mapred.job.tracker", "local");
      conf_.set("fs.default.name", "file:///");
    }

    // Set ReduceSideJoin columns
    conf_.setInt("OuterJoinColumn", smaller.schema.getColumnIndex(smallerJoinCol));
    conf_.setInt("InnerJoinColumn", larger.schema.getColumnIndex(largerJoinCol));

    conf_.set(PARAM_SMALLER_NAME, smaller.name);
    conf_.set(PARAM_LARGER_NAME, larger.name);

    JobConf conf = new JobConf(conf_);

    obtainResultSchema(larger, smaller, outRelation);

    conf.setJarByClass(ReduceSideJoin.class);

    // Mapper classes & Input files
    // TODO: this is a hack to disregard the outputted keys
    MultipleInputs.addInputPath(conf, new Path(smaller.storageFileName), TextInputFormat.class,
        SmallerRelationMapper.class);
    MultipleInputs.addInputPath(conf, new Path(larger.storageFileName), TextInputFormat.class,
        LargerRelationMapper.class);

    // Output path
    FileOutputFormat.setOutputPath(conf, new Path(outRelation.storageFileName));

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
    // TODO: SequenceFileInputFormat may be more efficient way of storing
    // intermediate data!

    // TODO: remove the key, so output is consistent with input. this do not
    // work!!!
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Text.class);

    // TODO: probably we'll use NullOutputFormat for the final result

    return conf;
  }

  /**
   * Convience method to made definitions shorter: Natural join
   * 
   * also initializes the resulting relation schema!
   * 
   * more obvious
   * 
   * @param
   * @return
   * @throws IOException
   */
  public static JobConf createJob(Configuration conf, Relation larger, Relation smaller,
      String naturalJoinCol, Relation outRelation) throws IOException {
    return createJob(conf, larger, naturalJoinCol, smaller, naturalJoinCol, outRelation);
  }

  /**
   * @param larger
   * @param smaller
   * @param outRelation
   */
  private static void obtainResultSchema(Relation larger, Relation smaller, Relation outRelation) {
    outRelation.schema = smaller.schema.join(larger.schema);
  }

}
