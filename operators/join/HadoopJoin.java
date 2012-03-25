package operators.join;

import java.io.IOException;
import java.util.List;

import operators.projection.ProjectionFilter;
import operators.selection.SelectionEntry;
import operators.selection.SelectionFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import relations.Relation;
import relations.Schema;

/** main interface for join operator */
public class HadoopJoin {

	public static final String PREFIX_JOIN_OUTER = "outer_";
	public static final String PREFIX_JOIN_INNER = "inner_";

	public static final String PARAM_OUTER_JOIN_COL = "outer_join_col";
	public static final String PARAM_INNER_JOIN_COL = "inner_join_col";
	
	/** maximum size of smaller relation for memory backed join */
	protected static long MemoryBackedThreshold = 20000000;
	
	public static JobConf join(Relation r, Relation s, Relation output,
			List<SelectionEntry<String>> rFilters,
			List<SelectionEntry<String>> sFilters,
			List<String> rProjection,
			List<String> sProjection,
			String joinKey,
			boolean forceReduceSideJoin) throws IOException{
		
		Configuration job_conf = new Configuration();

		FileSystem fs = FileSystem.get(job_conf);
		
		// Compute relations size
		long rSize = 0;
		FileStatus[] files = fs.listStatus(new Path(r.storageFileName));
		for (FileStatus file : files) {
			rSize += file.getLen();
		}
			
		long sSize = 0;
		files = fs.listStatus(new Path(s.storageFileName));
		for (FileStatus file : files) {
			sSize += file.getLen();
		}

		System.out.println("Relations Size: " + rSize + " " + sSize);
	
		// Swap to make sure r:smaller, s:larger
		if (rSize > sSize) {
			Relation tmpRel = r;r = s;	s = tmpRel;
			List<String> tmpProj = rProjection; rProjection = sProjection; sProjection = tmpProj;
			List<SelectionEntry<String>> tmpSel = rFilters; rFilters = sFilters; sFilters = tmpSel;
		}
		
		// Generate output's schema
		Schema projR = r.schema.projection(rProjection);
		Schema projS = s.schema.projection(sProjection);
		output.schema = projR.join(projS);
		System.out.println(output.name + ": " + 
				output.schema.getFields().toString());
		
		// Configuring
		job_conf = SelectionFilter.addSelectionsToJob(job_conf,
				PREFIX_JOIN_OUTER, rFilters, r.schema);

		job_conf = SelectionFilter.addSelectionsToJob(job_conf,
				PREFIX_JOIN_INNER, sFilters, s.schema);
		
		job_conf = ProjectionFilter.addProjectionsToJob(job_conf, 
				PREFIX_JOIN_OUTER, rProjection, r.schema);

		job_conf = ProjectionFilter.addProjectionsToJob(job_conf, 
				PREFIX_JOIN_INNER, sProjection, s.schema);
		
		// DEBUG
		//JobClient.runJob(MemoryBackedJoin.createJob(job_conf, 
		//r, s, joinKey, output));
		
		// join configuration
		if (forceReduceSideJoin || rSize > MemoryBackedThreshold) {
			System.out.println("ReduceSideJoin");
			return ReduceSideJoin.createJob(job_conf, r, s, joinKey, output);
		} else {
			System.out.println("MemoryBackedJoin");
			return MemoryBackedJoin.createJob(job_conf, r, s, joinKey, output);
		} 
	}

	public static long getMemoryBackedThreshold() {
		return MemoryBackedThreshold;
	}

	public static void setMemoryBackedThreshold(long memoryBackedThreshold) {
		MemoryBackedThreshold = memoryBackedThreshold;
	}
}
