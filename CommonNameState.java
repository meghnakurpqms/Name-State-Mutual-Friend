import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CommonNameState {
	// Map class
	public static class Maps extends Mapper<LongWritable, Text, Text, Text> 
	{
		static HashMap<Integer, String> map =new HashMap<Integer, String>();
		// setup method
		protected void setup(Context con)throws IOException, InterruptedException
		{
			
			super.setup(con);
			Configuration conf = con.getConfiguration();
			Path part = new Path(con.getConfiguration().get("Data"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss =fs.listStatus(part);
			for(FileStatus status : fss)
			{
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String record;
				record = br.readLine();
				while(record != null)
				{
					String[] data = record.split(",");
					if(data.length == 10)
					{
						map.put(Integer.parseInt(data[0]), data[1] + ":" + data[5]);
						//map.put(Integer.parseInt(data[0]), data[1] + ":" + data[5]);
					}
					record = br.readLine();
				}
			}
			
		}
		// Map class
		public void map(LongWritable key, Text val, Context con)throws IOException, InterruptedException
		{
			Configuration conf = con.getConfiguration();
			int inp_frnd1 = Integer.parseInt(conf.get("Input First Friend"));
			int inp_frnd2 = Integer.parseInt(conf.get("Input Second Friend"));
			String[] friend = val.toString().split("\t");
			if(friend.length == 2)
			{
				int frnd1ID = Integer.parseInt(friend[0]);
				String[] friends = friend[1].split(",");
				int frnd2ID;
				StringBuilder str;
				Text fr_key = new Text();
				for(String secondfriend : friends)
				{
					frnd2ID = Integer.parseInt(secondfriend);
					// If friends and input id match
					if((frnd1ID == inp_frnd1 && frnd2ID == inp_frnd2) || (frnd1ID == inp_frnd2 && frnd2ID == inp_frnd1))
					{
						// if id of first is less than the second print that first
						
						// string builder to remove from a string
						str = new StringBuilder();
						if(frnd1ID <frnd2ID)
						{
							fr_key.set(frnd1ID + "," + frnd2ID);
						}
						// if id of second is less than the first print that first
						
						else
						{
							fr_key.set(frnd2ID + "," + frnd1ID);
						}
						// for every info in all the friends
						for(String frndData : friends)
						{
							int frndData2 = Integer.parseInt(frndData);
							str.append(frndData2 + ":" + map.get(frndData2) + ",");
						}
						//// remove the last comma
						if(str.length() > 0)
							str.deleteCharAt(str.length() - 1);
						con.write(fr_key,new Text(str.toString()) );
					}//if
					
				}//for
			}// outer if
		}//void
		
	}//map

//class
// reducer class
public static class Reduce extends Reducer<Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterable<Text> val, Context context) throws IOException, InterruptedException 
	{
		HashMap<String, Integer> frndsHashMap = new HashMap<String, Integer>();
		StringBuilder mutualFriendrecord = new StringBuilder();
		Text mutualFriend = new Text();
		// for every tuple in the values
		for (Text set : val)
		{
			// split on comma
			String[] friendsList = set.toString().split(",");
			// foreach friend in the friendlist
			for (String perFriend : friendsList) {
				if (frndsHashMap.containsKey(perFriend)) {
					// split on :
					String[] info = perFriend.split(":");
					// append to mutual friend
					mutualFriendrecord.append(info[1]+":"+info[2]+ ",");
				} else {
					// else not in hashmap add it with a vlaue 1
					frndsHashMap.put(perFriend, 1);
				}
			}
		}
		if (mutualFriendrecord.length() > 0)
		{
			mutualFriendrecord.deleteCharAt(mutualFriendrecord.length() - 1);
		}
		mutualFriend.set(new Text(mutualFriendrecord.toString()));
		context.write(key, mutualFriend);
		}
	}
// Main class to set up configuration and jobs to call  the mapper and reducer class

public static void main(String[] args) throws Exception {
	Configuration conf1 = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
	//  Number of arguments has to be 5
	if (otherArgs.length != 5) {
		System.out.println(
				"Usage: <Common friends input file path> <user_data file path> <output_path> <User-ID1> <User-ID2> ");
		System.exit(1);
	}

	conf1.set("Input First Friend", otherArgs[3]);
	conf1.set("Input Second Friend", otherArgs[4]);
	conf1.set("Data", otherArgs[1]);

	Job job1 = Job.getInstance(conf1, "Mutual-Friends of userA and userB");

	job1.setJarByClass(CommonNameState.class);
	// Call map
	job1.setMapperClass(Maps.class);
	//Call reducer
	job1.setReducerClass(Reduce.class);

	job1.setMapOutputKeyClass(Text.class);
	job1.setMapOutputValueClass(Text.class);

	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

	if (!job1.waitForCompletion(true)) {
		System.exit(1);
	}
}

}


