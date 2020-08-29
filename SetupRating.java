import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class SetupRating {
    public static class SetupRatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input user,movie,rating
            String[] user_movie_rating = value.toString().trim().split(",");
            int userID = Integer.parseInt(user_movie_rating[0]);
            String movieID = user_movie_rating[1].trim();
            String rating = user_movie_rating[2];

            context.write(new IntWritable(userID), new Text(movieID + ":" + rating));
        }
    }

    public static class SetupRatingReducer extends Reducer<IntWritable, Text, Text, Text> {
        List<String> movieList = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //read movie watch history
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("movieList");
            Path pt = new Path(filePath);
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line = br.readLine();

            //user,movie,rating
            while (line != null && line.length() > 0) {
                String movie = line;
                movieList.add(movie);
                line = br.readLine();
            }
            br.close();
        }

        // reduce method
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double rateSum = 0d;
            int count = 0;
            Map<String, String> outputValueMap = new TreeMap<>(new Comparator<String>(){
                public int compare(String o1, String o2) {
                    return o1.compareTo(o2);
                }
            });
            while (values.iterator().hasNext()) {
                String movie_rating = values.iterator().next().toString();
                String movieID = movie_rating.split(":")[0].trim();
                String rating = movie_rating.split(":")[1];
                rateSum += Double.parseDouble(rating);
                count++;

                String newKey = key + "," + movieID;
                if (!outputValueMap.containsKey(newKey)) {
                    outputValueMap.put(newKey, rating);
                }
            }
            double avg = rateSum / count;
            for (String movie : movieList) {
                String newKey = key + "," + movie.trim();
                if (!outputValueMap.containsKey(newKey)) {
                    outputValueMap.put(newKey, String.valueOf(avg));
                }
            }

            for (Map.Entry<String, String> map : outputValueMap.entrySet()) {
                //key = user,movieId,rating value=""
                context.write(new Text(map.getKey() + "," + map.getValue()), new Text(""));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("movieList", args[2] + "/part-r-00000");

        Job job = Job.getInstance(conf);
        job.setMapperClass(SetupRatingMapper.class);
        job.setReducerClass(SetupRatingReducer.class);

        job.setJarByClass(SetupRating.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
