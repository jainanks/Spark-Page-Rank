import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * @author ankur
 * date: 04/26/2015
 *
 */

public class Sparkpart1 {

	public static void main(String[] args) throws IOException {
		SparkConf conf= new SparkConf().setAppName("sp");
		JavaSparkContext spark= new JavaSparkContext(conf);
		JavaRDD<String> file = spark.textFile("s3n://s15-p42-part1-easy/data/");
		
		JavaRDD<String> titles = file.flatMap(new FlatMapFunction<String, String>() { 
			
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String s) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(s.split("\t")[1]);
			}
	
		});
		
		final Long total= titles.distinct().count();
				
		JavaRDD<String> combined= file.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String s) throws Exception {
				String title= s.split("\t")[1];
				String[] words=s.split("\t")[3].replaceAll("\\<[^>]*>"," ").replace("\\n", " ").replaceAll("[^a-zA-Z]+", " ").toLowerCase().trim().split(" ");
				
				String[] arr= new String[words.length];
				for(int i=0;i<words.length;i++) {
					arr[i]= words[i]+","+title;
					//System.out.println(arr[i]);
				}
				
				return Arrays.asList(arr);
			}
		});
		
		JavaPairRDD<String, Integer> pairs = combined.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String s) { 
				return new Tuple2<String, Integer>(s, 1); 
			}
		});
		
		JavaPairRDD<String, Integer> wordTitleCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			 
			private static final long serialVersionUID = 1L;

			public Integer call(Integer a, Integer b) { return a + b; }

		});
		
		JavaPairRDD<String, Integer> check= wordTitleCount.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> s)
					throws Exception {
				String st= s._1();
				String word= st.split(",")[0];
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairRDD<String, Integer> wordCount = check.reduceByKey(new Function2<Integer, Integer, Integer>() {
			 
			private static final long serialVersionUID = 1L;

			public Integer call(Integer a, Integer b) { return a + b; }

		});
		
		JavaPairRDD<String, String> check1= wordTitleCount.mapToPair(new PairFunction<Tuple2<String,Integer>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, Integer> s)
					throws Exception {
				String st= s._1();
				String word= st.split(",")[0];
				String title= st.split(",")[1]+","+s._2();
				//System.out.println(word+" "+title);
				return new Tuple2<String, String>(word, title);
			}
		});
		
		JavaPairRDD<String, Tuple2<Integer, String>> joint= wordCount.join(check1);
		
		JavaPairRDD<String, Double> results= joint.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, String>>, String, Double>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Double> call(
					Tuple2<String, Tuple2<Integer, String>> s)
					throws Exception {
				// TODO Auto-generated method stub
				String word= s._1();
				Tuple2<Integer, String> t= s._2();
				int count= t._1();
				String titleCount= t._2();
				String title= titleCount.split(",")[0];
				int titleSum= Integer.parseInt(titleCount.split(",")[1]);
				//System.out.println(total+" "+word+" "+count+" "+title+" "+titleSum);
				double idf= Math.log10(total/(double)count);
				double w= titleSum*idf;
				String key= word+","+title;
				//System.out.println(key+" "+w);
				return new Tuple2<String, Double>(key, w);
			}
		});
		
		
		JavaPairRDD<String, String> cloud= results.mapToPair(new PairFunction<Tuple2<String, Double>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, Double> s)
					throws Exception {
				// TODO Auto-generated method stub
				String word= s._1.split(",")[0];
				String title= s._1.split(",")[1];
				Double w= s._2;
				return new Tuple2<String, String>(word, title+","+w);
			}
		});
		
		List<String> pairing= cloud.lookup("cloud");		
		final Map<String, Double> map= new HashMap<String, Double>();
	
		for(String s: pairing) {
			String[] each=  s.split(",");
			map.put(each[0],Double.parseDouble(each[1]));
		}
		
		//SortedSet<Double> set= new TreeSet<Double>(map.keySet());
		List<String> list= new ArrayList<String>(map.keySet());
		Collections.sort(list, new Comparator<String>() {

			@Override
			public int compare(String d1, String d2) {
				// TODO Auto-generated method stub
				double d=map.get(d1)-map.get(d2);
				if((d)>0) {
					return 1;
				} else if(d<0) {
					return -1;
				} else {
					return d1.compareTo(d2);
				}
			}
		});
		FileWriter outfile= new FileWriter("tfidf");
		for(int k=list.size()-1; k>list.size()-101;k--) {
			System.out.println(list.get(k)+"\t"+map.get(list.get(k)));
			outfile.write(list.get(k)+"\t"+map.get(list.get(k))+"\n");
		}
		
		outfile.close();
		//cloud.saveAsTextFile("hdfs:///output");
		
		
		/*JavaPairRDD<Double, String> cloudSort= cloud.sortByKey();
		
		JavaPairRDD<String, Double> top= cloudSort.mapToPair(new PairFunction<Tuple2<Double, String>, String, Double>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Double> call(Tuple2<Double, String> s)
					throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Double>(s._2,s._1);
			}
		});*/
		
		//top.saveAsTextFile("hdfs:///tfidf");
	}
}

