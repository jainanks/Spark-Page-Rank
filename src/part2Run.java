import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * @author aayush
 * date: 04/26/2015
 *
 */
public class part2Run {

	  private static class Sum implements Function2<Double, Double, Double> {
	   
		private static final long serialVersionUID = 1L;

		@Override
	    public Double call(Double a, Double b) {
	      return a + b;
	    }
	  }

	  public static void main(String[] args) throws Exception {
		  
	    SparkConf sparkConf = new SparkConf().setAppName("JavaPageRank");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

	    JavaRDD<String> arcs = ctx.textFile("s3n://s15-p42-part2/wikipedia_arcs");
	    JavaRDD<String> mapping = ctx.textFile("s3n://s15-p42-part2/wikipedia_mapping");
	    
	    // Loads all URLs from input file and initialize their neighbors.	    
	    JavaPairRDD<String, Iterable<String>> links = arcs.mapToPair(new PairFunction<String, String, String>() {
	
			private static final long serialVersionUID = 1L;

		@Override
	      public Tuple2<String, String> call(String s) {
			//System.out.println("s: "+s);
	        String[] parts = s.split("\t");
	        return new Tuple2<String, String>(parts[0], parts[1]);
	      }
	    }).distinct().groupByKey().cache();

	   //final Long total= links.distinct().count();
	   // System.out.println("total----------------------------------: "+total);
	    
	    JavaPairRDD<String, String> maps = mapping.mapToPair(new PairFunction<String, String, String>() {
	    	
			private static final long serialVersionUID = 1L;

		@Override
	      public Tuple2<String, String> call(String s) {
			//System.out.println("maps: "+s);
	        String[] parts = s.split("\t");
	        return new Tuple2<String, String>(parts[0], parts[1]);
	      }
	    });
	    
	    final Long total= maps.distinct().count();
	    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
	    JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
	     
			private static final long serialVersionUID = 1L;

		@Override
	      public Double call(Iterable<String> rs) {
	        return 1.0;
	      }
	    });

	    JavaPairRDD<String, String> joint=  maps.subtractByKey(ranks);
	    
	    
	    // Calculates and updates URL ranks continuously using PageRank algorithm.
	    for (int current = 0; current < 10; current++) {
	      // Calculates URL contributions to the rank of other URLs.
	    	
	      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
	        .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
	          private static final long serialVersionUID = 1L;

			@Override
	          public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
	            int urlCount = 0;
	            for (String n : s._1) {
	            	urlCount++;
	            }
	            //System.out.println("urlcount:  "+urlCount);
	            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
	            for (String n : s._1) {
	              results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
	              //System.out.println("val "+n+" "+(s._2() / urlCount));
	            }
	            return results;
	          }
	      });
	      
	     	      
	      // Re-calculates URL ranks based on neighbor contributions.
	      JavaPairRDD<String, Double> intermediate = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
	        private static final long serialVersionUID = 1L;

			@Override
	        public Double call(Double sum) {
	          return 0.15 + sum * 0.85;
	        }
	      });
	      
	      //Long total= contribs.distinct().count();
	      //System.out.println("total----------------------------------: "+total);
	      
	      JavaPairRDD<String, Tuple2<String,Double>> check= joint.join(intermediate);
	      
	      JavaDoubleRDD adding= check.mapToDouble(new DoubleFunction<Tuple2<String,Tuple2<String,Double>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public double call(Tuple2<String, Tuple2<String, Double>> s)
					throws Exception {
				
				Tuple2<String, Double> t= s._2;
				double d= t._2;
				//System.out.println("dddddddd::::::::::   "+s._1+" "+d);
				return d;
			}
		});
	      
	      Double all= adding.sum();
	      //System.out.println("alll::::::::::::::::::::::; --------------- "+all);
	      final Double divided= all/total;
	      
	      
			//final double danglevalue= 1;//sum.get(0)/total;
			
			ranks= intermediate.mapToPair(new PairFunction<Tuple2<String, Double>, String, Double>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Double> call(Tuple2<String, Double> s)
						throws Exception {
					// TODO Auto-generated method stub
					String node= s._1;
					Double value= s._2;
					value= value+divided;
					//System.out.println("ranks: :: "+node+" "+value);
					return new Tuple2<String, Double>(node, value);
				}
			});
	      
	    }

	    // Collects all URL ranks and dump them to console.
	    JavaPairRDD<String, Tuple2<Double, String>> combine= ranks.join(maps);
	    
	        
	    JavaPairRDD<Double, String> result= combine.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double,String>>, Double, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Double, String> call(
					Tuple2<String, Tuple2<Double, String>> s)
					throws Exception {
				// TODO Auto-generated method stub
				Tuple2<Double, String> t= s._2;
				Double val= t._1;
				String name= t._2;
				return new Tuple2<Double, String>(val, name);
			}

		});
	    
	    JavaPairRDD<Double, String> out= result.sortByKey(false);
	    
	    List<Tuple2<Double, String>> output= out.take(100);
	    
	    
	    FileWriter file= new FileWriter("pagerank");
	    for (int i=0; i<output.size();i++) {
	    	Tuple2<Double, String> tup= output.get(i);
	        System.out.println(tup._2() + "\t" + tup._1());
	        file.write(tup._2() + "\t" + tup._1());
	    }
	    
	    file.close();
	    ctx.stop();
	  }

}
