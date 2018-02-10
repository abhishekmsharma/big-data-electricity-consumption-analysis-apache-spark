package poweranalyzer;
//import java.util.logging.Logger;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.data.category.DefaultCategoryDataset;

import scala.Tuple2;

public class DataLoader 
{
	static String dir_hadoop = "/home/user01/project/hadoop";
	static String csv_file = "";
	
	public static void plotLineChart(List list_1, List list_2, String chartname)
	{
		
		  //int y_axis [] = List.toArray(new Integer[0]);
		  //String x_axis [] = {"2005_1","2006_2","2005_2","2006_1","2005_3"};
		
		  DefaultCategoryDataset dataset = new DefaultCategoryDataset( );
		  
		  for (int i=0;i<list_1.size();i++)
		  {
			  int y = Integer.parseInt(String.valueOf(list_1.get(i)));
			  float x = Float.parseFloat(String.valueOf(list_2.get(i)));
			  dataset.addValue(x, "Cluster Cost", String.valueOf(y));
		  }
		  JFreeChart linechart = ChartFactory.createLineChart("Cost vs No. of clusters", "Number of clusters", "Cost", dataset);
		  
		  ChartPanel chartpanel = new ChartPanel(linechart);
		  chartpanel.setPreferredSize( new java.awt.Dimension( 560 , 367 ) );
		  
		  CategoryPlot catPlot = linechart.getCategoryPlot();

		  final ValueAxis rangeAxis = ((CategoryPlot) catPlot).getRangeAxis(); 
		  CategoryAxis domainAxis = ((CategoryPlot) catPlot).getDomainAxis(); 
		  domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_90);
		  
		  try
		  {
			  OutputStream out = new FileOutputStream(chartname);
			  ChartUtils.writeChartAsPNG(out, linechart,1000,1000);
			  System.out.println(chartname + " chart created");
		  }
		  catch (Exception e){}  
	}
	
	public static void main(String[] args) 
	{
		  dir_hadoop = "/home/user01/project/hadoop";
		  csv_file = args[0];
		 // String new_csv_file = "/home/user01/project/new_household_power_consumption_test.csv";
		  System.setProperty("hadoop.home.dir", dir_hadoop);
			
		  int option = 0;
		  Scanner reader = new Scanner(System.in);
		  do
		  {
			  System.out.println("\n------------------\nChoose an option\n");
			  System.out.println("1. Analyze cost of clusters (KMeans)");
			  System.out.println("2. Visualize specifc attribute trend");
			  System.out.println("3. View attribute grouping (KMeans)");
			  System.out.println("0. Exit");
			  System.out.print("Enter your choice: ");
			  option = reader.nextInt(); 
			  
			  if (option==1)
			  {
				  System.out.print("\nEnter value of k: ");
				  reader = new Scanner(System.in);  // Reading from System.in
				  int k = reader.nextInt();
				  System.out.print("Enter number of iterations: ");
				  int numIterations = reader.nextInt();
				  kmeans(k, numIterations);
			  }
			  else if (option==2)
			  {
				  System.out.println("\nSelect area to visualize electricity usage: ");
				  System.out.println("1. Kitchen");
				  System.out.println("2. Laundry");
				  System.out.println("3. Electric water heater and air conditioner");
				  reader = new Scanner(System.in);  // Reading from System.in
				  System.out.print("Enter your choice: ");
				  int n = reader.nextInt() + 5;
				  System.out.println("");
				  System.out.print("Enter '0' for monthly or '1' for hourly visualization: ");
				  int month_hour_choice = reader.nextInt();
				  System.out.println("");
				  if (month_hour_choice==0)
					  analyzechoice(n, 0, 1, "Month");
				  else if (month_hour_choice==1)
					  analyzechoice(n, 1, 0, "Hour");
			  }
			  else if (option==3)
			  {
				  System.out.println("\nSelect area to group electricity usage: ");
				  System.out.println("1. Kitchen");
				  System.out.println("2. Laundry");
				  System.out.println("3. Electric water heater and air conditioner");
				  reader = new Scanner(System.in);
				  System.out.print("Enter your choice: ");
				  int n = reader.nextInt() + 5;
				  System.out.println("");
				  System.out.print("Enter '0' for monthly grouping or '1' for hourly grouping: ");
				  int month_hour_choice = reader.nextInt();
				  System.out.println("");
				  System.out.print("Enter value of k: ");
				  int k = reader.nextInt();
				  System.out.print("Enter number of iterations: ");
				  int ni = reader.nextInt();
				  if (month_hour_choice==0)
					  clusterchoice(n, k, ni, 0, 1, "Month");
				  else if (month_hour_choice==1)
					  clusterchoice(n, k, ni, 1, 0, "Hour");
			  }
			  
		  }while(option!=0);
		  reader.close();
	}
	
	public static void clusterchoice(int choice, int numClusters, int numIterations, int c1, int c2, String s1)
	{
		SparkSession sparkSession = SparkSession
	    		  .builder()
	    		  .master("local")
	    		  .config("spark.sql.warehouse.dir", dir_hadoop+"/warehouse")
	    		  .appName("JavaALSExample")
	    		  .getOrCreate();
	      
	      Logger rootLogger = LogManager.getRootLogger();
	      rootLogger.setLevel(Level.WARN); 
	      JavaRDD<String> heater_ac_RDD = sparkSession
	    		  .read().textFile(csv_file)
	    		  .javaRDD().filter( str-> !(null==str))
	    		  .filter(str-> !(str.length()==0))
	    		  .filter(str-> !str.contains("Date"))
	    		  .filter(str->!str.contains("?"))
	    		  .filter(str->!(str.split(";").length<9))
	    		  .map(str -> HousePowerUtility.getChoiceAnalyzer(str, choice, c1, c2));
	      
	      HashMap<Integer, Float> hmap = new HashMap<Integer, Float>();
	      List<String> heater_ac_String = heater_ac_RDD.collect();
	      
	      for (String s: heater_ac_String)
	      {
	    	  String temp_s [] = s.split(",");
	    	if (hmap.containsKey(Integer.parseInt(temp_s[0])))
	    	{
	    		float a = hmap.get(Integer.parseInt(temp_s[0]));
	    		a = a + Float.parseFloat(temp_s[1]);
	    		hmap.put(Integer.parseInt(temp_s[0]), a);	    		
	    	}
	    	else
	    	{
	    		hmap.put(Integer.parseInt(temp_s[0]), Float.parseFloat(temp_s[1]));
	    	}
	      }
	      
	      String value_string = "";
	      	if (choice==6)
	      		value_string = "kitchen";
	      	else if (choice==7)
	      		value_string = "laundry";
	      	else if (choice==8)
	      		value_string = "water_heater_and_ac";
	      	
	      	Set set = hmap.entrySet();
		  	Iterator iterator = set.iterator();
		  	List<Object> objectList = new ArrayList<Object> ();
		  	while(iterator.hasNext()) 
			{
				 Map.Entry mentry = (Map.Entry)iterator.next();
				 objectList.add(String.valueOf(mentry.getValue()));
			}
		    sparkSession.close();
		    SparkConf conf = new SparkConf().setAppName("DataLoader").setMaster("local[*]");
		    JavaSparkContext jsc = new JavaSparkContext(conf);
		  	JavaRDD<Object> data = jsc.parallelize(objectList);
		  	JavaRDD<Vector> parsedData = data.map(s -> 
		    {
		      String[] sarray = String.valueOf(s).split(",");
		      double[] values = new double[sarray.length];
		      for (int i = 0; i < sarray.length; i++) 
		      {
		        values[i] = Double.parseDouble(sarray[i]);
		      }
		      return Vectors.dense(values);
		    });
		    parsedData.cache();
		    
		    KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
		    
		    JavaRDD<Integer> predictions = clusters.predict(parsedData);
			  List<Integer> predict_cluster = predictions.collect();
			  /*int[] cluster_index = predict_cluster.stream().mapToInt(i->i).toArray();
			  for (int x=0;x<cluster_index.length;x++)
			  {
				  System.out.println(x + "\t" + cluster_index[x]);
			  }*/
			  
			  int[] cluster_index = predict_cluster.stream().mapToInt(i->i).toArray();
			  int[] distinct_clusters = Arrays.stream(cluster_index).distinct().toArray();
			  for (int i=0;i<distinct_clusters.length;i++)
			  {
				  System.out.print("\n------------------\nGroup " + (i+1)+": ");
				  for (int x=0;x<cluster_index.length;x++)
				  {
					  if (cluster_index[x]==i)
					  {
						  if (s1.equals("Month"))
						  {
							  System.out.print((x+1) + ", ");
						  }
						  else
						  {
							  System.out.print(x + ", ");
						  }
					  }
				  }
			  }
		    
		    jsc.stop();
	}
	
	public static void analyzechoice(int choice, int c1, int c2, String s1)
	{
		SparkSession sparkSession = SparkSession
	    		  .builder()
	    		  .master("local")
	    		  .config("spark.sql.warehouse.dir", dir_hadoop+"/warehouse")
	    		  .appName("JavaALSExample")
	    		  .getOrCreate();
	      
	      Logger rootLogger = LogManager.getRootLogger();
	      rootLogger.setLevel(Level.WARN); 
		  
		  JavaRDD<String> heater_ac_RDD = sparkSession
	    		  .read().textFile(csv_file)
	    		  .javaRDD().filter( str-> !(null==str))
	    		  .filter(str-> !(str.length()==0))
	    		  .filter(str-> !str.contains("Date"))
	    		  .filter(str->!str.contains("?"))
	    		  .filter(str->!(str.split(";").length<9))
	    		  .map(str -> HousePowerUtility.getChoiceAnalyzer(str, choice, c1, c2));
	      
	      HashMap<Integer, Float> hmap = new HashMap<Integer, Float>();
	      
	      List<String> heater_ac_String = heater_ac_RDD.collect();
	      
	      for (String s: heater_ac_String)
	      {
	    	  String temp_s [] = s.split(",");
	    	if (hmap.containsKey(Integer.parseInt(temp_s[0])))
	    	{
	    		float a = hmap.get(Integer.parseInt(temp_s[0]));
	    		a = a + Float.parseFloat(temp_s[1]);
	    		hmap.put(Integer.parseInt(temp_s[0]), a);	    		
	    	}
	    	else
	    	{
	    		hmap.put(Integer.parseInt(temp_s[0]), Float.parseFloat(temp_s[1]));
	    	}
	      }
		  	
	      String value_string = "";
	      	if (choice==6)
	      		value_string = "kitchen";
	      	else if (choice==7)
	      		value_string = "laundry";
	      	else if (choice==8)
	      		value_string = "water_heater_and_ac";
	      	
		  	Set set = hmap.entrySet();
		  	Iterator iterator = set.iterator();
		  	DefaultCategoryDataset dataset = new DefaultCategoryDataset( );
			while(iterator.hasNext()) 
			{
				 Map.Entry mentry = (Map.Entry)iterator.next();
				 System.out.println(s1 + ": " + mentry.getKey() + " - Usage: " + mentry.getValue());
				 float x =Float.parseFloat(String.valueOf(mentry.getValue()))/1000.00f;
				 String y = String.valueOf(mentry.getKey());
				 dataset.addValue(x, value_string + " usage in watt-hour", y);
			}
			JFreeChart linechart = ChartFactory.createLineChart(s1 + " vs " + value_string + " usage", s1 + " number", value_string +  " usage in watt-hour", dataset);
			  
			  ChartPanel chartpanel = new ChartPanel(linechart);
			  chartpanel.setPreferredSize( new java.awt.Dimension( 560 , 367 ) );
			  
			  CategoryPlot catPlot = linechart.getCategoryPlot();

			  final ValueAxis rangeAxis = ((CategoryPlot) catPlot).getRangeAxis(); 
			  CategoryAxis domainAxis = ((CategoryPlot) catPlot).getDomainAxis(); 
			  domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_90);
			  
			  try
			  {
				  OutputStream out = new FileOutputStream(value_string+"_"+s1+".png");
				  ChartUtils.writeChartAsPNG(out, linechart,1000,1000);
				  System.out.println(value_string+"_"+s1+".png" + " created");
			  }
			  catch (Exception e){}  
	}
	
	public static void kmeans(int k, int numIterations)
	{
		SparkSession sparkSession = SparkSession
	    		  .builder()
	    		  .master("local")
	    		  .config("spark.sql.warehouse.dir", dir_hadoop+"/warehouse")
	    		  .appName("JavaALSExample")
	    		  .getOrCreate();
	      
	      Logger rootLogger = LogManager.getRootLogger();
	      rootLogger.setLevel(Level.WARN); 
	      
	      System.out.println("Reading " + csv_file +" file");
	      JavaRDD<HousePowerUtility> housePURDD = sparkSession
	    		  .read().textFile(csv_file)
	    		  .javaRDD().filter( str-> !(null==str))
	    		  .filter(str-> !(str.length()==0))
	    		  .filter(str-> !str.contains("Date"))
	    		  .filter(str->!str.contains("?"))
	    		  .filter(str->!(str.split(";").length<9))
	    		  .map(str -> HousePowerUtility.parseRecord(str));
	      
	      
	      
	    
	      //housePURDD.foreach(m -> System.out.println(m.getHousePCVector()));
	     /*
	      JavaRDD<Sensor> sensorRdd = lines.map(new SensorData()).cache();
	   // transform data into javaPairRdd
	      JavaPairRDD<Integer, Sensor> deviceRdd = sensorRdd.mapToPair(
			   new PairFunction<Sensor, Integer, Sensor>() {   
				   	public Tuple2<Integer, Sensor> call(Sensor sensor) throws Exception {
	           Tuple2<Integer, Sensor>  tuple = new Tuple2<Integer, Sensor>
	           				(Integer.parseInt(sensor.getsId().trim()), sensor);
	           return tuple;
	       }
	   });*/
		     JavaPairRDD<String, Tuple2<List<Float>,Integer>> houseweekRdd =  housePURDD.mapToPair(
					   new PairFunction<HousePowerUtility, String , Tuple2<List<Float>,Integer>>() 
					   {   /**
						 * 
						 */
						private static final long serialVersionUID = 1L;

					@Override
						   	public Tuple2<String, Tuple2<List<Float>,Integer>> call(HousePowerUtility housepu) throws Exception 
						   	{
						   		Tuple2<String, Tuple2<List<Float>,Integer>> tuple = new Tuple2<String, 
						   				Tuple2<List<Float>,Integer>> (housepu.getYear()+"_"+housepu.getWeek(), new Tuple2<List<Float>,Integer>(housepu.getHousePCVector(),1));
						   		return tuple;
						   	
				                     }});
	      
		    // houseweekRdd.foreach(mw -> System.out.println(mw)); 
	      
	     @SuppressWarnings("unchecked")
		JavaPairRDD<String, List<Float>> houseweekRddAvg =  housePURDD.mapToPair(
				   new PairFunction<HousePowerUtility, String , Tuple2<List<Float>,Integer>>() 
				   {   /**
					 * 
					 */
					private static final long serialVersionUID = 1L;

				@Override
					   	public Tuple2<String, Tuple2<List<Float>,Integer>> call(HousePowerUtility housepu) throws Exception 
					   	{
					   		Tuple2<String, Tuple2<List<Float>,Integer>> tuple = new Tuple2<String, Tuple2<List<Float>,Integer>> (housepu.getYear()+"_"+housepu.getWeek(), new Tuple2<List<Float>,Integer>(housepu.getHousePCVector(),1));
					   		return tuple;
					   	} 
				   }).reduceByKey(
			        		 new Function2<Tuple2<List<Float>,Integer>, Tuple2<List<Float>,Integer>, Tuple2<List<Float>,Integer>>() {
			                     private static final long serialVersionUID = 1L;
								@Override
								public Tuple2<List<Float>, Integer> call(Tuple2<List<Float>, Integer> t1,
										Tuple2<List<Float>, Integer> t2) throws Exception {
			                    	 int length_list = t1._1().size();
			                    	 int length_list2 =t2._1().size();
			                    	 assert length_list == length_list2;
			                    	 
			                    	 List<Float> sumArray = new ArrayList<Float>();
			                    	 int count =0;
			                        
			                    	 for(int i = 0; i < length_list; i++){
			                    	     sumArray.add(t1._1().get(i) + t2._1().get(i));
								}
			                    	 count = t1._2()+ t2._2();
									return new Tuple2<List<Float>,Integer>(sumArray,count);
			                     }}).mapValues(
			                    		 new Function<Tuple2<List<Float>,Integer>,List<Float>>() {
											private static final long serialVersionUID = 1L;

												@Override
												public List<Float> call(Tuple2<List<Float>, Integer> ts)
														throws Exception {
													List<Float> avgArray = new ArrayList<Float>();
													int length_sumlist = ts._1().size();
							                        assert length_sumlist>0;
							                    	 for(int i = 0; i < length_sumlist; i++){
							                    	     avgArray.add(ts._1().get(i)/(ts._2));
							                    	 	}
													return avgArray; 
												}});
	     
	     //houseweekRddAvg.foreach(mw -> System.out.println(mw));
	     JavaRDD<List<Float>> hwavgRDDValue = houseweekRddAvg.map(x -> x._2);
	     JavaRDD<String> hwavgRDDKey = houseweekRddAvg.map(x -> x._1);
	     //hwavgRDDValue.foreach(mv -> System.out.println(mv));
	     //hwavgRDDKey.foreach(mk -> System.out.println(mk));
	     
	     JavaRDD<Vector> hwavgRDDVector = hwavgRDDValue.map(s -> {
 	      
 	      double[] values = new double[s.size()];
 	      for (int i = 0; i < s.size(); i++) {
 	        values[i] = s.get(i);
 	      }
 	      return Vectors.dense(values);
     });
	     hwavgRDDVector.cache();
	      /*
	      JavaRDD<Vector> parsedData = housePURDD.map(s -> {
	    	      
	    	      double[] values = new double[s.getHousePCVector().size()];
	    	      for (int i = 0; i < s.getHousePCVector().size(); i++) {
	    	        values[i] = s.getHousePCVector().get(i);
	    	      }
	    	      return Vectors.dense(values);
	        });
	        parsedData.cache();
	        */
	    
	      /*Dataset<Row> csv_read = sparkSession.read().format("com.databricks.spark.csv")
		        		      .option("header", "true")
		        		      .option("inferSchema", "true")
		        		      .load(csv_file);*/
		       
		  //csv_read.printSchema();
		  //csv_read.show();
		  
		  /*System.out.println("Creating clusters\n");
		  KMeansModel clusters = KMeans.train(hwavgRDDVector.rdd(), numClusters, numIterations);
		  System.out.println("Cluster centers:");
		  for (Vector center: clusters.clusterCenters()) 
		  {
		      System.out.println(" " + center);
		  }
		  double cost = clusters.computeCost(hwavgRDDVector.rdd());
		  System.out.println("Cost: " + cost);
		  
		  JavaRDD<Integer> predictions = clusters.predict(hwavgRDDVector);
		  List<Integer> predict_cluster = predictions.collect();
		  
		  predictions.foreach(p -> System.out.println(p)); */ 
		  
		  List<String>  hsPC_time = hwavgRDDKey.collect();
		  List<Double> costArray = new ArrayList<Double>();
		  List<Integer> kmIndex = new ArrayList<Integer>();
		  int numClusters = k;
		  for(int ck=1;ck<=numClusters; ck++)
		  {
			  kmIndex.add(ck);
		  }
		  KMeansModel clusters_tmp = null;
		  for(int item : kmIndex)
		  {
			  numClusters = item;
			  clusters_tmp = KMeans.train(hwavgRDDVector.rdd(), numClusters, numIterations);
			  double cost_tmp= (double) clusters_tmp.computeCost(hwavgRDDVector.rdd());
			  costArray.add(cost_tmp); 
			  System.out.println("----------------------------------\nKMeans Cluster: k = " + numClusters);
			  System.out.println("Cost: " + cost_tmp);
			  //System.out.println("Running KMeans Clustering for k= "+numClusters + " - Cost = " + cost_tmp);
		  }
		  
		  //System.out.println(kmIndex);
		  //System.out.println(costArray);
		  System.out.println("----------------------------------");
		  System.out.println("Plotting chart");
		  plotLineChart(kmIndex, costArray, "kmeans_cost.png");
		  
		  /*JavaRDD<Integer> predictions = clusters_tmp.predict(hwavgRDDVector);
		  List<Integer> predict_cluster = predictions.collect();
		  //System.out.println(predict_cluster);
		  //System.out.println(hsPC_time);
		  
		  int[] cluster_index = predict_cluster.stream().mapToInt(i->i).toArray();
		  String[] cluster_values = new String[hsPC_time.size()];
		  cluster_values = hsPC_time.toArray(cluster_values);
		  
		  int[] distinct_clusters = Arrays.stream(cluster_index).distinct().toArray();
		  
		  String [] weekly_values = new String[cluster_values.length];
		  
		  for(int i=0;i<cluster_values.length;i++)
		  {
			  String [] temp = cluster_values[i].split("_");
			  weekly_values [i] = temp[1];
		  }
		  
		  System.out.println("---------------");
		  for(int i=0;i<weekly_values.length;i++)
		  {
			  System.out.println(weekly_values[i]+", "+String.valueOf(cluster_index[i]));
		  }*/
	}

}