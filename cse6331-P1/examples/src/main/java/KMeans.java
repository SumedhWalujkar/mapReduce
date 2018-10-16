import java.io.*;
import java.util.*;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
//import org.apache.commons.math3.util.*;




class Point implements WritableComparable<Point> 
{
	 public double X;
	 public double Y;
	 
	 Point () {}
	 Point (Point p) 
	 {
		 X=p.X;
		 Y=p.Y;
	 }
	 Point (String s) {}
	 
	 Point (double x, double y)
		{
			X=x;
			Y=y;
		}

	 public void write ( DataOutput out ) throws IOException 
		{
			out. writeDouble (X);
			out. writeDouble (Y);
		 
		}
	 public void readFields ( DataInput in ) throws IOException 
		{
			X = in. readDouble ();
			Y = in. readDouble ();

		}
	 public String toString()
	 {
		 return String.valueOf(X)+" "+String.valueOf(Y);
	 }
	 public int compareTo(Point p) {
         Point thisValue = this;
         Point thatValue = p;
         if(thisValue.X==thatValue.X)
         {
         return (thisValue.Y < thatValue.Y ? -1 : (thisValue.Y==thatValue.Y ? 0 : 1));
         }
         else if(thisValue.X<thatValue.X)
         {
        	 return -1;
         }
         else
        	 return 1;
       }
	 
}


public class KMeans 
{
    static Vector<Point> centroids = new Vector<Point>(100);
    static Vector<String> entroids = new Vector<String>(100);
    //public static int count=0;
	
    public static class AvgMapper extends Mapper<Object,Text,Point,Point> 
    
	{
    @Override
    protected void setup(Context context) throws IOException, InterruptedException 
		{	
    		URI[] paths = context.getCacheFiles();
    		Configuration conf = context.getConfiguration();
    		FileSystem fs = FileSystem.get(conf);
    		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
    		String string1;
			while (( string1=reader.readLine()) != null)//
			{
				//count=count++;
			//String string1 =reader.readLine();
			String str[] = string1.split(",");//
			List<Double> coor = new ArrayList<Double>();			
			for (int k=0;k<str.length;k++)
				{	
					
					coor.add(Double.parseDouble(str[k]));
				}
			Point r =new Point(Double.parseDouble(str[0]),Double.parseDouble(str[1]));
			
			centroids.addElement(r);
			}
			;
		}
    @Override
    public void map ( Object key, Text value, Context context )
            throws IOException, InterruptedException 
    {
    	//System.out.println("Counnnnnn:    "+count);
    	//Scanner s = new Scanner(value.toString());//.useDelimiter(",");//
    	
    	String strr= value.toString();//
    	String[] sss =strr.split(",");//
    	Point p = new Point(Double.parseDouble(sss[0]),Double.parseDouble(sss[1]));
    	double min =Double.MAX_VALUE;
    	Point centroid_map=new Point();//
    	Double[] d= new Double[2];//
    	d[0]=p.X;//
    	d[1]=p.Y;//
    	/*int count=0;
    	for (Point temp:centroids)
    	{
    		
    		double centroid_x=temp.X;
    		double centroid_y=temp.Y;
    		double temp1=point_x-centroid_x;
    		double temp2=point_y-centroid_y;
    		double temp3=temp1*temp1;
    		double temp4=temp2*temp2;
    		double temp5=temp3+temp4;
    		double temp6=Math.sqrt(temp5);
    		double distance=Math.abs(temp6);
    		
    		if (distance<min)
    		{
    			min=count;
    		}
    			count++;
    	}*/
    	
    	for (Point temp: centroids)
    	{ 
    		Double[] e= new Double[2];//
    		e[0]=temp.X;//
    		e[1]=temp.Y;//
    		double result=(Math.sqrt(((e[0]-d[0])*(e[0]-d[0]))+((e[1]-d[1])*(e[1]-d[1]))));//
    		if(result<min)//
    		{
    			min=result;//
    			centroid_map=temp;//
    		}
    		
    	}
    	System.out.println(centroid_map.toString()+" "+p.toString());
    	context.write(new Point(centroid_map),new Point(p));
    	//s.close();
    }
	}

    public static class AvgReducer extends Reducer<Point,Point,Text,Object>
    {
    	  @Override
          public void reduce ( Point key, Iterable<Point> values, Context context )
                             throws IOException, InterruptedException
                             {
    		  						int count=0;
    		  						double sx=0;
    		  						double sy=0;
    		  						for(Point Point_reducer:values)
    		  						{	
    		  							count ++;
    		  							sx=sx+Point_reducer.X;
    		  							sy=sy+Point_reducer.Y;
    		  						}
    		  						sx=sx/(double)count;
    		  						sy=sy/(double)count;
    		  						Point final_one=new Point(sx,sy);
    		  						String Final_one=final_one.toString();	
    		  						context.write(new Text(Final_one),NullWritable.get());
    		  						
                             }
    
    }

    public static void main ( String[] args ) throws Exception 
    {
    	 Job job = Job.getInstance();
         job.setJobName("KMeans");
         job.setJarByClass(KMeans.class);
         job.setMapperClass(AvgMapper.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(NullWritable.class);
         job.setMapOutputKeyClass(Point.class);
         job.setMapOutputValueClass(Point.class);
         job.setReducerClass(AvgReducer.class);
         job.setInputFormatClass(TextInputFormat.class);
         job.setOutputFormatClass(TextOutputFormat.class);
         FileInputFormat.setInputPaths(job, new Path(args[0]));
         //MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,AvgMapper.class);
         //MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,DepartmentMapper.class);
        // job.addCacheFile(new URI(args[1]));
         FileOutputFormat.setOutputPath(job,new Path(args[2]));
         job.addCacheFile(new URI(args[1]));
         job.waitForCompletion(true);
    

    }
}