package poweranalyzer;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.stream.Stream;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.text.DateFormatSymbols;

public class HousePowerUtility implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String dateH;
	private String timeH;
	private float gapcH, grpcH, voltH, gintyH, ktMeterH, ladMeterH, hacMeterH;
	private int weekH,yearH;
	
	
    public HousePowerUtility() {};
	public HousePowerUtility(int yearHs,int weekHs, String dateHs, String timeHs, float gapcHs,float grpcHs,
			float gintyHs, float voltHs, float ktMeterHs, float ladMeterHs, float hacMeterHs) 
	
	{
		super();
		this.dateH = dateHs;
		this.timeH = timeHs;
		this.gapcH = gapcHs;
		this.grpcH = grpcHs;
		this.gintyH = gintyHs;
		this.voltH = voltHs;
		this.ktMeterH = ktMeterHs;
		this.ladMeterH = ladMeterHs;
		this.hacMeterH = hacMeterHs;
		this.weekH = weekHs;
		this.yearH = yearHs;
	
	}

	public int getWeek() {
		return weekH;
	}
	public void setWeek(int weekE ) {
		this.weekH= weekE;
	}
	
	public int getYear() {
		return yearH;
	}
	public void setYear(int yearE ) {
		this.yearH= yearE;
	}
	
	
	public String getDate() {
		return dateH;
	}
	public void setDate(String dateE ) {
		this.dateH= dateE;
	}
	
	public String getTime() {
		return timeH;
	}
	public void setTime(String timeE ) {
		this.timeH= timeE;
	}
	public float getgapc() {
		return gapcH;
	}
	
	public void setgapc(float  gapcE ) {
		this.gapcH= gapcE;
	}
	public void setginty(float  gintyE ) {
		this.gintyH= gintyE;
	}
	
	public float getgrpc() {
		return gapcH;
	}
	public void setgrpc(float  gapcE ) {
		this.gapcH= gapcE;
	}
	
	public float getvolt() {
		return voltH;
	}
	public void setvolt(float  voltH) {
		this.voltH= voltH;
	}
	public float getktMeter() {
		return ktMeterH;
	}
	public void setktMeter(float  ktMeterE) {
		this.ktMeterH= ktMeterE;
	}
	public float getladMeter() {
		return ladMeterH;
	}
	public void setladMeter(float  ladMeterE) {
		this.ladMeterH= ladMeterE;
	}
	public float gethacMeter() {
		return hacMeterH;
	}
	public String getRowRecord() {
		return new StringBuilder().append(this.gapcH).append(this.grpcH).append(this.gintyH)
				.append(this.voltH).append(this.ktMeterH).append(this.ladMeterH).toString();
	}
	
	public void setRowRecord(float  hacMeterE) {
		this.hacMeterH= hacMeterE;
	}
	
	public List<Float> getHousePCVector()
	{
	    List<Float> numbers = new ArrayList<Float>();
	    numbers.add(this.gapcH);
	    numbers.add(this.grpcH);
	    numbers.add(this.gintyH);
	    numbers.add(this.voltH);
	    numbers.add(this.ktMeterH);
	    numbers.add(this.ladMeterH);
	    numbers.add(this.hacMeterH);
	    return(numbers); 
	}
	
	public static String getChoiceAnalyzer(String str, int choice, int c, int i)
	{
		String[] fields = str.split(";");
	      if (fields.length != 9) 
	      {
	    	  System.out.println("The elements are ::" ); 
	    	  Stream.of(fields).forEach(System.out::println);
	    	  	throw new IllegalArgumentException
	    	  	("Each line must contain 9 fields while the current line has ::"+fields.length);
	      }
	      String [] full_date = fields[c].trim().split("/");
	      if (c==1)
	      {
	    	  full_date = fields[c].trim().split(":");
	      }
	      int selected_value = Integer.parseInt(full_date[i]);
	      float attr_value = Float.parseFloat(fields[choice].trim());
	      
	      String to_send = selected_value + "," + attr_value;
	      
	      return to_send;
	      
	}
	
	public static HousePowerUtility parseRecord(String str) 
	{
		  String[] fields = str.split(";");
	      if (fields.length != 9) 
	      {
	    	  System.out.println("The elements are ::" ); 
	    	  Stream.of(fields).forEach(System.out::println);
	    	  	throw new IllegalArgumentException
	    	  	("Each line must contain 9 fields while the current line has ::"+fields.length);
	      }
	      int weekR = 0;
	      int yearR =1900;
	      String dateR = fields[0].trim();
	      String format = "d/M/yy";
  		  SimpleDateFormat df = new SimpleDateFormat(format);
	      try {
				Date dateRParse = df.parse(fields[0].trim());
				Calendar cal = Calendar.getInstance();
				cal.setTime(dateRParse);
				weekR = cal.get(Calendar.WEEK_OF_YEAR);
				yearR = cal.get(Calendar.YEAR);
			} catch (ParseException e) {
				e.printStackTrace();}
	      String timeR = fields[1].trim();
	      float gapcR = Float.parseFloat(fields[2].trim());
	      float grpcR = Float.parseFloat(fields[3].trim());
	      float gintyR = Float.parseFloat(fields[4].trim());
	      float voltR = Float.parseFloat(fields[5].trim());
	      float ktMeterR = Float.parseFloat(fields[6].trim());
	      float ladMeterR = Float.parseFloat(fields[7].trim());
	      float hacMeterR = Float.parseFloat(fields[8].trim());
		return new HousePowerUtility(yearR, weekR, dateR,timeR, gapcR, grpcR, gintyR, voltR, ktMeterR,
	    		  ladMeterR, hacMeterR);
	}
}
	