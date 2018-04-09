package com.hof.imp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import org.json.*;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.Analytics.Data.Ga.Get;
import com.google.api.services.analytics.AnalyticsScopes;

import com.google.api.services.analytics.model.Column;
import com.google.api.services.analytics.model.GaData;
import com.google.api.services.analytics.model.GaData.ColumnHeaders;
import com.google.api.services.analytics.model.Profile;
import com.google.api.services.analytics.model.Profiles;
import com.google.api.services.analytics.model.Goals;
import com.hof.data.SessionBean;
import com.hof.jdbc.metadata.GoogleAnalyticsMetaData;
import com.hof.mi.thirdparty.interfaces.AbstractDataSet;
import com.hof.mi.thirdparty.interfaces.AbstractDataSource;
import com.hof.mi.thirdparty.interfaces.ColumnMetaData;
import com.hof.mi.thirdparty.interfaces.DataType;
import com.hof.mi.thirdparty.interfaces.FilterData;
import com.hof.mi.thirdparty.interfaces.FilterMetaData;
import com.hof.mi.thirdparty.interfaces.FilterOperator;
import com.hof.mi.thirdparty.interfaces.ScheduleDefinition;
import com.hof.mi.thirdparty.interfaces.ScheduleDefinition.FrequencyTypeCode;
import com.hof.mi.thirdparty.interfaces.ThirdPartyException;
import com.hof.pool.JDBCMetaData;
import com.hof.util.Const;
import com.hof.util.GoogleAnalyticsDataZoom;
import com.hof.util.OrgCache;
import com.hof.util.UtilString;
import com.hof.util.i4RequestRegistry;

public class GoogleAnalytics extends AbstractDataSource {

	private static GoogleCredential credential;
	private static Analytics analytics;
	private int rowsLimit=10000;
	
	private static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static JsonFactory JSON_FACTORY = new JacksonFactory();
	
	private JDBCMetaData sourceMetadata = null;
	private final static Logger log = Logger.getLogger(GoogleAnalytics.class);

	protected final static String UI_NAME_KEY = "uiName";
	protected final static String STATUS_KEY = "status";
	protected final static String DATA_TYPE_KEY = "dataType";
	protected final static String TYPE_KEY = "type";
	protected final static String ID_KEY = "id";
	protected final static String PROFILE_ID_KEY = "PROFILEID";
	protected final static String ALL_COLUMNS_METADATA_KEY = "ALLCOLUMNSMETADATA";

	public GoogleAnalytics() {
		
	}
	
	public ScheduleDefinition getScheduleDefinition() { 
		return new ScheduleDefinition(FrequencyTypeCode.DAILY, null, null); 
	};
	
	@Override
	public String getDataSourceName() {
		return GoogleAnalyticsDataZoom.getText("Google Analytics API", "mi.text.ga.datasource.name");
	}

	@Override
	public Collection<AbstractDataSet> getDataSets() {
		
		List<AbstractDataSet> dataSets = new ArrayList<AbstractDataSet>();
		
		dataSets.add(AllColumns());
		
		return dataSets;
	}
	
	public AbstractDataSet AllColumns()
	{
		AbstractDataSet dtSet=new AbstractDataSet() {
			
			@Override
			public List<FilterMetaData> getFilters() {
				
				boolean isETLContext = false;
				
				try {

					isETLContext = isTransformationContext();

				} catch (NoSuchMethodError e) {


				}
				List<String> fieldsAllowed = new ArrayList<String>();
				
				if (isETLContext)
				{
					fieldsAllowed = fieldsAllowed();
				}
				
				// TODO Auto-generated method stub
				ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
				
				fm.add(new FilterMetaData("Start Date", DataType.DATE, true, new FilterOperator[] {FilterOperator.EQUAL}));
				fm.add(new FilterMetaData("End Date", DataType.DATE, true, new FilterOperator[] {FilterOperator.EQUAL}));
				
				
					
					
					//ArrayList<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
					ArrayList<Column> cols=getAllColumnsMetadata();
					for (Column col:cols)
					{
						String YellowfinName=col.getAttributes().get("uiName");
						if (YellowfinName.equals("Users"))
						{
							if (col.getId().equals("ga:cohortActiveUsers"))
							{
								YellowfinName="Cohort Active Users";
							}
							else if (col.getId().equals("ga:cohortTotalUsersWithLifetimeCriteria"))
							{
								YellowfinName="Cohort Total Users With Lifetime Criteria";
							}
						}
						if (YellowfinName.equals("Social Network") && col.getId().equals("ga:socialInteractionNetwork"))
						{
							YellowfinName="Social Interaction Network";
						}
						DataType dt=DataType.TEXT;
						String status=col.getAttributes().get("status");
						if (status.equals("PUBLIC"))
						{
							if (YellowfinName.equals("Date"))
							{
								dt=DataType.DATE;
							}
							else if (YellowfinName.equals("Latitude") || YellowfinName.equals("Longitude"))
							{
								dt=DataType.NUMERIC;
							}
							else if (col.getAttributes().get("dataType").equals("STRING"))
							{
								dt=DataType.TEXT;
							}
							
							else if (col.getAttributes().get("dataType").equals("INTEGER"))
							{
								dt=DataType.INTEGER;
							}
							
							else if (col.getAttributes().get("dataType").equals("PERCENT") 
									|| col.getAttributes().get("dataType").equals("CURRENCY") 
									|| col.getAttributes().get("dataType").equals("FLOAT")
									|| col.getAttributes().get("dataType").equals("TIME"))
							{
								dt=DataType.NUMERIC;
							}
							
							else if (col.getAttributes().get("dataType").equals("DATE"))
							{
								dt=DataType.DATE;
							}
							
							boolean addFilter =false;
							if ((isETLContext && fieldsAllowed.contains(YellowfinName)) || !isETLContext)
							{
								addFilter = true;
							}
							
							if (addFilter)
							{
								if (col.getAttributes().get("type").equals("METRIC"))
								{
									fm.add(new FilterMetaData(YellowfinName, dt, false, new FilterOperator[] {FilterOperator.EQUAL, FilterOperator.NOTEQUAL, FilterOperator.GREATER, FilterOperator.LESS, FilterOperator.GREATEREQUAL, FilterOperator.LESSEQUAL, FilterOperator.INLIST}));
								}
								
								else if (col.getAttributes().get("type").equals("DIMENSION"))
								{
									fm.add(new FilterMetaData(YellowfinName, dt, false, new FilterOperator[] {FilterOperator.EQUAL, FilterOperator.NOTEQUAL, FilterOperator.CONTAINS, FilterOperator.NOTCONTAINS, FilterOperator.INLIST}));
								}
							}
							
						}
						
					}
					
					return fm;
				 
				
				
				
				//return fm;
			}
			
			@Override
			public String getDataSetName() {
				// TODO Auto-generated method stub
				return "All Columns";
			}
			
			@Override
			public List<ColumnMetaData> getColumns() {
				
				boolean isETLContext = false;
				
				try {

					isETLContext = isTransformationContext();

				} catch (NoSuchMethodError e) {


				}
				
				List<String> fieldsAllowed = new ArrayList<String>();
				
				if (isETLContext)
				{
					fieldsAllowed = fieldsAllowed();
				}
				ArrayList<Column> cols=getAllColumnsMetadata();
					
					ArrayList<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
					
					for (Column col:cols)
					{
						String YellowfinName=col.getAttributes().get("uiName");
						if (YellowfinName.equals("Users"))
						{
							if (col.getId().equals("ga:cohortActiveUsers"))
							{
								YellowfinName="Cohort Active Users";
							}
							else if (col.getId().equals("ga:cohortTotalUsersWithLifetimeCriteria"))
							{
								YellowfinName="Cohort Total Users With Lifetime Criteria";
							}
						}
						if (YellowfinName.equals("Social Network") && col.getId().equals("ga:socialInteractionNetwork"))
						{
							YellowfinName="Social Interaction Network";
						}
						DataType dt=DataType.TEXT;
						String status=col.getAttributes().get("status");
						if (status.equals("PUBLIC"))
						{
							if (YellowfinName.equals("Date"))
							{
								dt=DataType.DATE;
							}
							else if (YellowfinName.equals("Latitude") || YellowfinName.equals("Longitude"))
							{
								dt=DataType.NUMERIC;
							}
							else if (col.getAttributes().get("dataType").equals("STRING"))
							{
								dt=DataType.TEXT;
							}
							
							else if (col.getAttributes().get("dataType").equals("INTEGER"))
							{
								dt=DataType.INTEGER;
							}
							
							else if (col.getAttributes().get("dataType").equals("PERCENT") 
									|| col.getAttributes().get("dataType").equals("CURRENCY") 
									|| col.getAttributes().get("dataType").equals("FLOAT")
									|| col.getAttributes().get("dataType").equals("TIME"))
							{
								dt=DataType.NUMERIC;
							}
							
							else if (col.getAttributes().get("dataType").equals("DATE"))
							{
								dt=DataType.DATE;
							}
							
							
							boolean addField =false;
							if ((isETLContext && fieldsAllowed.contains(YellowfinName)) || !isETLContext)
							{
								addField = true;
							}
							
							if (addField)
							{
								columns.add(new ColumnMetaData(YellowfinName, dt));
							}
						}
						
					}
					
					return columns;
				
			}
			
			@Override
			public boolean getAllowsDuplicateColumns() {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public boolean getAllowsAggregateColumns() {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override 
			public Object[][] execute (List<ColumnMetaData> columns, List<FilterData> filters) 
			{
				
				String startDateStr="";
				String endDateStr="";
				int j;
				for (FilterData filter : filters)
				{
					if ("Start Date".equals(filter.getFilterName())){
						Date sDate = (Date) filter.getFilterValue();
						startDateStr = sDate.toString();
						startDateStr = startDateStr == null ? "today" : startDateStr;
					} 
					
					else if ("End Date".equals(filter.getFilterName())){
						Date eDate = (Date) filter.getFilterValue();
						endDateStr = eDate.toString();
						endDateStr = endDateStr == null ? "today" : endDateStr;
					}
				}
				
				if (UtilString.isNullOrEmpty(startDateStr)) startDateStr = "today";
				if (UtilString.isNullOrEmpty(endDateStr)) endDateStr = "today";
				
				HashMap<String, String> metricsHash=new HashMap<String, String>();
				HashMap<String, String> dimensionsHash=new HashMap<String, String>();
				
				
					ArrayList<Column> cols=getAllColumnsMetadata();
					
					for (Column col:cols)
					{
						if (col.getAttributes().get("status").equals("PUBLIC"))
						{
							if (col.getAttributes().get("type").equals("DIMENSION"))
							{
								if (col.getAttributes().get("uiName").equals("Social Network") && col.getId().equals("ga:socialInteractionNetwork"))
								{
									dimensionsHash.put(col.getId(), "Social Interaction Network");
								}
								else dimensionsHash.put(col.getId(), col.getAttributes().get("uiName"));
							}
							
							else if(col.getAttributes().get("type").equals("METRIC"))
							{
								if (col.getAttributes().get("uiName").equals("Users") && col.getId().equals("ga:cohortActiveUsers"))
								{
									metricsHash.put(col.getId(), "Cohort Active Users");
								}
								else if (col.getAttributes().get("uiName").equals("Users") && col.getId().equals("ga:cohortTotalUsersWithLifetimeCriteria"))
								{
									metricsHash.put(col.getId(), "Cohort Total Users With Lifetime Criteria");
								}
								else metricsHash.put(col.getId(), col.getAttributes().get("uiName"));
							}
						}
						
					}
				
				
				ArrayList<String> dimensions=new ArrayList<String>();
				ArrayList<String> metrics=new ArrayList<String>();
				
				
				for (j=0; j<columns.size(); j++)
				{
					if(dimensionsHash.containsValue(columns.get(j).getColumnName()))
					{
						for (String key:dimensionsHash.keySet())
						{
							if (dimensionsHash.get(key).equals(columns.get(j).getColumnName()))
							{
								dimensions.add(key);
							}
						}
					}
					
					else if (metricsHash.containsValue(columns.get(j).getColumnName()))
					{
						for (String key:metricsHash.keySet())
						{
							if (metricsHash.get(key).equals(columns.get(j).getColumnName()))
							{
								metrics.add(key);
							}
						}
					}
				}
				
				
				if (dimensions.size()>7)
				{
					throw new ThirdPartyException(GoogleAnalyticsDataZoom.getText("There can't be more than 7 dimensions in your report. Please remove some dimension fields.", "mi.text.ga.dataset.allcolumns.error.message1"));
				}
				else if (metrics.size()==0)
				{
					throw new ThirdPartyException(GoogleAnalyticsDataZoom.getText("There must be at least one metric in your report. Please add a metric field.", "mi.text.ga.dataset.allcolumns.error.message2"));
				}
				else if (metrics.size()>10)
				{
					throw new ThirdPartyException(GoogleAnalyticsDataZoom.getText("There can't be more than 10 metrics in your report. Please remove some metric fields.", "mi.text.ga.dataset.allcolumns.error.message3"));
				}
				
				try {
					Get get;
					String metricsStr="";
					String dimensString="";
					String profileId=getFirstProfileId();
					
					if (metrics.size()>0)
					{
						for (String metric: metrics)
						{
							metricsStr=metricsStr+metric+", ";
						}
						
						metricsStr=metricsStr.substring(0, metricsStr.length()-2);
					}
					
					if (dimensions.size()>0)
					{
						for (String dimension: dimensions)
						{
							dimensString=dimensString+dimension+", ";
						}
						
						dimensString=dimensString.substring(0, dimensString.length()-2);
					}
					analytics = getAnalytics();
					//System.out.println(metricsStr);
					//System.out.println(dimensString);
					get = analytics.data().ga().get("ga:" + profileId, // Table Id. ga: + profile id.
							startDateStr, // Start date.
							endDateStr, // End date.
							metricsStr.toString());
					
					
						/*if (rowsLimit>0)
							get.setMaxResults(rowsLimit);*/
					
					String filtersStr=compileFilters(filters, metricsHash, dimensionsHash);
					if (!filtersStr.equals(""))
					{
						get.setFilters(filtersStr);
					}
					
					//if (dimensionCount > 0) get.setDimensions(dimensionStr.toString());
					//if (filterCount > 0) get.setFilters(filtersStr.toString());
					get.setDimensions(dimensString);
					//System.out.println("");
					
					
					GaData result = null;
					Object[][] data = null;
					boolean isETLContext = false;
					
					try {

						isETLContext = isTransformationContext();

					} catch (NoSuchMethodError e) {


					}
					if(isETLContext)
					{
						if (maxResults!=-1)
						{
							//System.out.println("GA max results: "+maxResults);
							get.setMaxResults(maxResults);
						}
						List<GaData> results = new ArrayList<GaData>();
						result=get.execute();
						int totalSize = 0;
						if (result.getRows() !=null && result.getRows().size()>0)
						{
							boolean stop = false;
							results.add(result);
							totalSize = totalSize + result.getRows().size();
							int c=1;
							get.setStartIndex(rowsLimit+1);
							if (result.getRows().size()<rowsLimit)
							{
								stop = true;
							}
							
							while(!stop)
							{								
								result = get.execute();
								if (result.getRows() != null && result.getRows().size()>0)
								{
									results.add(result);
									c++;
									get.setStartIndex(c*rowsLimit+1);									
									totalSize = totalSize + result.getRows().size();
								}
								else
								{
									stop=true;
								}
								
							}
						}
						data = new Object[totalSize][columns.size()];
						int counter = 0;
						for (GaData gadata: results)
						{
							Object[][] tempData = parseData(gadata, metricsHash, dimensionsHash, columns);
							if (tempData != null && tempData.length>0)
							{
								for (Object[] o: tempData)
								{
									data[counter]=o;
									counter++;
								}
							}
						}
						return data;
					}
					else
					{
						result=get.execute();
						
						if (result == null || result.getRows() == null || result.getRows().isEmpty())
						{
							return null;							
						}
						data = parseData(result, metricsHash, dimensionsHash, columns);
					}
					
					
					
					
					return data;
					//data=buildResultset(result, columns);
				}
				
				catch(com.google.api.client.googleapis.json.GoogleJsonResponseException e)
				{
					throw new ThirdPartyException(e.getDetails().getMessage());
				}
				catch(IOException e)
				{
					throw new ThirdPartyException(e.getMessage());
					
				}
				
				
				
			}
			
			private Object[][] parseData(GaData result
					, HashMap<String, String> metricsHash
					, HashMap<String, String> dimensionsHash
					, List<ColumnMetaData> columns)
			{
				int i;
				Object[][] data=new Object[result.getRows().size()][result.getColumnHeaders().size()];
				List<ColumnHeaders> colsRes = result.getColumnHeaders();
				int counter=0;
				for (ColumnHeaders col:colsRes)
				{
					String name="";
					for (String colName:metricsHash.keySet())
					{
						if (colName.equals(col.getName()))
							name=metricsHash.get(colName);
					}
					
					for (String colName:dimensionsHash.keySet())
					{
						if (colName.equals(col.getName()))
							name=dimensionsHash.get(colName);
					}
					
					for (i=0; i<columns.size(); i++)
					{
						if (columns.get(i).getColumnName().equals(name))
						{
							int k;
							if (columns.get(i).getColumnType().equals(DataType.TEXT))
							{
								for (k=0; k<result.getRows().size(); k++)
									data[k][i]=String.valueOf(result.getRows().get(k).get(counter));
							}
							
							else if (columns.get(i).getColumnType().equals(DataType.DATE))
							{
								for (k=0; k<result.getRows().size(); k++)
								{
									String Dt=result.getRows().get(k).get(counter).substring(0, 4)+"-"+result.getRows().get(k).get(counter).substring(4, 6)+"-"+result.getRows().get(k).get(counter).substring(6, 8);
									java.sql.Date dt=java.sql.Date.valueOf(Dt);
									data[k][i]=dt;
								}
							}
							
							else if (columns.get(i).getColumnType().equals(DataType.INTEGER))
							{
								for (k=0; k<result.getRows().size(); k++)
								{
									data[k][i]=Integer.valueOf(result.getRows().get(k).get(counter));
								}
							}
							
							else if (columns.get(i).getColumnType().equals(DataType.NUMERIC))
							{
								for (k=0; k<result.getRows().size(); k++)
								{
									data[k][i]=Double.valueOf(result.getRows().get(k).get(counter));
								}
							}
						}
					}
					
					counter=counter+1;
				}
				
				return data;
			}

			private List<String> fieldsAllowed()
			{
				List<String> fieldsAllowed = new ArrayList<String>();
				
				fieldsAllowed.add("Country");
				fieldsAllowed.add("City");
				fieldsAllowed.add("Browser");
				fieldsAllowed.add("Referral Source");
				fieldsAllowed.add("Page");
				fieldsAllowed.add("Browser");
				fieldsAllowed.add("Users");
				fieldsAllowed.add("Avg. Session Duration");
				fieldsAllowed.add("Sessions");
				fieldsAllowed.add("Bounce Rate");
				fieldsAllowed.add("Pageviews");
				fieldsAllowed.add("Pages / Session");
				fieldsAllowed.add("Default Channel Grouping");
				fieldsAllowed.add("Page Title");
				fieldsAllowed.add("User Type");
				fieldsAllowed.add("Language");
				fieldsAllowed.add("Continent");
				fieldsAllowed.add("In-Market Segment");
				fieldsAllowed.add("Default Channel Grouping");
				fieldsAllowed.add("Gender");
				fieldsAllowed.add("Age");
				fieldsAllowed.add("Affinity Category (reach)");
				fieldsAllowed.add("Operating System");
				fieldsAllowed.add("Device Category");
				fieldsAllowed.add("Avg. Page Load Time (sec)");
				fieldsAllowed.add("Date");
				
				
				return fieldsAllowed;
			}
			private String compileFilters(List<FilterData> filters, HashMap<String, String> metricsHash, HashMap<String, String> dimensionsHash) 
			{
				// TODO Auto-generated method stub
				String request="";
				
				for (FilterData flt:filters)
				{
					String name=flt.getFilterName();
					String query="";
					boolean isMetrics=false;
					if (!name.equals("Start Date") && !name.equals("End Date"))
					{
						for (String key:metricsHash.keySet())
						{
							if (metricsHash.get(key).equals(name))
							{
								query=key;
								isMetrics=true;
							}
						}
						
						if (!isMetrics)
						{
							for (String key:dimensionsHash.keySet())
							{
								if (dimensionsHash.get(key).equals(name))
								{
									query=key;
								}
							}
						}
						
						String operator=getOperator(flt.getFilterOperator(), isMetrics);
						
						if (flt.getFilterOperator().equals(FilterOperator.INLIST))
						{
							List<Object> filterValues=getFilterValues(flt);
							for (Object val:filterValues)
							{
								request=request+query+operator+val+",";
							}
							
							request=request.substring(0, request.length()-1);
						}
						
						else
						{
							Object val=flt.getFilterValue();
							
							request=request+query+operator+val;
						}
						
						request=request+";";
						
						
					}
					
					
					
					
					}
				
				if (request.length()>0)
				{
					request=request.substring(0, request.length()-1);
				}
				
				else return "";
				log.info(request);
					
				return request;
			}

			private List<Object> getFilterValues(FilterData flt) 
			{
				List<Object> values=(List<Object>) flt.getFilterValue();
				
				return values;
			}
			
			
			private String getOperator(Object filterOperator, boolean isMetrics) 
			{
				// TODO Auto-generated method stub
				if (isMetrics)
				{
					if (filterOperator.equals(FilterOperator.EQUAL))
					{
						return "==";
					}
					
					else if (filterOperator.equals(FilterOperator.NOTEQUAL))
					{
						return "!=";
					}
					
					else if (filterOperator.equals(FilterOperator.GREATER))
					{
						return ">";
					}
					
					else if (filterOperator.equals(FilterOperator.LESS))
					{
						return "<";
					}
					
					else if (filterOperator.equals(FilterOperator.GREATEREQUAL))
					{
						return ">=";
					}
					
					else if (filterOperator.equals(FilterOperator.LESSEQUAL))
					{
						return "<=";
					}
					
					else if (filterOperator.equals(FilterOperator.INLIST))
					{
						return "==";
					}
				}
				
				else if (!isMetrics)
				{
					if (filterOperator.equals(FilterOperator.EQUAL))
					{
						return "==";
					}
					
					else if (filterOperator.equals(FilterOperator.NOTEQUAL))
					{
						return "!=";
					}
					
					else if (filterOperator.equals(FilterOperator.CONTAINS))
					{
						return "=@";
					}
					
					else if (filterOperator.equals(FilterOperator.NOTCONTAINS))
					{
						return "!@";
					}
					
					else if (filterOperator.equals(FilterOperator.INLIST))
					{
						return "==";
					}
				}
				
				return "==";
			}
		};
		
		return dtSet;
	}

	@Override
	public JDBCMetaData getDataSourceMetaData() {
		if (sourceMetadata == null) sourceMetadata = new GoogleAnalyticsMetaData();
		return sourceMetadata;
	}

	@Override
	public boolean authenticate() throws IOException {
		
	
		return true;
	}
	
	
	public void disconnect(){
		//API_KEY = null;
		//privKeyPEM = null;
		//serviceAccountId = null;
		//credential.setExpiresInSeconds(0L);
		//credential = null;
		//analytics = null;
	}
	
	/**
	 * Returns the google credential value.
	 * If the crdential has been set previously, just returns it,
	 * otherwise sets it using the google api. 
	 * */
	private GoogleCredential getCredential(){
		//if (credential != null) return credential;
		
		
		//this key came from the json file downloaded from the google developers console.
		//there are two options to validate the credential:
		// 1 - using the key from the json, creating a PrivateKey using KeyFactory and using it in the credential builder
		// 2 - using the p12 file directly into the credential builder
		//String privKeyPEM = "MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBANblh0DXwVYOVvYJUjcl0ziUpJe0kON0wdZfSy9TZAfY+m9qV2ygnHThTtKGBoCyxoM6FqwR3u5Cd4vQGEeh+qxP1NPGjklJ2iYUpIrOL+FLxCCYw5zvW5dS/WYPv0TKDwaYvkliOQnQ2bolTr92rBFnS2FFIMSquhMEbReSppobAgMBAAECgYAzAvdRUCYHzI2eB+ZpSuRR6Q8NKALAy6V7wtExIcV8C2ifbAnmslWRdS5l0QTYJhfzbKWXIQLfYg3ItZQd5PiA4XKD8litWXa/591DviT4wNAAcpSS5tAE57b4ZsMYPT8bFoPqKPAJIX/X4j+DTQcWwCBYC+8oQPaa74MvXX620QJBAOx/pShoN5gwTXAEwPsV98Slgz0hem+JAWMMmn7Ae9aoMtcGuAkjDoqBDAjEhYuGTovh7JOM+VI5UjJ29fl/U0kCQQDoneBj2keRPoh6R/Rr5XQSORIu6BiaZlQvXNMKqO+dqYhhOKak3FpUf6g8Pk61pNXHCdAd0aoSCtV8idKIx15DAkEAnH+7YwnUADm2hLIgogbfdpmwRvuocbZP3KOyeL4XNO0I95HSpvkz3iOXOxYQ6UtvHtHaI6neMrML2akvDHNdQQJBALW1LeWSSzmAOagbsSjfkm3xuux9TUq/CJ/+yLSZBqSIHAql8Db0EnPTTJ3SpjVqT7wtRC8m6s0xPVcNajKCWnUCQGxD4oKLyHs7e8ul5CuvKdv2/4pSovoCpRfYCyxRAri2KTTTMIMJnZKbQkab2p5btB77yBrEkdb63D58LFQgga4\u003d";
		
		/*byte [] encoded = Base64.decode(privKeyPEM);
		PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
		KeyFactory kf = null;
		try {
			kf = KeyFactory.getInstance("RSA");
		} catch (NoSuchAlgorithmException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		PrivateKey privKey = null;
		try {
			privKey = kf.generatePrivate(keySpec);
		} catch (InvalidKeySpecException e2) {
			// TODO Auto-generated catch block
			this.error = new IOException("Invalid Private Key.");
			e2.printStackTrace();
			credential = null;
			return null;
		}*/
		
		Collection<String> scopes = new ArrayList<String>();
		scopes.add(AnalyticsScopes.ANALYTICS_READONLY);
		
		credential = new GoogleCredential.Builder()
										.setTransport(HTTP_TRANSPORT)
										.setJsonFactory(JSON_FACTORY)
										.setClientSecrets(new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getData())), new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getZoom())))
										.build()
										.setAccessToken(getAccessToken())
										.setRefreshToken(getRefreshToken());
		
		return credential;
	}
	
	private String getRefreshToken() {
		String refreshToken;
		if (loadBlob("REFRESH_TOKEN")!=null)
		{
			refreshToken=new String(loadBlob("REFRESH_TOKEN"));
		}
		
		else 
		{
			refreshToken=(String) getAttribute("REFRESH_TOKEN");
		}
		return refreshToken;
	}

	private String getAccessToken() {
		String accessToken;
		if (loadBlob("ACCESS_TOKEN")!=null)
		{
			accessToken=new String(loadBlob("ACCESS_TOKEN"));
		}
		
		else 
		{
			accessToken=(String) getAttribute("ACCESS_TOKEN");
		}
		return accessToken;
	}

	/**
	 * Returns the google analytics object (connects using the credential).
	 * If the analytics has been set previously, just returns it,
	 * otherwise sets it using the google api. 
	 * */
	protected Analytics getAnalytics(){
		//if (analytics != null) return analytics;
		
		String pAddr=null;;
        String pPort=null;
        OrgCache oc;
		try {
			oc = OrgCache.getInstance();
		
		Integer ipOrg = Const.UNO;
		SessionBean sb = i4RequestRegistry.getInstance().getCurrentSessionBean();
		if (sb != null) ipOrg = sb.getPersonSearchIpOrg();
		pAddr = oc.getOrgParm(ipOrg, Const.C_OUTGOINGPROXYSERVER);
		pPort = oc.getOrgParm(ipOrg, Const.C_OUTGOINGPROXYPORT);
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (pAddr!=null && pPort!=null)
		{
			Proxy p = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(pAddr, Integer.valueOf(pPort)));
        	HTTP_TRANSPORT = new NetHttpTransport.Builder()
        			.setProxy(p)
        			.build();
		}
		
		analytics = new Analytics.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredential())
									.setApplicationName("Yellowfin")
									.build();
		
		
		return analytics;
	}

	public HashMap<String, Object> testConnection() throws IOException{
		HashMap<String, Object> res=new HashMap<String, Object>();
		try
		{
			
			String pAddr=null;;
	        String pPort=null;
	        OrgCache oc;
			try {
				oc = OrgCache.getInstance();
			
    		Integer ipOrg = Const.UNO;
    		SessionBean sb = i4RequestRegistry.getInstance().getCurrentSessionBean();
			if (sb != null) ipOrg = sb.getPersonSearchIpOrg();
    		pAddr = oc.getOrgParm(ipOrg, Const.C_OUTGOINGPROXYSERVER);
			pPort = oc.getOrgParm(ipOrg, Const.C_OUTGOINGPROXYPORT);
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (pAddr!=null && pPort!=null)
			{
				Proxy p = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(pAddr, Integer.valueOf(pPort)));
	        	HTTP_TRANSPORT = new NetHttpTransport.Builder()
	        			.setProxy(p)
	        			.build();
			}
			GoogleCredential credential = new GoogleCredential.Builder()
			.setTransport(HTTP_TRANSPORT)
			.setJsonFactory(JSON_FACTORY)
			.setClientSecrets(new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getData())), new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getZoom())))
			.build()
			.setAccessToken(getAccessToken())
			.setRefreshToken(getRefreshToken());
			
			Analytics a = new com.google.api.services.analytics.Analytics.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
			.setApplicationName("Yellowfin")
			.build();
			
			com.google.api.services.analytics.model.Profiles pfls=a.management().profiles().list("~all", "~all").execute();//accounts().list().execute();
			
			
			boolean profileIDCorrect=false;
			for (Profile p:pfls.getItems()) 
			{
				if(p.getId().equals((String) getAttribute("PROFILEID")))
				{
					res.put(GoogleAnalyticsDataZoom.getText("Name", "mi.text.ga.testconnection.name"), p.getName());
					res.put(GoogleAnalyticsDataZoom.getText("Type", "mi.text.ga.testconnection.type"), p.getType());
					res.put(GoogleAnalyticsDataZoom.getText("Website Url", "mi.text.ga.testconnection.websiteurl"), p.getWebsiteUrl());
					res.put(GoogleAnalyticsDataZoom.getText("Web Property ID", "mi.text.ga.testconnection.webpropertyid"), p.getWebPropertyId());
					res.put(GoogleAnalyticsDataZoom.getText("Created Date", "mi.text.ga.testconnection.createddate"), p.getCreated().toString());
					res.put(GoogleAnalyticsDataZoom.getText("Account ID", "mi.text.ga.testconnection.accountid"), p.getAccountId());
					res.put(GoogleAnalyticsDataZoom.getText("Default Page", "mi.text.ga.testconnection.defaultpage"), p.getDefaultPage());
					profileIDCorrect=true;
				}
				
			}
			
			if(!profileIDCorrect)
			{
				res.put("ERROR", GoogleAnalyticsDataZoom.getText("The Profile ID you provided was incorrect.", "mi.text.ga.dataset.allcolumns.error.message4"));
			}
		}
		
		catch(Exception e)
		{
			res.put("ERROR", GoogleAnalyticsDataZoom.getText("Unable to connect to the specified website.", "mi.text.ga.dataset.allcolumns.error.message5"));
		}
		
		return res;
	}
	
	public boolean autoRun()
	{
		cacheAllColumns();

		if (loadBlob("LASTRUN")!=null)
		{
			java.util.Date curDt=new java.util.Date();
			java.util.Date lastrun=new java.util.Date(new String(loadBlob("LASTRUN")));
			long diff = curDt.getTime() - lastrun.getTime();
			long minsDiff=diff / (60 * 1000);
			
			if (minsDiff>=50)
			{
				String pAddr=null;;
		        String pPort=null;
		        OrgCache oc;
				try {
					oc = OrgCache.getInstance();
				
	    		Integer ipOrg = Const.UNO;
	    		SessionBean sb = i4RequestRegistry.getInstance().getCurrentSessionBean();
				if (sb != null) ipOrg = sb.getPersonSearchIpOrg();
	    		pAddr = oc.getOrgParm(ipOrg, Const.C_OUTGOINGPROXYSERVER);
				pPort = oc.getOrgParm(ipOrg, Const.C_OUTGOINGPROXYPORT);
				
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (pAddr!=null && pPort!=null)
				{
					Proxy p = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(pAddr, Integer.valueOf(pPort)));
		        	HTTP_TRANSPORT = new NetHttpTransport.Builder()
		        			.setProxy(p)
		        			.build();
		        	
				}
				try {
					GoogleCredential cr;
					if(loadBlob("ACCESS_TOKEN")!=null && loadBlob("REFRESH_TOKEN")!=null)
					{
						cr = new GoogleCredential.Builder()
						.setTransport(HTTP_TRANSPORT)
						.setJsonFactory(JSON_FACTORY)
						.setClientSecrets(new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getData())), new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getZoom())))
						.build()
						.setAccessToken(new String(loadBlob("ACCESS_TOKEN")))
						.setRefreshToken(new String(loadBlob("REFRESH_TOKEN")));
					}
					
					else
					{
						cr = new GoogleCredential.Builder()
						.setTransport(HTTP_TRANSPORT)
						.setJsonFactory(JSON_FACTORY)
						.setClientSecrets(new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getData())), new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getZoom())))
						.build()
						.setAccessToken((String)getAttribute("ACCESS_TOKEN"))
						.setRefreshToken((String)getAttribute("REFRESH_TOKEN"));
					}
					if (cr.refreshToken())
					{
						saveBlob("ACCESS_TOKEN", cr.getAccessToken().getBytes());
						saveBlob("REFRESH_TOKEN", cr.getRefreshToken().getBytes());
						log.info("Access Token Updated Successfully");
					}
					else
					{
						log.error("Could not update token!");
						return false;
					}
					
					
					
					
				} 
				catch (IOException e) 
				{
					log.error("Could not update token!");
					return false;
				}
			}
			
			
			saveBlob("LASTRUN", new Date(System.currentTimeMillis()).toLocaleString().getBytes());
			return true;
		}
		
		else
		{
			saveBlob("LASTRUN", new Date(System.currentTimeMillis()).toLocaleString().getBytes());
			return true;
		}
		
	}
	
	private ArrayList<Column> getAllColumnsMetadata()
	{
		if (loadBlob("ALLCOLUMNSMETADATA")==null)
		{
			cacheAllColumns();
		}
		JSONArray columnsArray=new JSONArray(new String(loadBlob("ALLCOLUMNSMETADATA")));
		ArrayList<Column> cols=new ArrayList<Column>();
		
		int i;
		for (i=0; i<columnsArray.length(); i++)
		{
			Column c=new Column();
			c.setId(columnsArray.getJSONObject(i).getString("id"));
			
			HashMap<String, String> attributes=new HashMap<String, String>();
			attributes.put("uiName", columnsArray.getJSONObject(i).getString("uiName"));
			attributes.put("status", columnsArray.getJSONObject(i).getString("status"));	
			attributes.put("dataType", columnsArray.getJSONObject(i).getString("dataType"));
			attributes.put("type", columnsArray.getJSONObject(i).getString("type"));
			c.setAttributes(attributes);
			
			cols.add(c);
		}
		
		return cols;
	}

	/**
	 * Get all the columns from GA connector and save a json array which stores all the columns details
	 * into the database
	 * @return JSONArray JSONArray object that is saved into the db
	 */
	protected JSONArray cacheAllColumns() {

		JSONArray colsToSave = new JSONArray();

		try {
			Analytics analytics = getAnalytics();
			List<Column> cols = getAllColumns(analytics);
			Profiles pfls = getProfiles(analytics);

			int numberOfGoals = 0;
			String accountId = null;
			String webPropertyId = null;
			String profileId = (String) getAttributeObject(PROFILE_ID_KEY);
			
			for (Profile p: pfls.getItems()) {
				if (p.getId().equals((String) getAttributeObject(PROFILE_ID_KEY))) {
					accountId = p.getAccountId();
					webPropertyId = p.getWebPropertyId();
				}
			}
			
			if (accountId != null && webPropertyId != null) {
				Goals goals = getGoals(analytics, accountId, webPropertyId, profileId);
				numberOfGoals = goals.getItems().size();	
			}

			Map<String, Integer> columnUINameOcurrences = countDuplicatedColumnUINames(cols, numberOfGoals);

			for (Column col: cols) {
				if (col.getAttributes().get(UI_NAME_KEY).contains("Goal XX ") && numberOfGoals > 0) {
					for (int c = 1; c <= numberOfGoals; c++) {
						String uiName = col.getAttributes().get(UI_NAME_KEY).replace("XX", String.valueOf(c));
						String id = col.getId().replace("XX", String.valueOf(c));

						//if there are more than 1 occurrences of this UI Name, append the technical name of the column
						if (columnUINameOcurrences.get(uiName) != null &&
							columnUINameOcurrences.get(uiName) > 1) {
							uiName = uiName + " (" + id + ")";
						}

						JSONObject column = new JSONObject();

						column.put(UI_NAME_KEY, uiName);
						column.put(STATUS_KEY, col.getAttributes().get(STATUS_KEY));
						column.put(DATA_TYPE_KEY, col.getAttributes().get(DATA_TYPE_KEY));
						column.put(TYPE_KEY, col.getAttributes().get(TYPE_KEY));
						column.put(ID_KEY, id);

						colsToSave.put(column);
					}
				}
				else {
					String uiName = col.getAttributes().get(UI_NAME_KEY);
					String id = col.getId();

					//if there are more than 1 occurrences of this UI Name, append the technical name of the column
					if (columnUINameOcurrences.get(uiName) != null &&
						columnUINameOcurrences.get(uiName) > 1) {
						uiName = uiName + " (" + id + ")";
					}

					JSONObject column = new JSONObject();

					column.put(UI_NAME_KEY, uiName);
					column.put(STATUS_KEY, col.getAttributes().get(STATUS_KEY));
					column.put(DATA_TYPE_KEY, col.getAttributes().get(DATA_TYPE_KEY));
					column.put(TYPE_KEY, col.getAttributes().get(TYPE_KEY));
					column.put(ID_KEY, col.getId());

					colsToSave.put(column);
				}
			}	

			saveBlobData(ALL_COLUMNS_METADATA_KEY, colsToSave.toString().getBytes());
		} 
		catch (IOException e) {
			log.error("Error occurred when updating the list of columns");
		}
		
		return colsToSave;
	}

	protected Object getAttributeObject(String key) {
		return getAttribute(key);
	}
	/**
	 * Get all the columns of GA
	 * @param analytics Analytics instance
	 * @return List<Column> List of all columns of GA
	 * @throws Exception
	 */
	protected List<Column> getAllColumns(Analytics analytics) throws IOException {
		return analytics.metadata().columns().list("ga").execute().getItems();
	}

	/**
	 * Get profiles of GA
	 * @param analytics Analytics instance
	 * @return Profiles of GA
	 * @throws IOException
	 */
	protected Profiles getProfiles(Analytics analytics) throws IOException {
		return analytics.management().profiles().list("~all", "~all").execute();
	}

	/**
	 * Get goals of GA
	 * @param analytics Analytics instance
	 * @param accountId accountId
	 * @param webPropertyId webPropertyId
	 * @param profileId profileId
	 * @return
	 * @throws IOException
	 */
	protected Goals getGoals(Analytics analytics, String accountId, String webPropertyId, String profileId) throws IOException {
		return analytics.management().goals().list(accountId, webPropertyId, profileId).execute();

	}

	/**
	 * Count the number of occurrences of each column's UI name
	 * @param columns List of the Columns
	 * @param numberOfGoals Number of Goals
	 * @return Map<String, Integer> Map storing number of occurrences of UI names
	 */
	private Map<String, Integer> countDuplicatedColumnUINames(List<Column> columns, int numberOfGoals) {
		Map<String, Integer> columnUINamesCount = new HashMap<>();

		//loop through all the columns
		for (Column column: columns) {
			String uiName = column.getAttributes().get("uiName");

			//Custom Dimension/Metrics, where XX refers to the number/index of the custom dimension/metric
			if (uiName.contains("Goal XX ") && numberOfGoals > 0) {
				//looping through number of goals and replace XX with the index
				for (int c = 1; c <= numberOfGoals; c++) {
					String uiNameWithIndex = uiName.replace("XX", String.valueOf(c));

					checkDuplicatedColumnUINames(columnUINamesCount, uiNameWithIndex);
				}
			}
			else {
				checkDuplicatedColumnUINames(columnUINamesCount, uiName);
			}
		}

		return columnUINamesCount;
	}

	/**
	 * Check if a column's UI name is duplicated
	 * @param columnUINamesCount Map storing number of occurrences of UI names
	 * @param uiName Current UI Name that needs to check
	 */
	private void checkDuplicatedColumnUINames(Map<String, Integer> columnUINamesCount, String uiName) {
		Integer count = columnUINamesCount.get(uiName);

		//if there are no occurences yet, set 1
		if (count == null) {
			columnUINamesCount.put(uiName, 1);
		}
		else {//if there are already occurences, increment
			count = count + 1;
			columnUINamesCount.put(uiName, count);
		}
	}

	private String getFirstProfileId() throws IOException {
		String profileId = null;

		profileId=(String)getAttribute("PROFILEID");
		if (profileId==null || profileId=="")
		{
			log.error("No valid profile ID found");
		}
		return profileId;
	}
	
	public boolean isTransformationCompatible()
	{
		return true;
	}

	/**
	 * Save blob data by calling parent method from AbstractDataSource
	 * since it cannot be overriden (final method).
	 *
	 * Mainly used to mock data so it can be testable
	 * @param key key of the document
	 * @param data byte[] data
	 * @return
	 */
	protected boolean saveBlobData(String key, byte[] data) {
		return saveBlob(key, data);
	}

	/**
	 * Load blob data by calling parent method from AbstractDataSource
	 * since it cannot be overriden (final method).
	 *
	 * Mainly used to mock data so it can be testable
	 * @param key key of the document
	 * @return
	 */
	protected byte[] loadBlobData(String key) {
		return loadBlob(key);
	}
}
