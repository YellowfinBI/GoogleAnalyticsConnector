package com.hof.imp;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import org.json.*;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.Analytics.Data.Ga.Get;
import com.google.api.services.analytics.AnalyticsScopes;

import com.google.api.services.analytics.model.Column;
import com.google.api.services.analytics.model.Columns;
import com.google.api.services.analytics.model.GaData;
import com.google.api.services.analytics.model.GaData.ColumnHeaders;
import com.google.api.services.analytics.model.Profile;
import com.google.api.services.analytics.model.Profiles;
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

	private static String privKeyPEM;// = "MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBANblh0DXwVYOVvYJUjcl0ziUpJe0kON0wdZfSy9TZAfY+m9qV2ygnHThTtKGBoCyxoM6FqwR3u5Cd4vQGEeh+qxP1NPGjklJ2iYUpIrOL+FLxCCYw5zvW5dS/WYPv0TKDwaYvkliOQnQ2bolTr92rBFnS2FFIMSquhMEbReSppobAgMBAAECgYAzAvdRUCYHzI2eB+ZpSuRR6Q8NKALAy6V7wtExIcV8C2ifbAnmslWRdS5l0QTYJhfzbKWXIQLfYg3ItZQd5PiA4XKD8litWXa/591DviT4wNAAcpSS5tAE57b4ZsMYPT8bFoPqKPAJIX/X4j+DTQcWwCBYC+8oQPaa74MvXX620QJBAOx/pShoN5gwTXAEwPsV98Slgz0hem+JAWMMmn7Ae9aoMtcGuAkjDoqBDAjEhYuGTovh7JOM+VI5UjJ29fl/U0kCQQDoneBj2keRPoh6R/Rr5XQSORIu6BiaZlQvXNMKqO+dqYhhOKak3FpUf6g8Pk61pNXHCdAd0aoSCtV8idKIx15DAkEAnH+7YwnUADm2hLIgogbfdpmwRvuocbZP3KOyeL4XNO0I95HSpvkz3iOXOxYQ6UtvHtHaI6neMrML2akvDHNdQQJBALW1LeWSSzmAOagbsSjfkm3xuux9TUq/CJ/+yLSZBqSIHAql8Db0EnPTTJ3SpjVqT7wtRC8m6s0xPVcNajKCWnUCQGxD4oKLyHs7e8ul5CuvKdv2/4pSovoCpRfYCyxRAri2KTTTMIMJnZKbQkab2p5btB77yBrEkdb63D58LFQgga4\u003d";
	private static String serviceAccountId;// = 298870426746-su4690pfjnsk52a545nmencpv9l89bf3@developer.gserviceaccount.com
	private static GoogleCredential credential;
	private static Analytics analytics;
	private Collection<AbstractDataSet> dataSets;
	private IOException error = null;
	private int rowsLimit=10000;
	
	private static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static JsonFactory JSON_FACTORY = new JacksonFactory();
	
	private JDBCMetaData sourceMetadata = null;
	private final static Logger log = Logger.getLogger(GoogleAnalytics.class);
	
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
		if (this.dataSets != null) return this.dataSets;
		
		this.dataSets = getGoogleApiDataSetsByGroup();
		
		return this.dataSets;
	}

	private Collection<AbstractDataSet> getGoogleApiDataSetsByGroup() {
		//AnalyticsRequestInitializer analyticsInitializer = new AnalyticsRequestInitializer(API_KEY);

		Columns columns = null;
		try {
			columns = getAnalytics().metadata()
					.columns()
					.list("ga")
					.execute();
		} catch (GoogleJsonResponseException e) {

			//change HARDCODED message
			this.error = new IOException("There was a service error: " + e.getDetails().getCode() + " : "
					+ e.getDetails().getMessage()+ e.getDetails().getErrors());
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			this.error = e;
		}
		
		
		HashMap<String, AbstractDataSet> datasets = new HashMap<String, AbstractDataSet>();
		
		//this holds the information if the column (name) is a dimension (true) or a metric (false)
		final HashMap<String, Boolean> datasetColumnIsDimension = new HashMap<String, Boolean>();
		
		/*for (Column column : columns.getItems()) {
			final String groupName = column.getAttributes().get("group");
			
			String columnName = column.getId().split(":")[1];
			
			if (!allowedFieldsByGroup(columnName, groupName)) continue;
			
			String columnTypeStr = column.getAttributes().get("dataType");
			DataType columnType = DataType.TEXT;
			if ("FLOAT".equals(columnTypeStr) ||
					"CURRENCY".equals(columnTypeStr) ||
					"PERCENT".equals(columnTypeStr)) 
				columnType = DataType.NUMERIC;
			else if ("INTEGER".equals(columnTypeStr)) columnType = DataType.INTEGER;
			else if ("DATE".equals(columnTypeStr)) columnType = DataType.DATE;
			
			//test with aggregations
			AggregationType[] availableAggregations = null;
			/*if (columnType.equals(DataType.INTEGER)) {
				availableAggregations = new AggregationType[]{AggregationType.COUNT, AggregationType.COUNTDISTINCT, AggregationType.AVG, AggregationType.MAX, AggregationType.MIN};
			}else if (columnType.equals(DataType.TEXT)){
				availableAggregations = new AggregationType[]{AggregationType.COUNT, AggregationType.COUNTDISTINCT};
			}else{
				availableAggregations = null;
			}
			
			final ColumnMetaData cmd = new ColumnMetaData(columnName, columnType); 
			datasetColumnIsDimension.put(columnName, "DIMENSION".equals(column.getAttributes().get("type")));
			
			
			if (datasets.containsKey(groupName)){
				datasets.get(groupName).getColumns().add(cmd);
			} else {
				datasets.put(groupName, new AbstractDataSet() {
					
					
					List<ColumnMetaData> columns;
					
					@Override
					public List<FilterMetaData> getFilters() {
						ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
						
						fm.add(new FilterMetaData("Start Date", DataType.DATE, true, new FilterOperator[] {FilterOperator.EQUAL}));
						fm.add(new FilterMetaData("End Date", DataType.DATE, true, new FilterOperator[] {FilterOperator.EQUAL}));
						
						/*if ("User".equals(groupName)){
							FilterOperator[] operators = {FilterOperator.EQUAL, FilterOperator.NOTEQUAL, FilterOperator.INLIST};
							AggregationType[] aggregations = {AggregationType.SUM, AggregationType.AVG};
							fm.add(new FilterMetaData("userType", DataType.TEXT, false, operators, aggregations));
						}

						return fm;
					}
					
					@Override
					public String getDataSetName() {
						return groupName;
					}
					
					@Override
					public List<ColumnMetaData> getColumns() {
						if (columns == null){
							columns = new ArrayList<ColumnMetaData>();
							columns.add(cmd);
						}
						return columns;
					}
					
					
					@Override
					public Object[][] execute(List<ColumnMetaData> columns,
							List<FilterData> filters) {
						
						GaData gaData = null;
						try {
							gaData = executeDataQuery(columns, filters, datasetColumnIsDimension, super.maxResults);
						} catch (IOException e) {
							e.printStackTrace();
						}
						//printGaData(gaData);
						
						return buildResultset(gaData, columns); 
					}

					@Override
					public List<HierarchyMetaData> getHierarchies() {
						return null;
					}

					@Override
					public boolean getAllowsDuplicateColumns() {
						return false;
					}

					@Override
					public boolean getAllowsAggregateColumns() {
						/*if ("User".equals(groupName))
							return true;
						return false;
					}
				});
				
			}
		}*/
		//datasets.put("Dashboard Dataset", Dashboard());
		datasets.put("All Columns", AllColumns());
		//datasets.put("Bounces/Retained", RetainedBounced());
		//datasets.put("New VS Returning", NewVSReturning());
		return datasets.values();
	}
	
	public AbstractDataSet RetainedBounced()
	{
		AbstractDataSet dtSet=new AbstractDataSet() {
			
			@Override
			public List<FilterMetaData> getFilters() {
				// TODO Auto-generated method stub
				ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
				
				fm.add(new FilterMetaData("Start Date", DataType.DATE, true, new FilterOperator[] {FilterOperator.EQUAL}));
				fm.add(new FilterMetaData("End Date", DataType.DATE, true, new FilterOperator[] {FilterOperator.EQUAL}));
				
				
				
				return fm;
			}
			
			@Override
			public String getDataSetName() {
				// TODO Auto-generated method stub
				return "Bounces/Retained";
			}
			
			@Override
			public List<ColumnMetaData> getColumns() {
				
				ArrayList<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
				
				columns.add(new ColumnMetaData("Type", DataType.TEXT));
				columns.add(new ColumnMetaData("Quantity", DataType.INTEGER));
				//columns.add(new ColumnMetaData("Retained", DataType.INTEGER));
				//columns.add(new ColumnMetaData("Retained", DataType.INTEGER));
				
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
			public Object[][] execute (List<ColumnMetaData> columns, List<FilterData> filters) {
				
				HashMap<String, String> metricsHash=new HashMap<String, String>();
				HashMap<String, String> dimensionsHash=new HashMap<String, String>();
				
				
				metricsHash.put("ga:sessions", "Sessions");
				metricsHash.put("ga:bounces", "Bounces");
				
				
				boolean containsRetained=false;
				
				int i, j;
				String startDateStr="";
				String endDateStr="";
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
				
				ArrayList<String> dimensions=new ArrayList<String>();
				ArrayList<String> metrics=new ArrayList<String>();
								
				try {
					Get get;
					String metricsStr="";
					String dimensString="";
					String profileId=getFirstProfileId();
					
					
					
					get = analytics.data().ga().get("ga:" + profileId, // Table Id. ga: + profile id.
							startDateStr, // Start date.
							endDateStr, // End date.
							"ga:sessions,ga:bounces");
					
					/*if (maxResults > 0)
						get.setMaxResults(maxResults);*/
					
					//if (dimensionCount > 0) get.setDimensions(dimensionStr.toString());
					//if (filterCount > 0) get.setFilters(filtersStr.toString());
					get.setDimensions("");
					GaData result=get.execute();
					GaData retained=new GaData();
					int sessionsIndex=0, bouncesIndex=0;
					
					Object[][] data;
					
					data=new Object[result.getRows().size()][result.getColumnHeaders().size()];
					ArrayList<Object[]> table=new ArrayList<Object[]>();
					
					//Object[][] data=new Object[result.getRows().size()][columns.size()];
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
						
						if (name.equals("Sessions"))
						{
							sessionsIndex=counter;
						}
						
						else bouncesIndex=counter;
						
						counter++;						
					}
					
					List<List<String>> res=result.getRows();
					//List<Rows> rows=res.getRows();
					Object[][] full=new Object[2][2];
					for(List<String> row:res)
					{
						
						
						int Sessions=Integer.valueOf(row.get(sessionsIndex));
						full[0][0]="Bounced";
						full[0][1]=Integer.valueOf(row.get(bouncesIndex));
						full[1][0]="Retained";
						full[1][1]=(Integer)(Sessions-(Integer)full[0][1]);						
					}
					
					data=new Object[2][columns.size()];
					for (i = 0; i < columns.size(); i++) 
					{
						if (columns.get(i).getColumnName().equals("Type"))
						{
							data[0][i]=full[0][0];
							data[1][i]=full[1][0];
						}
						
						else if (columns.get(i).getColumnName().equals("Quantity"))
						{
							data[0][i]=full[0][1];
							data[1][i]=full[1][1];
						}
					}
					
					
					
					
					return data;
					//data=buildResultset(result, columns);
				}
				
				catch(IOException e)
				{
					e.printStackTrace();
				}
				return null;
				
				
			}
		};
		
		return dtSet;
	}
	
	public AbstractDataSet NewVSReturning()
	{
		AbstractDataSet dtSet=new AbstractDataSet() {
			
			@Override
			public List<FilterMetaData> getFilters() {
				// TODO Auto-generated method stub
				ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
				
				fm.add(new FilterMetaData("Start Date", DataType.DATE, true, new FilterOperator[] {FilterOperator.EQUAL}));
				fm.add(new FilterMetaData("End Date", DataType.DATE, true, new FilterOperator[] {FilterOperator.EQUAL}));
				
				
				
				return fm;
			}
			
			@Override
			public String getDataSetName() {
				// TODO Auto-generated method stub
				return "New VS Returning Users";
			}
			
			@Override
			public List<ColumnMetaData> getColumns() {
				
				ArrayList<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
				
				columns.add(new ColumnMetaData("Type", DataType.TEXT));
				columns.add(new ColumnMetaData("Quantity", DataType.INTEGER));
				//columns.add(new ColumnMetaData("Retained", DataType.INTEGER));
				
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
			public Object[][] execute (List<ColumnMetaData> columns, List<FilterData> filters) {
				
				HashMap<String, String> metricsHash=new HashMap<String, String>();
				HashMap<String, String> dimensionsHash=new HashMap<String, String>();
				
				
				metricsHash.put("ga:users", "Users");
				metricsHash.put("ga:newUsers", "New Users");
				
				
				boolean containsRetained=false;
				
				int i, j;
				String startDateStr="";
				String endDateStr="";
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
				
				ArrayList<String> dimensions=new ArrayList<String>();
				ArrayList<String> metrics=new ArrayList<String>();
								
				try {
					Get get;
					String metricsStr="";
					String dimensString="";
					String profileId=getFirstProfileId();
					
					
					
					get = analytics.data().ga().get("ga:" + profileId, // Table Id. ga: + profile id.
							startDateStr, // Start date.
							endDateStr, // End date.
							"ga:users,ga:newUsers");
					
					/*if (maxResults > 0)
						get.setMaxResults(maxResults);*/
					
					//if (dimensionCount > 0) get.setDimensions(dimensionStr.toString());
					//if (filterCount > 0) get.setFilters(filtersStr.toString());
					get.setDimensions("");
					GaData result=get.execute();
					int usersIndex=0, newUsersIndex=0;
					
					Object[][] data;
					
					data=new Object[result.getRows().size()][result.getColumnHeaders().size()];
					ArrayList<Object[]> table=new ArrayList<Object[]>();
					
					//Object[][] data=new Object[result.getRows().size()][columns.size()];
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
						
						if (name.equals("Users"))
						{
							usersIndex=counter;
						}
						
						else newUsersIndex=counter;
						
						counter++;						
					}
					
					List<List<String>> res=result.getRows();
					//List<Rows> rows=res.getRows();
					Object[][] full=new Object[2][2];
					for(List<String> row:res)
					{
						
						
						int users=Integer.valueOf(row.get(usersIndex));
						full[0][0]="New Users";
						full[0][1]=Integer.valueOf(row.get(newUsersIndex));
						full[1][0]="Returning Users";
						full[1][1]=(Integer)(users-(Integer)full[0][1]);						
					}
					
					data=new Object[2][columns.size()];
					
					for (i = 0; i < columns.size(); i++) 
					{
						if (columns.get(i).getColumnName().equals("Type"))
						{
							data[0][i]=full[0][0];
							data[1][i]=full[1][0];
						}
						
						else if (columns.get(i).getColumnName().equals("Quantity"))
						{
							data[0][i]=full[0][1];
							data[1][i]=full[1][1];
						}
					}
					
					return data;
					//data=buildResultset(result, columns);
				}
				
				catch(IOException e)
				{
					log.error("Error occurred when downloading data");
				}
				return null;
				
				
			}
		};
		
		return dtSet;
	}
	
	public AbstractDataSet Dashboard()
	{
		AbstractDataSet dtSet=new AbstractDataSet() {
			
			@Override
			public List<FilterMetaData> getFilters() {
				// TODO Auto-generated method stub
				ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
				
				fm.add(new FilterMetaData("Start Date", DataType.DATE, true, new FilterOperator[] {FilterOperator.EQUAL}));
				fm.add(new FilterMetaData("End Date", DataType.DATE, true, new FilterOperator[] {FilterOperator.EQUAL}));
				
				
				
				return fm;
			}
			
			@Override
			public String getDataSetName() {
				// TODO Auto-generated method stub
				return "Dashboard dataset";
			}
			
			@Override
			public List<ColumnMetaData> getColumns() {
				
				ArrayList<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
				
				columns.add(new ColumnMetaData("Date", DataType.DATE));
				columns.add(new ColumnMetaData("Users", DataType.INTEGER));
				columns.add(new ColumnMetaData("New Users", DataType.INTEGER));
				columns.add(new ColumnMetaData("Page Views", DataType.INTEGER));
				columns.add(new ColumnMetaData("Page Views Per Session", DataType.NUMERIC));
				columns.add(new ColumnMetaData("User Type", DataType.TEXT));
				columns.add(new ColumnMetaData("Sessions", DataType.INTEGER));
				columns.add(new ColumnMetaData("Landing Page Path", DataType.TEXT));
				columns.add(new ColumnMetaData("Entrances", DataType.INTEGER));
				columns.add(new ColumnMetaData("Channel Grouping", DataType.TEXT));
				columns.add(new ColumnMetaData("Country", DataType.TEXT));
				columns.add(new ColumnMetaData("Social Network", DataType.TEXT));
				columns.add(new ColumnMetaData("Browser", DataType.TEXT));
				columns.add(new ColumnMetaData("Browser Version", DataType.TEXT));
				columns.add(new ColumnMetaData("Operating System", DataType.TEXT));
				columns.add(new ColumnMetaData("Operating System Version", DataType.TEXT));
				columns.add(new ColumnMetaData("Bounces", DataType.INTEGER));
				//columns.add(new ColumnMetaData("Retained", DataType.INTEGER));
				
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
			public Object[][] execute (List<ColumnMetaData> columns, List<FilterData> filters) {
				
				HashMap<String, String> metricsHash=new HashMap<String, String>();
				HashMap<String, String> dimensionsHash=new HashMap<String, String>();
				
				
				metricsHash.put("ga:users", "Users");
				metricsHash.put("ga:newUsers", "New Users");
				metricsHash.put("ga:pageviews", "Page Views");
				metricsHash.put("ga:sessions", "Sessions");
				metricsHash.put("ga:entrances", "Entrances");
				metricsHash.put("ga:bounces", "Bounces");
				metricsHash.put("ga:pageviewsPerSession", "Page Views Per Session");
				
				dimensionsHash.put("ga:date", "Date");
				dimensionsHash.put("ga:userType", "User Type");
				dimensionsHash.put("ga:landingPagePath", "Landing Page Path");
				dimensionsHash.put("ga:channelGrouping", "Channel Grouping");
				dimensionsHash.put("ga:country", "Country");
				dimensionsHash.put("ga:socialNetwork", "Social Network");
				dimensionsHash.put("ga:browser", "Browser");
				dimensionsHash.put("ga:browserVersion", "Browser Version");
				dimensionsHash.put("ga:operatingSystem", "Operating System");
				dimensionsHash.put("ga:operatingSystemVersion", "Operating System Version");
				
				boolean containsRetained=false;
				
				int i, j;
				String startDateStr="";
				String endDateStr="";
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
					
					else if (columns.get(j).getColumnName().equals("Retained"))
					{
						containsRetained=true;
					}
				}
				
		
				if (metrics.isEmpty())
				{
					return null;
				}
				
				else if (metrics.size()+dimensions.size()>7)
				{
					log.info("You have selected more than 7 columns");
					return null;
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
					
					get = analytics.data().ga().get("ga:" + profileId, // Table Id. ga: + profile id.
							startDateStr, // Start date.
							endDateStr, // End date.
							metricsStr.toString());
					
					if (rowsLimit > 0)
						get.setMaxResults(rowsLimit);
					
					//if (dimensionCount > 0) get.setDimensions(dimensionStr.toString());
					//if (filterCount > 0) get.setFilters(filtersStr.toString());
					get.setDimensions(dimensString);
					GaData result=get.execute();
					GaData retained=new GaData();
					int sessionsIndex=0, bouncesIndex=0;
					if (containsRetained)
					{
						get.setMetrics("ga:sessions, ga:bounces");
						retained=get.execute();
						//int[] retains=new int[retained.getRows().size()];
						int counter=0;
						
						List<ColumnHeaders> cols=retained.getColumnHeaders();
						for(counter=0; counter<cols.size(); counter++)
						{
							if (cols.get(counter).getName().equals("ga:sessions"))
								sessionsIndex=counter;
							else if (cols.get(counter).getName().equals("ga:bounces")) bouncesIndex=counter;
						}
						
					}
					Object[][] data;
					if (containsRetained)
					{
						data=new Object[result.getRows().size()][result.getColumnHeaders().size()+1];
					}
					else
					{
						data=new Object[result.getRows().size()][result.getColumnHeaders().size()];
					}
					//Object[][] data=new Object[result.getRows().size()][columns.size()];
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
							if (columns.get(i).getColumnName().equals("Retained"))
							{
								int k;
								for (k=0; k<retained.getRows().size(); k++)
									data[k][i]=Integer.valueOf(result.getRows().get(k).get(sessionsIndex))-Integer.valueOf(result.getRows().get(k).get(bouncesIndex));
								
							}
							else if (columns.get(i).getColumnName().equals(name))
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
					//data=buildResultset(result, columns);
				}
				
				catch(IOException e)
				{
					throw new ThirdPartyException(e.getMessage());
				}
				//return null;
				
				
			}
		};
		
		return dtSet;
	}
	
	
	public AbstractDataSet AllColumns()
	{
		AbstractDataSet dtSet=new AbstractDataSet() {
			
			@Override
			public List<FilterMetaData> getFilters() {
				
				Boolean isTransformations = Boolean.valueOf((String)getAttribute("USEFORTRANSFORMATIONS"));
				List<String> fieldsAllowed = new ArrayList<String>();
				
				if (isTransformations)
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
							if ((isTransformations && fieldsAllowed.contains(YellowfinName)) || !isTransformations)
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
				
				Boolean isTransformations = Boolean.valueOf((String)getAttribute("USEFORTRANSFORMATIONS"));
				List<String> fieldsAllowed = new ArrayList<String>();
				
				if (isTransformations)
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
							if ((isTransformations && fieldsAllowed.contains(YellowfinName)) || !isTransformations)
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
				int i, j;
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
					
					
						if (rowsLimit>0)
							get.setMaxResults(rowsLimit);
					
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
					Boolean isTransformation = Boolean.valueOf((String)getAttribute("USEFORTRANSFORMATIONS"));
					if(isTransformation)
					{
						List<GaData> results = new ArrayList<GaData>();
						result=get.execute();
						int totalSize = 0;
						if (result.getRows()!=null && result.getRows().size()>0)
						{
							boolean stop = false;
							results.add(result);
							totalSize = totalSize + result.getRows().size();
							int c=1;
							get.setStartIndex(rowsLimit+1);
							if (result.size()<rowsLimit)
							{
								stop = true;
							}
							
							while(!stop)
							{
								result = get.execute();
								if (result!=null && result.getRows().size()>0)
								{
									results.add(result);			
									c++;
									get.setStartIndex(c*rowsLimit+1);
								}
								else
								{
									stop=true;
								}
								
							}
						}
					}
					else
					{
						List<GaData> results = new ArrayList<GaData>();
						result=get.execute();
						
						if (result.getRows()==null && result.getRows().size()>0)
						{
							
						}
						
					}
					
					Object[][] data=new Object[result.getRows().size()][result.getColumnHeaders().size()];
					
					
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
				fieldsAllowed.add("Visits");
				fieldsAllowed.add("Pages per Visit");
				fieldsAllowed.add("Sessions");
				fieldsAllowed.add("Bounce Rate");
				fieldsAllowed.add("New Visits");
				fieldsAllowed.add("Visits");
				fieldsAllowed.add("Unique Visitors");
				
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
		
		/*if (getAttribute("PRIVATE_KEY")==null){
			return false;
		}
		if (getAttribute("SVC_ACCOUNT_ID")==null){
			return false;
		}
		
		//API_KEY = (String) parameters.get("API_KEY");
		privKeyPEM = (String) getAttribute("PRIVATE_KEY");
		serviceAccountId = (String) getAttribute("SVC_ACCOUNT_ID");

		//tries to connect to the API server
		//if doesn't work, returns false
		if (getCredential() == null)
			return false;

		
		//if it works sets the variable values and return true*/
		return true;
	}
	
	private static boolean validParameterValues(){
		//return !(UtilString.isNullOrEmpty(API_KEY) || UtilString.isNullOrEmpty(privKeyPEM) || UtilString.isNullOrEmpty(serviceAccountId)); 
		return !(UtilString.isNullOrEmpty(privKeyPEM) || UtilString.isNullOrEmpty(serviceAccountId));
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
	private Analytics getAnalytics(){
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
		/*if (!validParameterValues() || getAnalytics() == null){
			return null;
		}
		
		Collection<AbstractDataSet> ds = getDataSets();
		
		if (error != null) {
			throw this.error;
		}
		
		return ds;*/
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

	private void cacheAllColumns() 
	{
		try 
		{
			List<Column> cols=getAnalytics().metadata().columns().list("ga").execute().getItems();
			
			int numberOfGoals = 0;
			
			
			Profiles pfls = getAnalytics().management().profiles().list("~all", "~all").execute();
			String accountId = null;
			String webPropertyId = null;
			String profileId = (String) getAttribute("PROFILEID");
			
			for (Profile p:pfls.getItems()) 
			{
				if(p.getId().equals((String) getAttribute("PROFILEID")))
				{
					accountId = p.getAccountId();
					webPropertyId = p.getWebPropertyId();
				}
			}
			
			if (accountId != null && webPropertyId != null)
			{
				com.google.api.services.analytics.model.Goals goals = getAnalytics().management().goals().list(accountId, webPropertyId, profileId).execute();
				numberOfGoals = goals.getItems().size();	
			}
			
			
			String colstoSave="[";
			for ( Column col:cols)
			{
				JSONObject column=new JSONObject();
				
				if (col.getAttributes().get("uiName").contains("Goal XX ") && numberOfGoals > 0)
				{
					int c;
					for (c=1; c<=numberOfGoals; c++)
					{
						String uiName=col.getAttributes().get("uiName").replace("XX", String.valueOf(c));
						String id= col.getId().replace("XX", String.valueOf(c));
						column.put("uiName", uiName);
						column.put("status", col.getAttributes().get("status"));
						column.put("dataType", col.getAttributes().get("dataType"));
						column.put("type", col.getAttributes().get("type"));
						column.put("id", id);
						colstoSave=colstoSave+column+", ";
					}
				}
				else 
				{
					column.put("uiName", col.getAttributes().get("uiName"));
					column.put("status", col.getAttributes().get("status"));
					column.put("dataType", col.getAttributes().get("dataType"));
					column.put("type", col.getAttributes().get("type"));
					column.put("id", col.getId());
					colstoSave=colstoSave+column+", ";
				}			
				
				
			}	
			
			colstoSave=colstoSave+"]";
			
			saveBlob("ALLCOLUMNSMETADATA", colstoSave.getBytes());
		} 
		
		catch (IOException e) 
		{
			log.error("Error occurred when updating the list of columns");
		}
		
		
	}

	private Object[][] buildResultset(GaData gaData, List<ColumnMetaData> columns) {
		if (gaData == null ||
				gaData.getRows() == null || gaData.getRows().size() == 0 ||
				gaData.getColumnHeaders() == null || gaData.getColumnHeaders().size() == 0)
			return null;
		
		Object[][] result = new Object[gaData.getRows().size()][gaData.getColumnHeaders().size()];
		
		//the order of the requested columns might be different from the order in the gaData, so, the result must be built correctly;
		ArrayList<Integer> colMap = new ArrayList<Integer>();
		int position;
		for (ColumnHeaders ch : gaData.getColumnHeaders()){
			position = 0;
			for (ColumnMetaData cm : columns){
				if (ch.getName().equals("ga:"+cm.getColumnName())){
					colMap.add(position);
					break;
				}
				position++;
				
			}
		}
		
		for (int i = 0 ; i < gaData.getRows().size() ; i++){
			for (int j = 0 ; j < gaData.getColumnHeaders().size() ; j++){
				//int position = 
				result[i][colMap.get(j)] = setDataType(columns.get(colMap.get(j)), gaData.getRows().get(i).get(j));
			}
		}

		return result;
	}
	
	private Object setDataType (ColumnMetaData columnMD, Object data){
		if (DataType.INTEGER.equals(columnMD.getColumnType())){
			data = new Long(data.toString());
		}else if (DataType.NUMERIC.equals(columnMD.getColumnType())){
			data = new BigDecimal(data.toString());
		}else if (DataType.DATE.equals(columnMD.getColumnType())){
			DateFormat format = new SimpleDateFormat("yyyy/mm/dd");
			try {
				data = format.parse(data.toString());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return data;
	}
	
	
	private String getFirstProfileId() throws IOException {
		String profileId = null;

		/*Analytics analytics = getAnalytics();
		// Query accounts collection.
		Accounts accounts = getAnalytics().management().accounts().list().execute();

		if (accounts.getItems().isEmpty()) {
			System.err.println("No accounts found");
		} else {
			String firstAccountId = accounts.getItems().get(0).getId();

			// Query webproperties collection.
			Webproperties webproperties =
					analytics.management().webproperties().list(firstAccountId).execute();

			if (webproperties.getItems().isEmpty()) {
				System.err.println("No Webproperties found");
			} else {
				String firstWebpropertyId = webproperties.getItems().get(0).getId();

				// Query profiles collection.
				Profiles profiles =
						analytics.management().profiles().list(firstAccountId, firstWebpropertyId).execute();

				if (profiles.getItems().isEmpty()) {
					System.err.println("No profiles found");
				} else {
					profileId = profiles.getItems().get(0).getId();
				}
			}
		}*/
		
		profileId=(String)getAttribute("PROFILEID");
		if (profileId==null || profileId=="")
		{
			log.error("No valid profile ID found");
		}
		return profileId;
	}

	/**
	 * Returns the top 25 organic search keywords and traffic source by visits. The Core Reporting API
	 * is used to retrieve this data.
	 *
	 * @return the response from the API.
	 * @throws IOException tf an API error occured.
	 */
	private GaData executeDataQuery(List<ColumnMetaData> fields, List<FilterData> filters, HashMap<String, Boolean> datasetColumnDimension, int maxResults) throws IOException {

		

		if (!validParameterValues()) return null; 


		// Set up and return Google Analytics API client.
		Analytics analytics = getAnalytics();

		String profileId = "";
		try {
			profileId = getFirstProfileId();
		} catch (IOException e) {
			e.printStackTrace(); 
		}
		
		StringBuilder filtersStr = new StringBuilder("");
		int filterCount = 0;
		String startDateStr = "";
		String endDateStr = "";
		for (FilterData filter : filters){
			if ("Start Date".equals(filter.getFilterName())){
				Date sDate = (Date) filter.getFilterValue();
				startDateStr = sDate.toString();
				startDateStr = startDateStr == null ? "today" : startDateStr;
			} else if ("End Date".equals(filter.getFilterName())){
				Date eDate = (Date) filter.getFilterValue();
				endDateStr = eDate.toString();
				endDateStr = endDateStr == null ? "today" : endDateStr;
			} else if (filter.getFilterValue() != null){
				if (filter.getFilterOperator().equals(FilterOperator.INLIST)){
					if (filter.getFilterValue() instanceof List<?>){
						ArrayList<String> filterValuesList = (ArrayList<String>)filter.getFilterValue();
						for (String item : filterValuesList){
							if (item.trim().length() == 0) continue;
							if (filtersStr.length() > 0) filtersStr.append(",");
							filtersStr.append("ga:"+filter.getFilterName()+"==" );
							filtersStr.append(item);
							filterCount++;
						}
					}
				} else {
					if (!"".equals(filter.getFilterValue())){
						filtersStr.append("ga:" + filter.getFilterName());
						if (filter.getFilterOperator().equals(FilterOperator.NOTEQUAL)) filtersStr.append("!=");
						else filtersStr.append("==");
						filtersStr.append(filter.getFilterValue());
						filterCount++;
					}
				}
			} else if (filter.getFilterMetaData().isMandatory()){
				throw new IOException("Mandatory filter not set: "+filter.getFilterName());
			}
		}
		//if the filter was not passed in the filters list
		if (UtilString.isNullOrEmpty(startDateStr)) startDateStr = "today";
		if (UtilString.isNullOrEmpty(endDateStr)) endDateStr = "today";
		
		StringBuilder metricsStr = new StringBuilder("");
		int metricCount = 0;
		StringBuilder dimensionStr = new StringBuilder("");
		int dimensionCount = 0;
		for (ColumnMetaData field : fields){
			if (datasetColumnDimension.get(field.getColumnName())){
				if (dimensionCount > 6) continue;
				if (dimensionCount > 0) dimensionStr.append(",");
				dimensionStr.append("ga:"+field.getColumnName());
				System.out.print("dimension: ga:"+field.getColumnName());
				if (field.getSelectedAggregation() != null) System.out.print(" ("+field.getSelectedAggregation()+")");
				System.out.println();
				dimensionCount++;
				
			} else {
				if (metricCount > 9) continue;
				if (metricCount > 0) metricsStr.append(",");
				metricsStr.append("ga:"+field.getColumnName());
				System.out.print("metric: ga:"+field.getColumnName());
				if (field.getSelectedAggregation() != null) System.out.print(" ("+field.getSelectedAggregation()+")");
				System.out.println();
				metricCount++;
			}
			
		}
		
		if (metricCount < 1) return null;
		Get get = analytics.data().ga().get("ga:" + profileId, // Table Id. ga: + profile id.
				startDateStr, // Start date.
				endDateStr, // End date.
				metricsStr.toString());
		
		if (rowsLimit > 0)
			get.setMaxResults(rowsLimit);
		
		if (dimensionCount > 0) get.setDimensions(dimensionStr.toString());
		if (filterCount > 0) get.setFilters(filtersStr.toString());
		//get.setMaxResults(25);
		
		return get.execute();
	}

	/**
	 * Prints the output from the Core Reporting API. The profile name is printed along with each
	 * column name and all the data in the rows.
	 *
	 * @param results data returned from the Core Reporting API.
	 */
	private static void printGaData(GaData results) {
		System.out.println("printing results for profile: " + results.getProfileInfo().getProfileName());

		if (results.getRows() == null || results.getRows().isEmpty()) {
			System.out.println("No results Found.");
		} else {

			// Print column headers.
			for (GaData.ColumnHeaders header : results.getColumnHeaders()) {
				System.out.printf("%30s", header.getName());
			}
			System.out.println();

			// Print actual data.
			for (List<String> row : results.getRows()) {
				for (String column : row) {
					System.out.printf("%30s", column);
				}
				System.out.println();
			}

			System.out.println();
		}
	}
	
	private static HashMap<String, String> getAllMetrics()
	{
		HashMap<String, String> metrics= new HashMap<String, String>();
		
		//Group User
		metrics.put("ga:users", "Users");
		metrics.put("ga:newUsers", "");
		metrics.put("ga:percentNewSessions", "");
		metrics.put("ga:sessionsPerUser", "");
		
		//Group Session
		metrics.put("ga:sessions", "");
		metrics.put("ga:bounces", "");
		metrics.put("ga:bounceRate", "");
		metrics.put("ga:sessionDuration", "");
		metrics.put("ga:avgSessionDuration", "");
		metrics.put("ga:hits", "");
		
		//Group Traffic Sources
		metrics.put("ga:organicSearches", "");
		
		//Group Adwords
		metrics.put("ga:impressions", "");
		metrics.put("ga:adClicks", "");
		metrics.put("ga:adCost", "");
		metrics.put("ga:CPM", "");
		metrics.put("ga:CPC", "");
		metrics.put("ga:CTR", "");
		metrics.put("ga:costPerTransaction", "");
		metrics.put("ga:costPerGoalConversion", "");
		metrics.put("ga:costPerConversion", "");
		metrics.put("ga:RPC", "");
		metrics.put("ga:ROAS", "");
		
		//Group Goal Conversions
		metrics.put("ga:goalXXStarts", "");
		metrics.put("ga:goalStartsAll", "");
		metrics.put("ga:goalXXCompletions", "");
		metrics.put("ga:goalCompletionsAll", "");
		metrics.put("ga:goalXXValue", "");
		metrics.put("ga:goalValueAll", "");
		metrics.put("ga:goalValuePerSession", "");
		metrics.put("ga:goalXXConversionRate", "");
		metrics.put("ga:goalConversionRateAll", "");
		metrics.put("ga:goalXXAbandons", "");
		metrics.put("ga:goalAbandonsAll", "");
		metrics.put("ga:goalXXAbandonRate", "");
		metrics.put("ga:goalAbandonRateAll", "");
		
		//Group Platform or Device
		
		
		//Group Geo Network
		
		
		//Group System
		
		
		//Group Social Activities
		metrics.put("ga:socialActivities", "");
		
		
		//Group Page Tracking
		metrics.put("ga:pageValue", "");
		metrics.put("ga:entrances", "");
		metrics.put("ga:entranceRate", "");
		metrics.put("ga:pageviews", "");
		metrics.put("ga:pageviewsPerSession", "");
		metrics.put("ga:uniquePageviews", "");
		metrics.put("ga:timeOnPage", "");
		metrics.put("ga:avgTimeOnPage", "");
		metrics.put("ga:exits", "");
		metrics.put("ga:exitRate", "");
		
		//Group Content Grouping
		metrics.put("ga:contentGroupUniqueViewsXX", "");
		
		//Group Internal Search
		metrics.put("ga:searchResultViews", "");
		metrics.put("ga:searchUniques", "");
		metrics.put("ga:avgSearchResultViews", "");
		metrics.put("ga:searchSessions", "");
		metrics.put("ga:percentSessionsWithSearch", "");
		metrics.put("ga:searchDepth", "");
		metrics.put("ga:avgSearchDepth", "");
		metrics.put("ga:searchRefinements", "");
		metrics.put("ga:percentSearchRefinements", "");
		metrics.put("ga:searchDuration", "");
		metrics.put("ga:avgSearchDuration", "");
		metrics.put("ga:searchExits", "");
		metrics.put("ga:searchExitRate", "");
		metrics.put("ga:searchGoalXXConversionRate", "");
		metrics.put("ga:searchGoalConversionRateAll", "");
		metrics.put("ga:goalValueAllPerSearch", "");
		
		//Group Site Speed
		metrics.put("ga:pageLoadTime", "");
		metrics.put("ga:pageLoadSample", "");
		metrics.put("ga:avgPageLoadTime", "");
		metrics.put("ga:domainLookupTime", "");
		metrics.put("ga:avgDomainLookupTime", "");
		metrics.put("ga:pageDownloadTime", "");
		metrics.put("ga:avgPageDownloadTime", "");
		metrics.put("ga:redirectionTime", "");
		metrics.put("ga:avgRedirectionTime", "");
		metrics.put("ga:serverConnectionTime", "");
		metrics.put("ga:avgServerConnectionTime", "");
		metrics.put("ga:serverResponseTime", "");
		metrics.put("ga:avgServerResponseTime", "");
		metrics.put("ga:speedMetricsSample", "");
		metrics.put("ga:domInteractiveTime", "");
		metrics.put("ga:avgDomInteractiveTime", "");
		metrics.put("ga:domContentLoadedTime", "");
		metrics.put("ga:avgDomContentLoadedTime", "");
		metrics.put("ga:domLatencyMetricsSample", "");
		
		//Group App Tracking
		metrics.put("ga:screenviews", "");
		metrics.put("ga:uniqueScreenviews", "");
		metrics.put("ga:screenviewsPerSession", "");
		metrics.put("ga:timeOnScreen", "");
		metrics.put("ga:avgScreenviewDuration", "");
		
		//Group Event Tracking
		metrics.put("ga:totalEvents", "");
		metrics.put("ga:uniqueEvents", "");
		metrics.put("ga:eventValue", "");
		metrics.put("ga:avgEventValue", "");
		metrics.put("ga:sessionsWithEvent", "");
		metrics.put("ga:eventsPerSessionWithEvent", "");
		
		//Group Ecommerce
		metrics.put("ga:transactions", "");
		metrics.put("ga:transactionsPerSession", "");
		metrics.put("ga:transactionRevenue", "");
		metrics.put("ga:revenuePerTransaction", "");
		metrics.put("ga:transactionRevenuePerSession", "");
		metrics.put("ga:transactionShipping", "");
		metrics.put("ga:transactionTax", "");
		metrics.put("ga:totalValue", "");
		metrics.put("ga:itemQuantity", "");
		metrics.put("ga:uniquePurchases", "");
		metrics.put("ga:revenuePerItem", "");
		metrics.put("ga:itemRevenue", "");
		metrics.put("ga:itemsPerPurchase", "");
		metrics.put("ga:localTransactionRevenue", "");
		metrics.put("ga:localTransactionShipping", "");
		metrics.put("ga:localTransactionTax", "");
		metrics.put("ga:localItemRevenue", "");
		metrics.put("ga:buyToDetailRate", "");
		metrics.put("ga:cartToDetailRate", "");
		metrics.put("ga:internalPromotionCTR", "");
		metrics.put("ga:internalPromotionClicks", "");
		metrics.put("ga:internalPromotionViews", "");
		metrics.put("ga:localProductRefundAmount", "");
		metrics.put("ga:localRefundAmount", "");
		metrics.put("ga:productAddsToCart", "");
		metrics.put("ga:productCheckouts", "");
		metrics.put("ga:productDetailViews", "");
		metrics.put("ga:productListCTR", "");
		metrics.put("ga:productListClicks", "");
		metrics.put("ga:productListViews", "");
		metrics.put("ga:productRefundAmount", "");
		metrics.put("ga:productRefunds", "");
		metrics.put("ga:productRemovesFromCart", "");
		metrics.put("ga:productRevenuePerPurchase", "");
		metrics.put("ga:quantityAddedToCart", "");
		metrics.put("ga:quantityCheckedOut", "");
		metrics.put("ga:quantityRefunded", "");
		metrics.put("ga:quantityRemovedFromCart", "");
		metrics.put("ga:refundAmount", "");
		metrics.put("ga:revenuePerUser", "");
		metrics.put("ga:totalRefunds", "");
		metrics.put("ga:transactionsPerUser", "");
		
		//Group Social Interactions
		metrics.put("ga:socialInteractions", "");
		metrics.put("ga:uniqueSocialInteractions", "");
		metrics.put("ga:socialInteractionsPerSession", "");
		
		//Group User Timings
		metrics.put("", "");
		//Group Exceptions
		//Group Content Experiments
		//Group Custom Variables or Columns
		//Group Time
		//Group DoubleClick Campaign Manager
		//Group Audience
		//Group Adsense
		//Group Channel Grouping
		//Group Related Products
		
		return metrics;
	}
	
	private static HashMap<String, String> getAllDimensions()
	{
		HashMap<String, String> dimensions= new HashMap<String, String>();
		
		//Group User
		dimensions.put("ga:userType", "User Type");
		dimensions.put("ga:sessionCount", "Session Count");
		dimensions.put("ga:daysSinceLastSession", "Days Since Last Session");
		dimensions.put("ga:userDefinedValue", "User Defined Value");
		
		//Group Session
		dimensions.put("ga:sessionDurationBucket", "Session Duration Bucket");
		
		//Traffic Sources
		dimensions.put("ga:referralPath", "Referral Path");
		dimensions.put("ga:fullReferrer", "Full Referrer");
		dimensions.put("ga:campaign", "Campaign");
		dimensions.put("ga:source", "Source");
		dimensions.put("ga:medium", "Medium");
		dimensions.put("ga:sourceMedium", "Source Medium");
		dimensions.put("ga:keyword", "Keyword");
		dimensions.put("ga:adContent", "Ad Content");
		dimensions.put("ga:socialNetwork", "Social Network");
		dimensions.put("ga:hasSocialSourceReferral", "Has Social Source Referral");
		dimensions.put("ga:campaignCode", "Campaign Code");
		
		//Group Adwords
		dimensions.put("ga:adGroup", "Ad Group");
		dimensions.put("ga:adSlot", "Ad Slot");
		dimensions.put("ga:adDistributionNetwork", "Ad Distribution Network");
		dimensions.put("ga:adMatchType", "Ad Match Type");
		dimensions.put("ga:adKeywordMatchType", "Ad Keyword Match Type");
		dimensions.put("ga:adMatchedQuery", "Ad Matched Query");
		dimensions.put("ga:adPlacementDomain", "Ad Placement Domain");
		dimensions.put("ga:adPlacementUrl", "Ad Placement URL");
		dimensions.put("ga:adFormat", "Ad Format");
		dimensions.put("ga:adTargetingType", "Ad Targeting Type");
		dimensions.put("ga:adTargetingOption", "Ad Targeting Option");
		dimensions.put("ga:adDisplayUrl", "Ad Display URL");
		dimensions.put("ga:adDestinationUrl", "Ad Destination URL");
		dimensions.put("ga:adwordsCustomerID", "Adwords Customer ID");
		dimensions.put("ga:adwordsCampaignID", "Adwords Campaign ID");
		dimensions.put("ga:adwordsAdGroupID", "Adwords Ad Group ID");
		dimensions.put("ga:adwordsCreativeID", "Adwords Creative ID");
		dimensions.put("ga:adwordsCriteriaID", "Adwords Criteria ID");
		dimensions.put("ga:adQueryWordCount", "Ad Query Word Count");
		dimensions.put("ga:isTrueViewVideoAd", "Is True View Video Ad");
		
		
		//Group Goal Conversions
		dimensions.put("ga:goalCompletionLocation", "Goal Completion Location");
		dimensions.put("ga:goalPreviousStep1", "Goal Previous Step 1");
		dimensions.put("ga:goalPreviousStep2", "Goal Previous Step 2");
		dimensions.put("ga:goalPreviousStep3", "Goal Previous Step 3");
		
		//Group Platform or Device
		dimensions.put("ga:browser", "Browser");
		dimensions.put("ga:browserVersion", "Browser Version");
		dimensions.put("ga:operatingSystem", "Operating System");
		dimensions.put("ga:operatingSystemVersion", "Operating System Version");
		dimensions.put("ga:mobileDeviceBranding", "Mobile Device Branding");
		dimensions.put("ga:mobileDeviceModel", "Mobile Device Model");
		dimensions.put("ga:mobileInputSelector", "Mobile Input Selector");
		dimensions.put("ga:mobileDeviceInfo", "Mobile Device Info");
		dimensions.put("ga:mobileDeviceMarketingName", "Mobile Device Marketing Name");
		dimensions.put("ga:deviceCategory", "Device Category");
		dimensions.put("ga:dataSource", "Data Source");
		
		//Group Geo Network
		dimensions.put("ga:continent", "Continent");
		dimensions.put("ga:subContinent", "Sub Continent");
		dimensions.put("ga:country", "Country");
		dimensions.put("ga:region", "Region");
		dimensions.put("ga:metro", "Metro");
		dimensions.put("ga:city", "City");
		dimensions.put("ga:latitude", "Latitude");
		dimensions.put("ga:longitude", "Longitude");
		dimensions.put("ga:networkDomain", "Network Domain");
		dimensions.put("ga:networkLocation", "Network Location");
		dimensions.put("ga:cityId", "City ID");
		dimensions.put("ga:countryIsoCode", "Country ISO Code");
		dimensions.put("ga:regionId", "Region ID");
		dimensions.put("ga:regionIsoCode", "Region ISO Code");
		dimensions.put("ga:subContinentCode", "Sub Continent Code");
		
		//Group System
		dimensions.put("ga:flashVersion", "Flash Version");
		dimensions.put("ga:javaEnabled", "Java Enabled");
		dimensions.put("ga:language", "Language");
		dimensions.put("ga:screenColors", "Screen Colors");
		dimensions.put("ga:sourcePropertyDisplayName", "Source Property Display name");
		dimensions.put("ga:sourcePropertyTrackingId", "Source Property Tracking ID");
		dimensions.put("ga:screenResolution", "Screen Resolution");
		
		//Group Social Activities
		dimensions.put("ga:socialActivityEndorsingUrl", "Social Activity Endorsing URL");
		dimensions.put("ga:socialActivityDisplayName", "Social Activity Display Name");
		dimensions.put("ga:socialActivityPost", "Social Activity Post");
		dimensions.put("ga:socialActivityTimestamp", "Social Activity Timestamp");
		dimensions.put("ga:socialActivityUserHandle", "Social Activity User Handle");
		dimensions.put("ga:socialActivityUserPhotoUrl", "Social Activity User Photo URL");
		dimensions.put("ga:socialActivityUserProfileUrl", "Social Activity User Profile URL");
		dimensions.put("ga:socialActivityContentUrl", "Social Activity Content URL");
		dimensions.put("ga:socialActivityTagsSummary", "Social Activity Tags Summary");
		dimensions.put("ga:socialActivityAction", "Social Activity Action");
		dimensions.put("ga:socialActivityNetworkAction", "Social Activity Network Action");
		
		//Group Page Tracking
		dimensions.put("ga:hostname", "Hostname");
		dimensions.put("ga:pagePath", "Page Path");
		dimensions.put("ga:pagePathLevel1", "Page Path Level 1");
		dimensions.put("ga:pagePathLevel2", "Page Path Level 2");
		dimensions.put("ga:pagePathLevel3", "Page Path Level 3");
		dimensions.put("ga:pagePathLevel4", "Page Path Level 4");
		dimensions.put("ga:pageTitle", "Page Title");
		dimensions.put("ga:landingPagePath", "Landing Page Path");
		dimensions.put("ga:secondPagePath", "Second Page Path");
		dimensions.put("ga:exitPagePath", "Exit Page Path");
		dimensions.put("ga:previousPagePath", "Previous Page Path");
		dimensions.put("ga:pageDepth", "Page Depth");
		
		//Group Content Grouping
		dimensions.put("ga:landingContentGroupXX", "Landing Content Group XX");
		dimensions.put("ga:previousContentGroupXX", "Previous Content Group XX");
		dimensions.put("ga:contentGroupXX", "Content Group XX");
		
		//Group Internal Search
		dimensions.put("ga:searchUsed", "Search Used");
		dimensions.put("ga:searchKeyword", "Search Keyword");
		dimensions.put("ga:searchKeywordRefinement", "Search Keyword Refinement");
		dimensions.put("ga:searchCategory", "Search Category");
		dimensions.put("ga:searchStartPage", "Search Start Page");
		dimensions.put("ga:searchDestinationPage", "Search Destination Page");
		dimensions.put("ga:searchAfterDestinationPage", "Search After Destination Page");
		
		//Group Site Speed
		
		//Group App Tracking
		dimensions.put("ga:appInstallerId", "App Installer ID");
		dimensions.put("ga:appVersion", "App Version");
		dimensions.put("ga:appName", "App Name");
		dimensions.put("ga:appId", "App ID");
		dimensions.put("ga:screenName", "Screen Name");
		dimensions.put("ga:screenDepth", "Screen Depth");
		dimensions.put("ga:landingScreenName", "Landing Screen Name");
		dimensions.put("ga:exitScreenName", "Exit Screen Name");
		
		//Group Event Tracking
		dimensions.put("ga:eventCategory", "Event Category");
		dimensions.put("ga:eventAction", "Event Action");
		dimensions.put("ga:eventLabel", "Event Label");
		
		//Group Ecommerce
		dimensions.put("ga:transactionId", "Transaction ID");
		dimensions.put("ga:affiliation", "Affiliation");
		dimensions.put("ga:sessionsToTransaction", "Session To Transaction");
		dimensions.put("ga:daysToTransaction", "Days To Transaction");
		dimensions.put("ga:productSku", "Product SKU");
		dimensions.put("ga:productName", "Product Name");
		dimensions.put("ga:productCategory", "Product Category");
		dimensions.put("ga:currencyCode", "Currency Code");
		dimensions.put("ga:checkoutOptions", "Check Out Options");
		dimensions.put("ga:internalPromotionCreative", "Internal Promotion Creative");
		dimensions.put("ga:internalPromotionId", "Internal Promotion ID");
		dimensions.put("ga:internalPromotionName", "Internal Promotion Name");
		dimensions.put("ga:internalPromotionPosition", "Internal Promotion Position");
		dimensions.put("ga:orderCouponCode", "Order Coupon Code");
		dimensions.put("ga:productBrand", "Product Brand");
		dimensions.put("ga:productCategoryHierarchy", "Product Category Hierarchy");
		dimensions.put("ga:productCategoryLevelXX", "Product Category Level XX");
		dimensions.put("ga:productCouponCode", "Product Coupon Code");
		dimensions.put("ga:productListName", "Product List Name");
		dimensions.put("ga:productListPosition", "Product List Position");
		dimensions.put("ga:productVariant", "Product Variant");
		dimensions.put("ga:shoppingStage", "Shopping Stage");
		
		//Group Social Interactions
		dimensions.put("ga:socialInteractionNetwork", "Social Interaction Network");
		dimensions.put("ga:socialInteractionAction", "Social Interaction Action");
		dimensions.put("ga:socialInteractionNetworkAction", "Social Interaction Network Action");
		dimensions.put("ga:socialInteractionTarget", "Social Interaction Target");
		dimensions.put("ga:socialEngagementType", "Social Engagement Type");

		
		//Group User Timings
		dimensions.put("ga:userTimingCategory", "User Timing Category");
		dimensions.put("ga:userTimingLabel", "User Timing Label");
		dimensions.put("ga:userTimingVariable", "User Timing Variable");
		
		
		//Group Exceptions
		dimensions.put("ga:exceptionDescription", "Exception Description");
		
		//Group Content Experiments
		dimensions.put("ga:experimentId", "Experiment ID");
		dimensions.put("ga:experimentVariant", "Experiment Variant");
		
		//Group Custom Variables or Columns
		dimensions.put("ga:dimensionXX", "Dimension XX");
		dimensions.put("ga:customVarNameXX", "Custom Var Name XX");
		dimensions.put("ga:customVarValueXX", "Custom Var Value XX");
		
		//Group Time
		dimensions.put("ga:date", "Date");
		dimensions.put("ga:year", "Year");
		dimensions.put("ga:month", "Month");
		dimensions.put("ga:week", "Week");
		dimensions.put("ga:day", "Day");
		dimensions.put("ga:hour", "Hour");
		dimensions.put("ga:minute", "Minute");
		dimensions.put("ga:nthMonth", "Nth Month");
		dimensions.put("ga:nthWeek", "Nth Week");
		dimensions.put("ga:nthDay", "Nth Day");
		dimensions.put("ga:nthMinute", "Nth Minute");
		dimensions.put("ga:dayOfWeek", "Day Of Week");
		dimensions.put("ga:dayOfWeekName", "Day Of Week Name");
		dimensions.put("ga:dateHour", "Date Hour");
		dimensions.put("ga:yearMonth", "Year Month");
		dimensions.put("ga:yearWeek", "Year Week");
		dimensions.put("ga:isoWeek", "ISO Week");
		dimensions.put("ga:isoYear", "ISO Year");
		dimensions.put("ga:isoYearIsoWeek", "ISO Year ISO Week");
		dimensions.put("ga:nthHour", "Nth Hour");
		
		
		//Group DoubleClick Campaign Manager
		dimensions.put("ga:dcmClickAd", "DCM Click Ad");
		dimensions.put("ga:dcmClickAdId", "DCM Click Ad ID");
		dimensions.put("ga:dcmClickAdType", "DCM Click Ad Type");
		dimensions.put("ga:dcmClickAdTypeId", "DCM Click Ad Type ID");
		dimensions.put("ga:dcmClickAdvertiser", "DCM Click Advertiser");
		dimensions.put("ga:dcmClickAdvertiserId", "DCM Click Advertiser ID");
		dimensions.put("ga:dcmClickCampaign", "DCM Click Campaign");
		dimensions.put("ga:dcmClickCampaignId", "DCM Click Campaign ID");
		dimensions.put("ga:dcmClickCreativeId", "DCM Click Creative ID");
		dimensions.put("ga:dcmClickCreative", "DCM Click Creative");
		dimensions.put("ga:dcmClickRenderingId", "DCM Click Rendering ID");
		dimensions.put("ga:dcmClickCreativeType", "DCM Click Creative Type");
		dimensions.put("ga:dcmClickCreativeTypeId", "DCM Click Creative Type ID");
		dimensions.put("ga:dcmClickCreativeVersion", "DCm Click Creative Version");
		dimensions.put("ga:dcmClickSite", "DCM Click Site");
		dimensions.put("ga:dcmClickSiteId", "DCM Click Site ID");
		dimensions.put("ga:dcmClickSitePlacement", "DCM Click Site Placement");
		dimensions.put("ga:dcmClickSitePlacementId", "DCM Click Site Placement ID");
		dimensions.put("ga:dcmClickSpotId", "DCM Click Spot ID");
		dimensions.put("ga:dcmFloodlightActivity", "DCM Floodlight Activity");
		dimensions.put("ga:dcmFloodlightActivityAndGroup", "DCM Floodlight Activity and Group");
		dimensions.put("ga:dcmFloodlightActivityGroup", "DCM Floodlight Activity Group");
		dimensions.put("ga:dcmFloodlightActivityGroupId", "DCM Floodlight Activity Group ID");
		dimensions.put("ga:dcmFloodlightActivityId", "DCM Floodlight Activity ID");
		dimensions.put("ga:dcmFloodlightAdvertiserId", "DCM Floodlight Advertiser ID");
		dimensions.put("ga:dcmFloodlightSpotId", "DCM Floodlight Spot ID");
		dimensions.put("ga:dcmLastEventAd", "DCM Last Event Ad");
		dimensions.put("ga:dcmLastEventAdId", "DCM Last Event Ad ID");
		dimensions.put("ga:dcmLastEventAdType", "DCM Last Event Ad Type");
		dimensions.put("ga:dcmLastEventAdTypeId", "DCM Last Event Type ID");
		dimensions.put("ga:dcmLastEventAdvertiser", "DCM Last Event Advertiser");
		dimensions.put("ga:dcmLastEventAdvertiserId", "DCM Last Event Advertiser ID");
		dimensions.put("ga:dcmLastEventAttributionType", "DCM Last Event Attribution Type");
		dimensions.put("ga:dcmLastEventCampaign", "DCM Last Event Campaign");
		dimensions.put("ga:dcmLastEventCampaignId", "DCM Last Event Campaign ID");
		dimensions.put("ga:dcmLastEventCreativeId", "DCM Last Event Creative ID");
		dimensions.put("ga:dcmLastEventCreative", "DCM Last Event Creative");
		dimensions.put("ga:dcmLastEventRenderingId", "DCM Last Event Rendering ID");
		dimensions.put("ga:dcmLastEventCreativeType", "DCM Last Event Creative Type");
		dimensions.put("ga:dcmLastEventCreativeTypeId", "DCM Last Event Creative Type ID");
		dimensions.put("ga:dcmLastEventCreativeVersion", "DCM Last Event Creative Version");
		dimensions.put("ga:dcmLastEventSite", "DCM Last Event Site");
		dimensions.put("ga:dcmLastEventSiteId", "DCM Last Event Site ID");
		dimensions.put("ga:dcmLastEventSitePlacement", "DCM Last Event Site Placement");
		dimensions.put("ga:dcmLastEventSitePlacementId", "DCM Last Event Placement ID");
		dimensions.put("ga:dcmLastEventSpotId", "DCM Last Event Spot ID");
		
		
		//Group Audience
		dimensions.put("ga:userAgeBracket", "User Age Bracket");
		dimensions.put("ga:userGender", "User Gender");
		dimensions.put("ga:interestOtherCategory", "Interest Other Category");
		dimensions.put("ga:interestAffinityCategory", "Interest Affinity Category");
		dimensions.put("ga:interestInMarketCategory", "Interest In Market Category");
		
		//Group Adsense
		
		//Group Channel Grouping
		dimensions.put("ga:channelGrouping", "Channel Grouping");
		
		//Group Related Products
		dimensions.put("ga:correlationModelId", "Correlation Model ID");
		dimensions.put("ga:queryProductId", "Query Product ID");
		dimensions.put("ga:queryProductName", "Query Product Name");
		dimensions.put("ga:queryProductVariation", "Query Product Variation");
		dimensions.put("ga:relatedProductId", "Related Product ID");
		dimensions.put("ga:relatedProductName", "Related Product Name");
		dimensions.put("ga:relatedProductVariation", "Related Product Variation");
		
		return dimensions;
	}
	private static final HashMap<String, String> allowedFields = new HashMap<String, String>();
	private void buildAllowedFields(){
		if (allowedFields.size() > 0 ) return;
		
		allowedFields.put("userType", 				"User");
		allowedFields.put("sessionCount", 			"User");
		allowedFields.put("daysSinceLastSession", 	"User");
		allowedFields.put("userDefinedValue", 		"User");
		//
		allowedFields.put("users", "User");
		allowedFields.put("newUsers", "User");
		allowedFields.put("percentNewSessions", "User");
		allowedFields.put("sessionsPerUser", "User");
//		allowedFields.put("", "User");
//		allowedFields.put("", "User");
//		allowedFields.put("", "User");
//		allowedFields.put("", "User");
//		allowedFields.put("", "User");
//		allowedFields.put("", "User");
//		allowedFields.put("", "User");
//		allowedFields.put("", "User");
//		allowedFields.put("", "User");


		allowedFields.put("sessionDurationBucket", 	"Session");
		//
		allowedFields.put("sessions", 			"Session");
		allowedFields.put("bounces", 	"Session");
		allowedFields.put("bounceRate", 		"Session");
		allowedFields.put("sessionDuration", "Session");
		allowedFields.put("avgSessionDuration", "Session");
		allowedFields.put("hits", "Session");
//		allowedFields.put("", "Session");
//		allowedFields.put("", "Session");
//		allowedFields.put("", "Session");
//		allowedFields.put("", "Session");
//		allowedFields.put("", "Session");
//		allowedFields.put("", "Session");
//		allowedFields.put("", "Session");
//		allowedFields.put("", "Session");
//		allowedFields.put("", "Session");
//		allowedFields.put("", "Session");
		
		
		

		allowedFields.put("referralPath", 	"Traffic Sources");
		allowedFields.put("fullReferrer", 			"Traffic Sources");
		allowedFields.put("campaign", 	"Traffic Sources");
		allowedFields.put("source", 		"Traffic Sources");
		allowedFields.put("medium", "Traffic Sources");
		allowedFields.put("sourceMedium", "Traffic Sources");
		allowedFields.put("keyword", "Traffic Sources");
		//ga:adContent
		//ga:socialNetwork
		//ga:hasSocialSourceReferral
		//ga:campaignCode
		allowedFields.put("organicSearches", "Traffic Sources");
//		allowedFields.put("", "Traffic Sources");
//		allowedFields.put("", "Traffic Sources");
//		allowedFields.put("", "Traffic Sources");
//		allowedFields.put("", "Traffic Sources");
//		allowedFields.put("", "Traffic Sources");
//		allowedFields.put("", "Traffic Sources");
//		allowedFields.put("", "Traffic Sources");
//		allowedFields.put("", "Traffic Sources");
//		allowedFields.put("", "Traffic Sources");
		

		allowedFields.put("adGroup", 				"Adwords");
		allowedFields.put("adSlot", 			"Adwords");
		allowedFields.put("adSlotPosition", 	"Adwords");
		allowedFields.put("adDistributionNetwork", 		"Adwords");
		allowedFields.put("adMatchType", "Adwords");
		allowedFields.put("adKeywordMatchType", "Adwords");
		allowedFields.put("adMatchedQuery", "Adwords");
//		ga:adPlacementDomain
//		ga:adPlacementUrl
//		ga:adFormat
//		ga:adTargetingType
//		ga:adTargetingOption
//		ga:adDisplayUrl
//		ga:adDestinationUrl
//		ga:adwordsCustomerID
//		ga:adwordsCampaignID
//		ga:adwordsAdGroupID
//		ga:adwordsCreativeID
//		ga:adwordsCriteriaID
//		ga:isTrueViewVideoAd
		allowedFields.put("impressions", "Adwords");
		allowedFields.put("adClicks", "Adwords");
		allowedFields.put("adCost", "Adwords");
		allowedFields.put("CPM", "Adwords");
		allowedFields.put("CPC", "Adwords");
		allowedFields.put("CTR", "Adwords");
		allowedFields.put("costPerTransaction", "Adwords");
		allowedFields.put("costPerGoalConversion", "Adwords");
		allowedFields.put("costPerConversion", "Adwords");
		allowedFields.put("RPC", "Adwords");
//		ga:ROAS
		
		
		
		allowedFields.put("goalCompletionLocation", 				"Goal Conversions");
		allowedFields.put("goalPreviousStep1", 			"Goal Conversions");
		allowedFields.put("goalPreviousStep2", 	"Goal Conversions");
		allowedFields.put("goalPreviousStep3", 		"Goal Conversions");
		//
		allowedFields.put("goalXXStarts", "Goal Conversions");
		allowedFields.put("goalStartsAll", "Goal Conversions");
		allowedFields.put("goalXXCompletions", "Goal Conversions");
		allowedFields.put("goalCompletionsAll", "Goal Conversions");
		allowedFields.put("goalXXValue", "Goal Conversions");
		allowedFields.put("goalValueAll", "Goal Conversions");
		allowedFields.put("goalValuePerSession", "Goal Conversions");
		allowedFields.put("goalXXConversionRate", "Goal Conversions");
		allowedFields.put("goalConversionRateAll", "Goal Conversions");
		allowedFields.put("goalXXAbandons", "Goal Conversions");
//		allowedFields.put("", "Goal Conversions");
//		allowedFields.put("", "Goal Conversions");
//		allowedFields.put("", "Goal Conversions");
//		ga:goalAbandonsAll
//		ga:goalXXAbandonRate
//		ga:goalAbandonRateAll
		
		
		
		
		allowedFields.put("browser", 				"Platform or Device");
		allowedFields.put("browserVersion", 			"Platform or Device");
		allowedFields.put("operatingSystem", 	"Platform or Device");
		allowedFields.put("operatingSystemVersion", 		"Platform or Device");
		allowedFields.put("mobileDeviceBranding", "Platform or Device");
		allowedFields.put("mobileDeviceModel", "Platform or Device");
		allowedFields.put("mobileInputSelector", "Platform or Device");
//		ga:mobileDeviceInfo
//		ga:mobileDeviceMarketingName
//		ga:deviceCategory
		allowedFields.put("", "Platform or Device");
		allowedFields.put("", "Platform or Device");
		allowedFields.put("", "Platform or Device");
		allowedFields.put("", "Platform or Device");
		allowedFields.put("", "Platform or Device");
		allowedFields.put("", "Platform or Device");
		allowedFields.put("", "Platform or Device");
		allowedFields.put("", "Platform or Device");
		allowedFields.put("", "Platform or Device");
		allowedFields.put("", "Platform or Device");
			
		

		allowedFields.put("continent", 				"Geo Network");
		allowedFields.put("subContinent", 			"Geo Network");
		allowedFields.put("country", 	"Geo Network");
		allowedFields.put("region", 		"Geo Network");
		allowedFields.put("metro", "Geo Network");
		allowedFields.put("city", "Geo Network");
		allowedFields.put("latitude", "Geo Network");
//		ga:longitude
//		ga:networkDomain
//		ga:networkLocation
//		ga:cityId
//		ga:countryIsoCode
//		ga:regionId
//		ga:regionIsoCode
//		ga:subContinentCode
		allowedFields.put("", "Geo Network");
		allowedFields.put("", "Geo Network");
		allowedFields.put("", "Geo Network");
		allowedFields.put("", "Geo Network");
		allowedFields.put("", "Geo Network");
		allowedFields.put("", "Geo Network");
		allowedFields.put("", "Geo Network");
		allowedFields.put("", "Geo Network");
		allowedFields.put("", "Geo Network");
		allowedFields.put("", "Geo Network");
		

		allowedFields.put("flashVersion", 				"System");
		allowedFields.put("javaEnabled", 			"System");
		allowedFields.put("language", 	"System");
		allowedFields.put("screenColors", 		"System");
		allowedFields.put("sourcePropertyDisplayName", "System");
		allowedFields.put("sourcePropertyTrackingId", "System");
		allowedFields.put("screenResolution", "System");
		allowedFields.put("", "System");
		allowedFields.put("", "System");
		allowedFields.put("", "System");
		allowedFields.put("", "System");
		allowedFields.put("", "System");
		allowedFields.put("", "System");
		allowedFields.put("", "System");
		allowedFields.put("", "System");
		allowedFields.put("", "System");
		allowedFields.put("", "System");
		
		

		allowedFields.put("socialActivityEndorsingUrl", 				"Social Activities");
		allowedFields.put("socialActivityDisplayName", 			"Social Activities");
		allowedFields.put("socialActivityPost", 	"Social Activities");
		allowedFields.put("socialActivityTimestamp", 		"Social Activities");
		allowedFields.put("socialActivityUserHandle", "Social Activities");
		allowedFields.put("socialActivityUserPhotoUrl", "Social Activities");
		allowedFields.put("socialActivityUserProfileUrl", "Social Activities");
//		ga:socialActivityContentUrl
//		ga:socialActivityTagsSummary
//		ga:socialActivityAction
//		ga:socialActivityNetworkAction
		allowedFields.put("socialActivities", "Social Activities");
//		allowedFields.put("", "Social Activities");
//		allowedFields.put("", "Social Activities");
//		allowedFields.put("", "Social Activities");
//		allowedFields.put("", "Social Activities");
//		allowedFields.put("", "Social Activities");
//		allowedFields.put("", "Social Activities");
//		allowedFields.put("", "Social Activities");
//		allowedFields.put("", "Social Activities");
//		allowedFields.put("", "Social Activities");
		
		
				
		

		allowedFields.put("hostname", 				"Page Tracking");
		allowedFields.put("pagePath", 			"Page Tracking");
		allowedFields.put("pagePathLevel1", 	"Page Tracking");
		allowedFields.put("pagePathLevel2", 		"Page Tracking");
		allowedFields.put("pagePathLevel3", "Page Tracking");
		allowedFields.put("pagePathLevel4", "Page Tracking");
		allowedFields.put("pageTitle", "Page Tracking");
//		ga:landingPagePath
//		ga:secondPagePath
//		ga:exitPagePath
//		ga:previousPagePath
//		ga:nextPagePath
//		ga:pageDepth
		allowedFields.put("pageValue", "Page Tracking");
		allowedFields.put("entrances", "Page Tracking");
		allowedFields.put("entranceRate", "Page Tracking");
		allowedFields.put("pageviews", "Page Tracking");
		allowedFields.put("pageviewsPerSession", "Page Tracking");
		allowedFields.put("uniquePageviews", "Page Tracking");
		allowedFields.put("timeOnPage", "Page Tracking");
		allowedFields.put("avgTimeOnPage", "Page Tracking");
		allowedFields.put("exits", "Page Tracking");
		allowedFields.put("exitRate", "Page Tracking");
		
		
				
		
		allowedFields.put("searchUsed", 				"Internal Search");
		allowedFields.put("searchKeyword", 			"Internal Search");
		allowedFields.put("searchKeywordRefinement", 	"Internal Search");
		allowedFields.put("searchCategory", 		"Internal Search");
		allowedFields.put("searchStartPage", "Internal Search");
		allowedFields.put("searchDestinationPage", "Internal Search");
		allowedFields.put("searchAfterDestinationPage", "Internal Search");
		//
		allowedFields.put("searchResultViews", "Internal Search");
		allowedFields.put("searchUniques", "Internal Search");
		allowedFields.put("avgSearchResultViews", "Internal Search");
		allowedFields.put("searchSessions", "Internal Search");
		allowedFields.put("percentSessionsWithSearch", "Internal Search");
		allowedFields.put("searchDepth", "Internal Search");
		allowedFields.put("avgSearchDepth", "Internal Search");
		allowedFields.put("searchRefinements", "Internal Search");
		allowedFields.put("percentSearchRefinements", "Internal Search");
		allowedFields.put("searchDuration", "Internal Search");
//		ga:avgSearchDuration
//		ga:searchExits
//		ga:searchExitRate
//		ga:searchGoalXXConversionRate
//		ga:searchGoalConversionRateAll
//		ga:goalValueAllPerSearch
		
		
		//metrics only
		allowedFields.put("pageLoadTime", "Site Speed");
		allowedFields.put("pageLoadSample", "Site Speed");
		allowedFields.put("avgPageLoadTime", "Site Speed");
		allowedFields.put("domainLookupTime", "Site Speed");
		allowedFields.put("avgDomainLookupTime", "Site Speed");
		allowedFields.put("pageDownloadTime", "Site Speed");
		allowedFields.put("avgPageDownloadTime", "Site Speed");
		allowedFields.put("redirectionTime", "Site Speed");
		allowedFields.put("avgRedirectionTime", "Site Speed");
		allowedFields.put("serverConnectionTime", "Site Speed");
		
//		allowedFields.put("", 				"Site Speed");
//		allowedFields.put("", 			"Site Speed");
//		allowedFields.put("", 	"Site Speed");
//		allowedFields.put("", 		"Site Speed");
//		allowedFields.put("", "Site Speed");
//		allowedFields.put("", "Site Speed");
//		allowedFields.put("", "Site Speed");
//		ga:avgServerConnectionTime
//		ga:serverResponseTime
//		ga:avgServerResponseTime
//		ga:speedMetricsSample
//		ga:domInteractiveTime
//		ga:avgDomInteractiveTime
//		ga:domContentLoadedTime
//		ga:avgDomContentLoadedTime
//		ga:domLatencyMetricsSample
		
		
		

		allowedFields.put("appInstallerId", 				"App Tracking");
		allowedFields.put("appVersion", 			"App Tracking");
		allowedFields.put("appName", 	"App Tracking");
		allowedFields.put("appId", 		"App Tracking");
		allowedFields.put("screenName", "App Tracking");
		allowedFields.put("screenDepth", "App Tracking");
		allowedFields.put("landingScreenName", "App Tracking");
		//ga:exitScreenName
		allowedFields.put("screenviews", "App Tracking");
		allowedFields.put("uniqueScreenviews", "App Tracking");
		allowedFields.put("screenviewsPerSession", "App Tracking");
		allowedFields.put("timeOnScreen", "App Tracking");
		allowedFields.put("avgScreenviewDuration", "App Tracking");
//		allowedFields.put("", "App Tracking");
//		allowedFields.put("", "App Tracking");
//		allowedFields.put("", "App Tracking");
//		allowedFields.put("", "App Tracking");
//		allowedFields.put("", "App Tracking");
		
		
				

		allowedFields.put("eventCategory", 				"Event Tracking");
		allowedFields.put("eventAction", 			"Event Tracking");
		allowedFields.put("eventLabel", 	"Event Tracking");
		allowedFields.put("", 		"Event Tracking");
		//
		allowedFields.put("totalEvents", "Event Tracking");
		allowedFields.put("uniqueEvents", "Event Tracking");
		allowedFields.put("eventValue", "Event Tracking");
		allowedFields.put("avgEventValue", "Event Tracking");
		allowedFields.put("sessionsWithEvent", "Event Tracking");
		allowedFields.put("eventsPerSessionWithEvent", "Event Tracking");
//		allowedFields.put("", "Event Tracking");
//		allowedFields.put("", "Event Tracking");
//		allowedFields.put("", "Event Tracking");
//		allowedFields.put("", "Event Tracking");
//		allowedFields.put("", "Event Tracking");
//		allowedFields.put("", "Event Tracking");
//		allowedFields.put("", "Event Tracking");
		
		
		
		

		allowedFields.put("itemsPerPurchase", 				"Ecommerce");
		allowedFields.put("affiliation", 			"Ecommerce");
		allowedFields.put("sessionsToTransaction", 	"Ecommerce");
		allowedFields.put("daysToTransaction", 		"Ecommerce");
		allowedFields.put("productSku", "Ecommerce");
		allowedFields.put("productName", "Ecommerce");
		allowedFields.put("productCategory", "Ecommerce");
//		ga:transactionId
//		ga:revenuePerItem
//		ga:itemRevenue
//		ga:localTransactionRevenue
//		ga:localTransactionShipping
//		ga:localTransactionTax
//		ga:localItemRevenue
//		ga:buyToDetailRate
//		ga:cartToDetailRate
//		ga:internalPromotionCTR
//		ga:internalPromotionClicks
//		ga:internalPromotionViews
//		ga:localProductRefundAmount
//		ga:localRefundAmount
//		ga:productAddsToCart
//		ga:productCheckouts
//		ga:productDetailViews
//		ga:productListCTR
//		ga:productListClicks
//		ga:productListViews
//		ga:productRefundAmount
//		ga:productRefunds
//		ga:productRemovesFromCart
//		ga:productRevenuePerPurchase
//		ga:quantityAddedToCart
//		ga:quantityCheckedOut
//		ga:quantityRefunded
//		ga:quantityRemovedFromCart
//		ga:refundAmount
//		ga:revenuePerUser
//		ga:totalRefunds
//		ga:transactionsPerUser

		allowedFields.put("transactions", "Ecommerce");
		allowedFields.put("transactionsPerSession", "Ecommerce");
		allowedFields.put("transactionRevenue", "Ecommerce");
		allowedFields.put("revenuePerTransaction", "Ecommerce");
		allowedFields.put("transactionRevenuePerSession", "Ecommerce");
		allowedFields.put("transactionShipping", "Ecommerce");
		allowedFields.put("transactionTax", "Ecommerce");
		allowedFields.put("totalValue", "Ecommerce");
		allowedFields.put("itemQuantity", "Ecommerce");
		allowedFields.put("uniquePurchases", "Ecommerce");
//		ga:revenuePerItem
//		ga:itemRevenue
//		ga:itemsPerPurchase
//		ga:localTransactionRevenue
//		ga:localTransactionShipping
//		ga:localTransactionTax
//		ga:localItemRevenue
//		ga:buyToDetailRate
//		ga:cartToDetailRate
//		ga:internalPromotionCTR
//		ga:internalPromotionClicks
//		ga:internalPromotionViews
//		ga:localProductRefundAmount
//		ga:localRefundAmount
//		ga:productAddsToCart
//		ga:productCheckouts
//		ga:productDetailViews
//		ga:productListCTR
//		ga:productListClicks
//		ga:productListViews
//		ga:productRefundAmount
//		ga:productRefunds
//		ga:productRemovesFromCart
//		ga:produSocial Activities
//		ga:socialActivities
		
		

		allowedFields.put("socialInteractionNetwork", 				"Social Interactions");
		allowedFields.put("socialInteractionAction", 			"Social Interactions");
		allowedFields.put("socialInteractionNetworkAction", 	"Social Interactions");
		allowedFields.put("socialInteractionTarget", 		"Social Interactions");
		allowedFields.put("socialEngagementType", "Social Interactions");
		//
		allowedFields.put("socialInteractions", "Social Interactions");
		allowedFields.put("uniqueSocialInteractions", "Social Interactions");
		allowedFields.put("socialInteractionsPerSession", "Social Interactions");
//		allowedFields.put("", "Social Interactions");
//		allowedFields.put("", "Social Interactions");
//		allowedFields.put("", "Social Interactions");
//		allowedFields.put("", "Social Interactions");
//		allowedFields.put("", "Social Interactions");
//		allowedFields.put("", "Social Interactions");
//		allowedFields.put("", "Social Interactions");
//		allowedFields.put("", "Social Interactions");
//		allowedFields.put("", "Social Interactions");
		

		allowedFields.put("userTimingCategory", 				"User Timings");
		allowedFields.put("userTimingLabel", 			"User Timings");
		allowedFields.put("userTimingVariable", 	"User Timings");
		//
		allowedFields.put("userTimingValue", 		"User Timings");
		allowedFields.put("userTimingSample", "User Timings");
		allowedFields.put("avgUserTimingValue", "User Timings");
//		allowedFields.put("", "User Timings");
//		allowedFields.put("", "User Timings");
//		allowedFields.put("", "User Timings");
//		allowedFields.put("", "User Timings");
//		allowedFields.put("", "User Timings");
//		allowedFields.put("", "User Timings");
//		allowedFields.put("", "User Timings");
//		allowedFields.put("", "User Timings");
//		allowedFields.put("", "User Timings");
//		allowedFields.put("", "User Timings");
//		allowedFields.put("", "User Timings");
		
		
		
		allowedFields.put("exceptionDescription", 				"Exceptions");
		//
		allowedFields.put("exceptions", 			"Exceptions");
		allowedFields.put("exceptionsPerScreenview", 	"Exceptions");
		allowedFields.put("fatalExceptions", 		"Exceptions");
		allowedFields.put("fatalExceptionsPerScreenview", "Exceptions");
//		allowedFields.put("", "Exceptions");
//		allowedFields.put("", "Exceptions");
//		allowedFields.put("", "Exceptions");
//		allowedFields.put("", "Exceptions");
//		allowedFields.put("", "Exceptions");
//		allowedFields.put("", "Exceptions");
//		allowedFields.put("", "Exceptions");
//		allowedFields.put("", "Exceptions");
//		allowedFields.put("", "Exceptions");
//		allowedFields.put("", "Exceptions");
//		allowedFields.put("", "Exceptions");
//		allowedFields.put("", "Exceptions");
		

		//only dimensions
		allowedFields.put("experimentId", 				"Content Experiments");
		allowedFields.put("experimentVariant", 			"Content Experiments");
		allowedFields.put("", 	"Content Experiments");
		allowedFields.put("", 		"Content Experiments");
//		allowedFields.put("", "Content Experiments");
//		allowedFields.put("", "Content Experiments");
//		allowedFields.put("", "Content Experiments");
//		allowedFields.put("", "Content Experiments");
//		allowedFields.put("", "Content Experiments");
//		allowedFields.put("", "Content Experiments");
//		allowedFields.put("", "Content Experiments");
//		allowedFields.put("", "Content Experiments");
//		allowedFields.put("", "Content Experiments");
//		allowedFields.put("", "Content Experiments");
//		allowedFields.put("", "Content Experiments");
//		allowedFields.put("", "Content Experiments");
//		allowedFields.put("", "Content Experiments");
		

		allowedFields.put("dimensionXX", 				"Custom Variables or Columns");
		allowedFields.put("customVarNameXX", 			"Custom Variables or Columns");
		allowedFields.put("customVarValueXX", 	"Custom Variables or Columns");
		//
		allowedFields.put("metricXX", 		"Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
//		allowedFields.put("", "Custom Variables or Columns");
		

		//only dimensions
		allowedFields.put("date", 				"Time");
		allowedFields.put("year", 			"Time");
		allowedFields.put("month", 	"Time");
		allowedFields.put("week", 		"Time");
		allowedFields.put("day", "Time");
		allowedFields.put("hour", "Time");
		allowedFields.put("minute", "Time");
//		ga:nthMonth
//		ga:nthWeek
//		ga:nthDay
//		ga:nthMinute
//		ga:dayOfWeek
//		ga:dayOfWeekName
//		ga:dateHour
//		ga:yearMonth
//		ga:yearWeek
//		ga:isoWeek
//		ga:isoYear
//		ga:isoYearIsoWeek
//		ga:nthHour
//		allowedFields.put("", "Time");
//		allowedFields.put("", "Time");
//		allowedFields.put("", "Time");
//		allowedFields.put("", "Time");
//		allowedFields.put("", "Time");
//		allowedFields.put("", "Time");
//		allowedFields.put("", "Time");
//		allowedFields.put("", "Time");
//		allowedFields.put("", "Time");
//		allowedFields.put("", "Time");
		
		
		

		allowedFields.put("dcmClickAd", 				"DoubleClick Campaign Manager");
		allowedFields.put("dcmClickAdId", 			"DoubleClick Campaign Manager");
		allowedFields.put("dcmClickAdType", 	"DoubleClick Campaign Manager");
		allowedFields.put("dcmClickAdTypeId", 		"DoubleClick Campaign Manager");
		allowedFields.put("dcmClickAdvertiser", "DoubleClick Campaign Manager");
		allowedFields.put("dcmClickAdvertiserId", "DoubleClick Campaign Manager");
		allowedFields.put("dcmClickCampaign", "DoubleClick Campaign Manager");
//		ga:dcmClickCampaignId
//		ga:dcmClickCreativeId
//		ga:dcmClickCreative
//		ga:dcmClickRenderingId
//		ga:dcmClickCreativeType
//		ga:dcmClickCreativeTypeId
//		ga:dcmClickCreativeVersion
//		ga:dcmClickSite
//		ga:dcmClickSiteId
//		ga:dcmClickSitePlacement
//		ga:dcmClickSitePlacementId
//		ga:dcmClickSpotId
//		ga:dcmFloodlightActivity
//		ga:dcmFloodlightActivityAndGroup
//		ga:dcmFloodlightActivityGroup
//		ga:dcmFloodlightActivityGroupId
//		ga:dcmFloodlightActivityId
//		ga:dcmFloodlightAdvertiserId
//		ga:dcmFloodlightSpotId
//		ga:dcmLastEventAd
//		ga:dcmLastEventAdId
//		ga:dcmLastEventAdType
//		ga:dcmLastEventAdTypeId
//		ga:dcmLastEventAdvertiser
//		ga:dcmLastEventAdvertiserId
//		ga:dcmLastEventAttributionType
//		ga:dcmLastEventCampaign
//		ga:dcmLastEventCampaignId
//		ga:dcmLastEventCreativeId
//		ga:dcmLastEventCreative
//		ga:dcmLastEventRenderingId
//		ga:dcmLastEventCreativeType
//		ga:dcmLastEventCreativeTypeId
//		ga:dcmLastEventCreativeVersion
//		ga:dcmLastEventSite
//		ga:dcmLastEventSiteId
//		ga:dcmLastEventSitePlacement
//		ga:dcmLastEventSitePlacementId
//		ga:dcmLastEventSpotId
		allowedFields.put("dcmFloodlightQuantity", "DoubleClick Campaign Manager");
		allowedFields.put("dcmFloodlightRevenue", "DoubleClick Campaign Manager");
		allowedFields.put("dcmCPC", "DoubleClick Campaign Manager");
		allowedFields.put("dcmCTR", "DoubleClick Campaign Manager");
		allowedFields.put("dcmClicks", "DoubleClick Campaign Manager");
		allowedFields.put("dcmCost", "DoubleClick Campaign Manager");
		allowedFields.put("dcmImpressions", "DoubleClick Campaign Manager");
		allowedFields.put("dcmROAS", "DoubleClick Campaign Manager");
		allowedFields.put("dcmRPC", "DoubleClick Campaign Manager");
		//allowedFields.put("", "DoubleClick Campaign Manager");
		

		allowedFields.put("landingContentGroupXX", 				"Content Grouping");
		allowedFields.put("previousContentGroupXX", 			"Content Grouping");
		allowedFields.put("contentGroupXX", 	"Content Grouping");
		allowedFields.put("nextContentGroupXX", 		"Content Grouping");
		//
		allowedFields.put("contentGroupUniqueViewsXX", "Content Grouping");
//		allowedFields.put("", "Content Grouping");
//		allowedFields.put("", "Content Grouping");
//		allowedFields.put("", "Content Grouping");
//		allowedFields.put("", "Content Grouping");
//		allowedFields.put("", "Content Grouping");
//		allowedFields.put("", "Content Grouping");
//		allowedFields.put("", "Content Grouping");
//		allowedFields.put("", "Content Grouping");
//		allowedFields.put("", "Content Grouping");
//		allowedFields.put("", "Content Grouping");
//		allowedFields.put("", "Content Grouping");
//		allowedFields.put("", "Content Grouping");
		
		

		//dimensions only
		allowedFields.put("userAgeBracket", 				"Audience");
		allowedFields.put("userGender", 			"Audience");
		allowedFields.put("interestOtherCategory", 	"Audience");
		allowedFields.put("interestAffinityCategory", 		"Audience");
		allowedFields.put("interestInMarketCategory", "Audience");
//		allowedFields.put("", "Audience");
//		allowedFields.put("", "Audience");
//		allowedFields.put("", "Audience");
//		allowedFields.put("", "Audience");
//		allowedFields.put("", "Audience");
//		allowedFields.put("", "Audience");
//		allowedFields.put("", "Audience");
//		allowedFields.put("", "Audience");
//		allowedFields.put("", "Audience");
//		allowedFields.put("", "Audience");
//		allowedFields.put("", "Audience");
//		allowedFields.put("", "Audience");
		

		allowedFields.put("channelGrouping", 				"Channel Grouping");
		allowedFields.put("", 			"Channel Grouping");
		allowedFields.put("", 	"Channel Grouping");
		allowedFields.put("", 		"Channel Grouping");
		//
//		allowedFields.put("", "Channel Grouping");
//		allowedFields.put("", "Channel Grouping");
//		allowedFields.put("", "Channel Grouping");
//		allowedFields.put("", "Channel Grouping");
//		allowedFields.put("", "Channel Grouping");
//		allowedFields.put("", "Channel Grouping");
//		allowedFields.put("", "Channel Grouping");
//		allowedFields.put("", "Channel Grouping");
//		allowedFields.put("", "Channel Grouping");
//		allowedFields.put("", "Channel Grouping");
//		allowedFields.put("", "Channel Grouping");
//		allowedFields.put("", "Channel Grouping");
//		allowedFields.put("", "Channel Grouping");
		

		
		
		//metrics (no dimensions)
		allowedFields.put("adsenseRevenue", 				"Adsense");
		allowedFields.put("adsenseAdUnitsViewed", 			"Adsense");
		allowedFields.put("adsenseAdsViewed", 	"Adsense");
		allowedFields.put("adsenseAdsClicks", 		"Adsense");
		allowedFields.put("adsensePageImpressions", "Adsense");
		allowedFields.put("adsenseCTR", "Adsense");
		allowedFields.put("adsenseECPM", "Adsense");
		allowedFields.put("adsenseExits", "Adsense");
		allowedFields.put("adsenseViewableImpressionPercent", "Adsense");
		allowedFields.put("adsenseCoverage", "Adsense");
//		allowedFields.put("", "Adsense");
//		allowedFields.put("", "Adsense");
//		allowedFields.put("", "Adsense");
//		allowedFields.put("", "Adsense");
//		allowedFields.put("", "Adsense");
//		allowedFields.put("", "Adsense");
//		allowedFields.put("", "Adsense");
		

		allowedFields.put("correlationModelId", 				"Related Products");
		allowedFields.put("queryProductId", 			"Related Products");
		allowedFields.put("queryProductName", 	"Related Products");
		allowedFields.put("queryProductVariation", 		"Related Products");
		allowedFields.put("relatedProductId", "Related Products");
		allowedFields.put("relatedProductName", "Related Products");
		//
		allowedFields.put("correlationScore", "Related Products");
		allowedFields.put("queryProductQuantity", "Related Products");
//		allowedFields.put("", "Related Products");
//		allowedFields.put("", "Related Products");
//		allowedFields.put("", "Related Products");
//		allowedFields.put("", "Related Products");
//		allowedFields.put("", "Related Products");
//		allowedFields.put("", "Related Products");
//		allowedFields.put("", "Related Products");
//		allowedFields.put("", "Related Products");
//		allowedFields.put("", "Related Products");
		
	}
	
	private boolean allowedFieldsByGroup(String fieldName, String groupName){
		if (allowedFields.size() == 0) buildAllowedFields();
		
		if (allowedFields.containsKey(fieldName) && allowedFields.get(fieldName).equals(groupName)) return true;
		
		return false;
	}
}
