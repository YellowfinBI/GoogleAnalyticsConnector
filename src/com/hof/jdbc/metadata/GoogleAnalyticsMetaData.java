package com.hof.jdbc.metadata;

import com.hof.util.Const;
import com.hof.util.GoogleAnalyticsDataZoom;
import com.hof.util.OrgCache;
import com.hof.util.i4RequestRegistry;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.scribe.builder.ServiceBuilder;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.model.Verifier;
import org.scribe.oauth.OAuthService;

import com.google.api.client.auth.oauth2.AuthorizationCodeFlow;
import com.google.api.client.auth.oauth2.BearerToken;
import com.google.api.client.auth.oauth2.ClientParametersAuthentication;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.model.Profile;
import com.google.api.services.analyticsreporting.v4.*;
import com.hof.data.SessionBean;
import com.hof.mi.data.ReportImageItemBean;
import com.hof.mi.interfaces.UserInputParameters.Parameter;
import com.hof.pool.DBType;
import com.hof.pool.JDBCMetaData;

public class GoogleAnalyticsMetaData extends JDBCMetaData {

	boolean initialised = false;
	String url;
	Token requestToken=new Token("", "");
	
	private static HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static JsonFactory JSON_FACTORY = new JacksonFactory();
	private static String SCOPE = "https://www.googleapis.com/auth/analytics.readonly";
	private static String AUTHORIZATION_SERVER_URL = "https://accounts.google.com/o/oauth2/auth";
	private static String TOKEN_SERVER_URL="https://accounts.google.com/o/oauth2/token";
	
	private static AuthorizationCodeFlow flow = new AuthorizationCodeFlow.Builder(BearerToken
		      .authorizationHeaderAccessMethod(),
		      HTTP_TRANSPORT,
		      JSON_FACTORY,
		      new GenericUrl(TOKEN_SERVER_URL),
		      new ClientParametersAuthentication(
		    		  new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getData())), new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getZoom()))),
		    		  new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getData())),
		      AUTHORIZATION_SERVER_URL).setScopes(Arrays.asList(SCOPE))
		      .build();
	//private static ArrayList<String> Ws=new ArrayList<String>();
	//private boolean setSelect=false;
	
	
	public GoogleAnalyticsMetaData() {
		
		super();
		
		sourceName = GoogleAnalyticsDataZoom.getText("Google Analytics API", "mi.text.ga.datasource.name");
		sourceCode = "GOOGLE_ANALYTICS";
		driverName = "com.hof.imp.GoogleAnalytics";
		sourceType = DBType.THIRD_PARTY;
	}
	
	public  void initialiseParameters() {
		
		super.initialiseParameters();
		
		if (!initialised)
		{
			
			String urlAddr = flow
					.newAuthorizationUrl()
					.setState("xyz")
					.setRedirectUri("urn:ietf:wg:oauth:2.0:oob").build();
			url=urlAddr;
			initialised=true;
		}
		String inst=GoogleAnalyticsDataZoom.getText("1. Click the 'Authorize Google Analytics' button, login, and 'Allow' access to your account.", "mi.text.ga.connection.instructions.line1", "mi.text.ga.connection.request.pin.button.text")+"<br>"+  
				GoogleAnalyticsDataZoom.getText("2. Copy the PIN provided and paste it into Yellowfin.", "mi.text.ga.connection.instructions.line2")+"<br>"+ 
				GoogleAnalyticsDataZoom.getText("3. Click the 'Validate PIN' button.", "mi.text.ga.connection.instructions.line3", "mi.text.ga.connection.validate.pin.button.text");
		
		addParameter(new Parameter("HELP", GoogleAnalyticsDataZoom.getText("Connection Instructions", "mi.text.ga.connection.instructions.label"),  inst, TYPE_NUMERIC, DISPLAY_STATIC_TEXT, null, true));
        
        Parameter p = new Parameter("URL", GoogleAnalyticsDataZoom.getText("1. Request Access PIN", "mi.text.ga.connection.request.pin.button.label"), GoogleAnalyticsDataZoom.getText("Connect to Google Analytics to receive a PIN for data access", "mi.text.ga.connection.request.pin.button.description"),TYPE_UNKNOWN, DISPLAY_URLBUTTON,  null, true);
        p.addOption("BUTTONTEXT", GoogleAnalyticsDataZoom.getText("Authorize Google Analytics", "mi.text.ga.connection.request.pin.button.text"));
        p.addOption("BUTTONURL", url);
        addParameter(p);
        addParameter(new Parameter("PIN", GoogleAnalyticsDataZoom.getText("2. Enter PIN", "mi.text.ga.connection.request.pin.field.label"),  GoogleAnalyticsDataZoom.getText("Enter the PIN received from Google Analytics", "mi.text.ga.connection.request.pin.field.description"), TYPE_TEXT, DISPLAY_TEXT_MED, null, true));
        p = new Parameter("POSTPIN", GoogleAnalyticsDataZoom.getText("3. Validate Pin","mi.text.ga.connection.validate.pin.button.label"),  GoogleAnalyticsDataZoom.getText("Validate the PIN", "mi.text.ga.connection.validate.pin.button.description"), TYPE_TEXT, DISPLAY_BUTTON, null, true);
        p.addOption("BUTTONTEXT", GoogleAnalyticsDataZoom.getText("Validate PIN", "mi.text.ga.connection.validate.pin.button.text"));
        addParameter(p);
		
		addParameter(new Parameter("ACCESS_TOKEN", GoogleAnalyticsDataZoom.getText("Access Token", "mi.text.ga.connection.access.token.field.label"), GoogleAnalyticsDataZoom.getText("Access Token", "mi.text.ga.connection.access.token.field.description"),TYPE_TEXT, DISPLAY_PASSWORD,  null, true));
		addParameter(new Parameter("REFRESH_TOKEN", GoogleAnalyticsDataZoom.getText("Refresh Token", "mi.text.ga.connection.refresh.token.field.label"), GoogleAnalyticsDataZoom.getText("Refresh Token", "mi.text.ga.connection.refresh.token.field.description"),TYPE_TEXT, DISPLAY_PASSWORD,  null, true));
		addParameter(new Parameter("WEBSITE", GoogleAnalyticsDataZoom.getText("Website", "mi.text.ga.connection.website.field.label"), GoogleAnalyticsDataZoom.getText("Website", "mi.text.ga.connection.website.field.description"),TYPE_TEXT, DISPLAY_TEXT_LONG,  null, true));
		addParameter(new Parameter("PROFILEID", GoogleAnalyticsDataZoom.getText("Profile ID", "mi.text.ga.connection.profileid.field.label"), GoogleAnalyticsDataZoom.getText("Profile ID", "mi.text.ga.connection.profileid.field.description"),TYPE_TEXT, DISPLAY_TEXT_LONG,  null, true));
		//addParameter(new Parameter("WEB", GoogleAnalyticsDataZoom.getText("Web", "mi.text.ga.connection.website.field.labelll"), GoogleAnalyticsDataZoom.getText("Website", "mi.text.ga.connection.website.field.description"),TYPE_TEXT, DISPLAY_SELECT,  null, true));
		
		/*List<Object[]> opt=getParameter("WEBSITE").getOptions();
		if (!Ws.isEmpty())
		{
			if (getParameter("WEB")==null)
			{
				Parameter select = new Parameter("WEB", "WEB" ,  "Sites", TYPE_TEXT, DISPLAY_SELECT, null, true);
				
				for (String val:Ws)
				{
					Object[] check=new Object[2];
					check[0]=val;
					check[1]=val;
					if (!getParameter("WEB").getOptions().contains(check))
						select.addOption(val, val);
				}
				
				addParameter(select);
				
				//Ws.clear();
				
			}
			
		}*/
		
		
		
		/*while (((String)getParameterValue("ACCESS_TOKEN")).equals(""))
		{
			
		}*/
		
	}
	
	public String buttonPressed(String buttonName) throws Exception 
	{
        if (buttonName.equals("POSTPIN") && getParameterValue("PIN")!=null)
        {
        	String ver=(String)getParameterValue("PIN");
        	
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
	        	
	        	flow = new AuthorizationCodeFlow.Builder(BearerToken
	      		      .authorizationHeaderAccessMethod(),
	      		      HTTP_TRANSPORT,
	      		      JSON_FACTORY,
	      		      new GenericUrl(TOKEN_SERVER_URL),
	      		      new ClientParametersAuthentication(
	      		    		  new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getData())), new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getZoom()))),
	      		    		  new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getData())),
	      		      AUTHORIZATION_SERVER_URL).setScopes(Arrays.asList(SCOPE))
	      		      .build();
			}
        	
        	
        	TokenResponse resp=flow.newTokenRequest(ver).setRedirectUri("urn:ietf:wg:oauth:2.0:oob").execute();
			
        	setParameterValue("ACCESS_TOKEN", resp.getAccessToken());
			setParameterValue("REFRESH_TOKEN", resp.getRefreshToken());
			
			Collection<String> scopes=new ArrayList<String>();
			scopes.add(AnalyticsReportingScopes.ANALYTICS_READONLY);
			
			GoogleCredential credential2 = new GoogleCredential.Builder()
			.setTransport(HTTP_TRANSPORT)
			.setJsonFactory(JSON_FACTORY)
			.setClientSecrets(new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getData())), new String(com.hof.util.Base64.decode(GoogleAnalyticsDataZoom.getZoom())))
			.build()
			.setAccessToken((String)getParameterValue("ACCESS_TOKEN"))
			.setRefreshToken((String)getParameterValue("REFRESH_TOKEN"));
			
			Analytics a = new Analytics.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential2)
			.setApplicationName("Yellowfin")
			.build();
			
			com.google.api.services.analytics.model.Profiles pfls=a.management().profiles().list("~all", "~all").execute();//accounts().list().execute();
			int i;
			String IDs="", Websites="";
			
			
	        List<Profile> profilesCollection=pfls.getItems();
	        if(profilesCollection.size()>0)
	        {
	        	IDs=profilesCollection.get(0).getId();
	        	Websites=profilesCollection.get(0).getName();
	        	//getParameter("WEB").addOption(profilesCollection.get(0).getId(), profilesCollection.get(0).getName());
	        	for(i=1;i<profilesCollection.size(); i++)
	        	{
	        		IDs=IDs+", "+profilesCollection.get(i).getId();
	        		Websites=Websites+", "+profilesCollection.get(i).getName();
	        		//getParameter("WEB").addOption(profilesCollection.get(i).getId(), profilesCollection.get(i).getName());
		        	
	        	}
	        }
			/*for (Profile p:pfls.getItems())
			{
				IDs=IDs+p.getId()+", ";
				Websites=Websites+p.getName()+", ";
			}*/
			
			/*if (Ws.isEmpty())
			{
				for (Profile p:pfls.getItems())
				{
					if (!Ws.contains(p.getName()))
						Ws.add(p.getName());
				}
			}*/
			
			setParameterValue("PROFILEID", IDs);
			setParameterValue("WEBSITE", Websites);
			
			/*Parameter pp=getParameter("WEB");
			for (String val:Ws) {
				pp.addOption(val, val);
			}*/
			//setSelect=true;

    		/*JSONObject companiesObj=new JSONObject(response.getBody());
    		JSONArray companies=new JSONArray();
    		if (companiesObj.has("values"))
    		{
    			companies=companiesObj.getJSONArray("values");
    		}
    		
    		if (companies.length()>0)
    		{
    			setParameterValue("COMPANY", companies.getJSONObject(0).getString("name"));
    		}*/
    		
    		
        }      
        return null;
        
    }
	
	@Override
	public String getDatasourceShortDescription(){
		return GoogleAnalyticsDataZoom.getText("Connect to Google Analytics", "mi.text.ga.short.description");
	}
	
	@Override
	public String getDatasourceLongDescription(){
		return GoogleAnalyticsDataZoom.getText("Analyse and monitor your website traffic data from Google Analytics.", "mi.text.ga.long.description");
	}

	@Override
	public byte[] getDatasourceIcon() {
		String str = "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";
		return str.getBytes();
	}
}
