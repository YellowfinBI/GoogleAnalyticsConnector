package com.hof.util;


public class GoogleAnalyticsDataZoom {

	//private final static String API_KEY="MTAwNDcwOTEwMzM5My05Y2xqZWNuNjdlNXE5NnNoODY1YmMxbnQyZTJvZWo4ZS5hcHBzLmdvb2dsZXVzZXJjb250ZW50LmNvbQ==";
	//private final static String API_SECRET="VWNFLUJGRFJzU3h4Nk9BOXZqMDlzS09S";
	
	private final static char[] data={'M', 'T', 'A', 'w', 'N', 'D', 'c', 'w', 'O', 'T', 'E', 'w', 'M', 'z', 'M', '5', 'M', 'y', '0', '5', 'Y', '2', 'x', 'q', 'Z', 'W', 'N', 'u', 'N', 'j', 'd', 'l', 'N', 'X', 'E', '5', 'N', 'n', 'N', 'o', 'O', 'D', 'Y', '1', 'Y', 'm', 'M', 'x', 'b', 'n', 'Q', 'y', 'Z', 'T', 'J', 'v', 'Z', 'W', 'o', '4', 'Z', 'S', '5', 'h', 'c', 'H', 'B', 'z', 'L', 'm', 'd', 'v', 'b', '2', 'd', 's', 'Z', 'X', 'V', 'z', 'Z', 'X', 'J', 'j', 'b', '2', '5', '0', 'Z', 'W', '5', '0', 'L', 'm', 'N', 'v', 'b', 'Q', '=', '='};
	private final static char[] zoom={'V', 'W', 'N', 'F', 'L', 'U', 'J', 'G', 'R', 'F', 'J', 'z', 'U', '3', 'h', '4', 'N', 'k', '9', 'B', 'O', 'X', 'Z', 'q', 'M', 'D', 'l', 'z', 'S', '0', '9', 'S'};
	
	public static String getData() {
		return new String(data);
	}
	
	public static String getZoom() {
		return new String(zoom);
	}
	
	public static String getText(String englishText, String translationKey) {

	    String text = UtilString.getResourceString(translationKey);
	    if (text==null) return englishText;
	    return text;

	}
	
	public static String getText(String englishText, String translationKey, String paramKey) {
		String key="";
		if(UtilString.getResourceString(paramKey)!=null)
		{
			key=UtilString.getResourceString(paramKey);
		}
		String text = UtilString.getResourceString(translationKey);
	    if (text==null) return englishText;
	    
	    else if (text.contains("{0}"))
	    {
	    	text=text.replace("{0}", "'"+key+"'");
	    }
	    else if (text.contains("{1}"))
	    {
	    	text=text.replace("{1}", "'"+key+"'");
	    }
	    
	    return text;
	}
	

}
