package com.hof.imp;

import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.model.Goal;
import com.google.api.services.analytics.model.Goals;
import com.google.api.services.analytics.model.Profile;
import com.google.api.services.analytics.model.Profiles;
import org.docx4j.wml.CTRecipientData;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.api.services.analytics.model.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class GoogleAnalyticsTest {

    private GoogleAnalytics classUnderTest;

    private final String SAMPLE_PROFILE_ID_1 = "111";
    private final String SAMPLE_PROFILE_ID_2 = "222";
    private final String SAMPLE_PROFILE_ID_3 = "333";

    private final String SAMPLE_ACCOUNT_ID = "444";
    private final String SAMPLE_WEB_PROPERTY_ID = "555";

    @Mock
    private Analytics analytics;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        classUnderTest = spy(new GoogleAnalytics());
        doReturn(analytics).when(classUnderTest).getAnalytics();
    }

    /**
     * Test caching columns from GA when all Columns have unique UI names and no custom indexes by goals
     * @throws Exception
     */
    @Test
    public void testCacheAllColumns_UniqueUINames() throws Exception {
        List<Column> columns = new ArrayList<>();
        columns.add(getSampleColumn("ga:column1", "Column1", "Sample Status", "Text", "Text"));
        columns.add(getSampleColumn("ga:column2", "Column2", "Sample Status", "Text", "Text"));
        columns.add(getSampleColumn("ga:column3", "Column3", "Sample Status", "Text", "Text"));
        columns.add(getSampleColumn("ga:column4", "Column4", "Sample Status", "Text", "Text"));

        doReturn(columns).when(classUnderTest).getAllColumns(analytics);

        setupMocksForCacheAllColumns();

        JSONArray jsonArray = classUnderTest.cacheAllColumns();

        int numberOfColumns = jsonArray.length();

        assertThat(numberOfColumns, is(4));

        //Assert if all the attributes from json objects match with the passed in columns
        for (int i = 0; i < jsonArray.length(); i++) {
            Column column = columns.get(i);
            JSONObject jsonObject = jsonArray.getJSONObject(i);

            assertThat(column.getAttributes().get(GoogleAnalytics.UI_NAME_KEY), is (jsonObject.getString(GoogleAnalytics.UI_NAME_KEY)));
            assertThat(column.getAttributes().get(GoogleAnalytics.STATUS_KEY), is (jsonObject.getString(GoogleAnalytics.STATUS_KEY)));
            assertThat(column.getAttributes().get(GoogleAnalytics.DATA_TYPE_KEY), is (jsonObject.getString(GoogleAnalytics.DATA_TYPE_KEY)));
            assertThat(column.getAttributes().get(GoogleAnalytics.TYPE_KEY), is (jsonObject.getString(GoogleAnalytics.TYPE_KEY)));
        }
    }

    /**
     * Test caching columns from GA when there are 2 columns with the same UI names and no custom indexes by goals
     * @throws Exception
     */
    @Test
    public void testCacheAllColumns_DuplicatedUINames() throws Exception {
        List<Column> columns = new ArrayList<>();
        //2 columns with Column1 UI Name
        columns.add(getSampleColumn("ga:column1", "Column1", "Sample Status", "Text", "Text"));
        columns.add(getSampleColumn("ga:column2", "Column1", "Sample Status", "Text", "Text"));
        columns.add(getSampleColumn("ga:column3", "Column3", "Sample Status", "Text", "Text"));
        columns.add(getSampleColumn("ga:column4", "Column4", "Sample Status", "Text", "Text"));

        doReturn(columns).when(classUnderTest).getAllColumns(analytics);

        setupMocksForCacheAllColumns();

        JSONArray jsonArray = classUnderTest.cacheAllColumns();

        int numberOfColumns = jsonArray.length();

        assertThat(numberOfColumns, is(4));

        JSONObject jsonObject1 = jsonArray.getJSONObject(0);
        assertThat(jsonObject1.getString(GoogleAnalytics.UI_NAME_KEY), is("Column1 (ga:column1)"));

        JSONObject jsonObject2 = jsonArray.getJSONObject(1);
        assertThat(jsonObject2.getString(GoogleAnalytics.UI_NAME_KEY), is("Column1 (ga:column2)"));
    }

    /**
     * Test caching columns from GA when all columns have unique UI names and there are custom indexes by Goals
     * @throws Exception
     */
    @Test
    public void testCacheAllColumns_MultipleGoals_UniqueUINames() throws Exception {
        List<Column> columns = new ArrayList<>();
        //1 Column with XX for custom index
        columns.add(getSampleColumn("ga:columnXX", "Goal XX Column", "Sample Status", "Text", "Text"));
        columns.add(getSampleColumn("ga:column2", "Column1", "Sample Status", "Text", "Text"));
        columns.add(getSampleColumn("ga:column3", "Column3", "Sample Status", "Text", "Text"));
        columns.add(getSampleColumn("ga:column4", "Column4", "Sample Status", "Text", "Text"));

        doReturn(columns).when(classUnderTest).getAllColumns(analytics);

        setupMocksForCacheAllColumns();

        JSONArray jsonArray = classUnderTest.cacheAllColumns();

        int numberOfColumns = jsonArray.length();

        //Should have 6 columns because there are 3 goals and there is 1 column with XX (custom indexes)
        assertThat(numberOfColumns, is(6));

        //Assert if XX is replaced correctly with the index of Goal in returned UI name
        JSONObject jsonObject1 = jsonArray.getJSONObject(0);
        assertThat(jsonObject1.getString(GoogleAnalytics.UI_NAME_KEY), is("Goal 1 Column"));

        JSONObject jsonObject2 = jsonArray.getJSONObject(1);
        assertThat(jsonObject2.getString(GoogleAnalytics.UI_NAME_KEY), is("Goal 2 Column"));

        JSONObject jsonObject3 = jsonArray.getJSONObject(2);
        assertThat(jsonObject3.getString(GoogleAnalytics.UI_NAME_KEY), is("Goal 3 Column"));
    }

    /**
     * Test caching columns from GA when there are columns with similar UI names and there are custom indexes by Goals
     * @throws Exception
     */
    @Test
    public void testCacheAllColumns_MultipleGoals_DuplicatedUINames() throws Exception {
        List<Column> columns = new ArrayList<>();
        //2 Columns with XX for custom index (duplicated)
        columns.add(getSampleColumn("ga:columnXX", "Goal XX Column", "Sample Status", "Text", "Text"));
        columns.add(getSampleColumn("ga:columnAXX", "Goal XX Column", "Sample Status", "Text", "Text"));
        columns.add(getSampleColumn("ga:column3", "Column3", "Sample Status", "Text", "Text"));
        columns.add(getSampleColumn("ga:column4", "Column4", "Sample Status", "Text", "Text"));

        doReturn(columns).when(classUnderTest).getAllColumns(analytics);

        setupMocksForCacheAllColumns();

        JSONArray jsonArray = classUnderTest.cacheAllColumns();

        int numberOfColumns = jsonArray.length();

        //Should have 8 columns because there are 3 goals and there are 2 columns with XX (custom indexes)
        assertThat(numberOfColumns, is(8));

        //Assert if XX is replaced correctly with the index of Goal in returned UI name
        // and duplicated UI names are appened with technical names (id)
        JSONObject jsonObject1 = jsonArray.getJSONObject(0);
        assertThat(jsonObject1.getString(GoogleAnalytics.UI_NAME_KEY), is("Goal 1 Column (ga:column1)"));

        JSONObject jsonObject2 = jsonArray.getJSONObject(1);
        assertThat(jsonObject2.getString(GoogleAnalytics.UI_NAME_KEY), is("Goal 2 Column (ga:column2)"));

        JSONObject jsonObject3 = jsonArray.getJSONObject(2);
        assertThat(jsonObject3.getString(GoogleAnalytics.UI_NAME_KEY), is("Goal 3 Column (ga:column3)"));

        JSONObject jsonObject4 = jsonArray.getJSONObject(3);
        assertThat(jsonObject4.getString(GoogleAnalytics.UI_NAME_KEY), is("Goal 1 Column (ga:columnA1)"));

        JSONObject jsonObject5 = jsonArray.getJSONObject(4);
        assertThat(jsonObject5.getString(GoogleAnalytics.UI_NAME_KEY), is("Goal 2 Column (ga:columnA2)"));

        JSONObject jsonObject6 = jsonArray.getJSONObject(5);
        assertThat(jsonObject6.getString(GoogleAnalytics.UI_NAME_KEY), is("Goal 3 Column (ga:columnA3)"));
    }

    /**
     * Generate and return the sample goals
     * @return
     */
    private Goals getSampleGoals() {
        Goal goal1 = new Goal();
        Goal goal2 = new Goal();
        Goal goal3 = new Goal();

        List<Goal> goalList = new ArrayList<>();
        goalList.add(goal1);
        goalList.add(goal2);
        goalList.add(goal3);

        Goals goals = new Goals();
        goals.setItems(goalList);

        return goals;
    }

    /**
     * Generate and return the sample profiles
     * @return
     */
    private Profiles getSampleProfiles() {
        Profile profile1 = new Profile();
        profile1.setId(SAMPLE_PROFILE_ID_1);
        profile1.setAccountId(SAMPLE_ACCOUNT_ID);
        profile1.setWebPropertyId(SAMPLE_WEB_PROPERTY_ID);

        Profile profile2 = new Profile();
        profile2.setId(SAMPLE_PROFILE_ID_2);

        Profile profile3 = new Profile();
        profile3.setId(SAMPLE_PROFILE_ID_3);

        List<Profile> profileList = new ArrayList<>();
        profileList.add(profile1);
        profileList.add(profile2);
        profileList.add(profile3);

        Profiles profiles = new Profiles();
        profiles.setItems(profileList);

        return profiles;
    }

    /**
     * Generate and return a sample column
     * @param id technical name of the column (must be unique)
     * @param uiName UI name of the column (can be duplicated)
     * @param status Status of a column
     * @param dataType Data type of a column
     * @param type Type of a column
     * @return
     */
    private Column getSampleColumn(String id, String uiName, String status, String dataType, String type) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(GoogleAnalytics.UI_NAME_KEY, uiName);
        attributes.put(GoogleAnalytics.STATUS_KEY, status);
        attributes.put(GoogleAnalytics.DATA_TYPE_KEY, dataType);
        attributes.put(GoogleAnalytics.TYPE_KEY, type);

        Column column = new Column();
        column.setAttributes(attributes);
        column.setId(id);

        return column;
    }

    /**
     * Set up mocks before calling cacheAllColumns method
     * @throws Exception
     */
    private void setupMocksForCacheAllColumns() throws Exception {
        doReturn(getSampleProfiles()).when(classUnderTest).getProfiles(analytics);
        doReturn(getSampleGoals()).when(classUnderTest).getGoals(eq(analytics), anyString(), anyString(), anyString());
        doReturn(SAMPLE_PROFILE_ID_1).when(classUnderTest).getAttribute(GoogleAnalytics.PROFILE_ID_KEY);
        doReturn(true).when(classUnderTest).saveBlobData(eq(GoogleAnalytics.ALL_COLUMNS_METADATA_KEY), any(byte[].class));
    }
}