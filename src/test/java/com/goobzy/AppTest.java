package com.goobzy;


import com.google.common.collect.Lists;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.parse4j.*;
import org.parse4j.callback.FindCallback;

import static org.junit.Assert.*;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class AppTest
{

  static String BATCH_CLASS = "ParseObjectBatch";
  static String UPLOAD_CLASS = "ImageMedia";
  static String CLUSTER_CLASS = "MediaCluster";
  static String EVENT_CLASS = "Event";

  static ParseGeoPoint USA_CA_LOOMIS = new ParseGeoPoint(38.780323, -121.196213);
  static ParseGeoPoint USA_CA_SAN_FRANCISCO = new ParseGeoPoint(37.802568, -122.437969);
  static ParseGeoPoint USA_NV_INCLINE_VILLAGE = new ParseGeoPoint(39.239642, -119.943108);

  private static String deviceId;
  private static FindCallback<ParseObject> backgroundDeleteCallback = new FindCallback<ParseObject>() {
    @Override
    public void done(List<ParseObject> list, ParseException parseException) {
      if (list != null) {
        for (ParseObject listItem : list) {
          listItem.deleteInBackground();
        }
      }
      if (parseException != null) {
        parseException.printStackTrace();
      }
    }
  };

  @BeforeClass
  public static void setUpClass() throws ParseException {
    Parse.initialize("WsUm1i8wVx37JEQKYFtK8YGEKCcP87gxMmf23G9b", "dbLrShGNndpZhKm1w1gUhq8zHqaSnKKEZVvGErtu");
    deviceId = generateTestDeviceId();
//
//    getUploadObject(3, USA_CA_LOOMIS, false, UUID.randomUUID().toString()).save();
//    getUploadObject(3, USA_CA_LOOMIS, false, UUID.randomUUID().toString()).save();
//    getUploadObject(4, USA_CA_LOOMIS, false, UUID.randomUUID().toString()).save();
//    getUploadObject(1, USA_CA_LOOMIS, false, UUID.randomUUID().toString()).save(); // shouldn't be in the event, too recent
//    getUploadObject(1, USA_CA_SAN_FRANCISCO, false, UUID.randomUUID().toString()).save(); // shouldn't be in the event, too recent
//    getUploadObject(2, USA_CA_SAN_FRANCISCO, false, UUID.randomUUID().toString()).save();
//    getUploadObject(2, USA_CA_SAN_FRANCISCO, false, UUID.randomUUID().toString()).save();
//    //save the last one in the foreground so the queries in the tests have objects to retrieve
//    getUploadObject(2, USA_CA_SAN_FRANCISCO, false, UUID.randomUUID().toString()).save();

  }

  private static ParseObject getUploadObject(int hoursAgo, ParseGeoPoint location, Boolean selfieFlag, String filePath) {
    ParseObject upload = new ParseObject(UPLOAD_CLASS);
    upload.put("deviceId", deviceId);
    upload.put("dateImageTakenTs", milliseconds(hoursAgo));
    upload.put("location", location);
    upload.put("selfie", selfieFlag);
    upload.put("filePath", filePath);
    return upload;
  }

  private static long milliseconds(long hoursAgo) {
    return System.currentTimeMillis() - (hoursAgo * 3600000);
  }

  private static String generateTestDeviceId() {
//        return "test";
    return "test_" + UUID.randomUUID().toString();
  }

  @Test
  public void testUnique() throws ParseException, InterruptedException {
    ParseQuery<ParseObject> uploadCount = ParseQuery.getQuery(UPLOAD_CLASS);
    uploadCount.whereEqualTo("deviceId", deviceId);
    int initialCount = uploadCount.count();
    getUploadObject(1, USA_CA_LOOMIS, false, "first").save();
    Thread.sleep(1000);
    try {
      getUploadObject(1, USA_NV_INCLINE_VILLAGE, false, "first").save();
    } catch (ParseException es) {
      assertEquals(142, es.getCode());
    }
    int finalCount = uploadCount.count();
    assertEquals(initialCount + 1, finalCount);
  }

  @Test
  @Ignore(value = "Ignore from general runs, requires background batch to be executed to determine state")
  public void testBatchCreate() {
    ParseObject batchObject = new ParseObject(BATCH_CLASS);
    List<JSONObject> batchList = Lists.newArrayList(
        getUploadObject(2, USA_CA_SAN_FRANCISCO, false, "duplicate_path").getParseData(),
        getUploadObject(2, USA_CA_SAN_FRANCISCO, false, "duplicate_path2").getParseData(),
        getUploadObject(2, USA_CA_SAN_FRANCISCO, false, "duplicate_path3").getParseData()
    );
    JSONArray batchArray = new JSONArray(batchList);
    batchObject.put("deviceId", deviceId);
    batchObject.put("parseObjectType", UPLOAD_CLASS);
    batchObject.put("processed", Boolean.FALSE);
    batchObject.put("parseObjectJson", batchArray.toString());
    try {
      batchObject.save();
    } catch (ParseException e) {
      e.printStackTrace();
    }

  }

  @Test
  @Ignore //currently disabled cluster creation after image upload
  public void testHasCluster() throws ParseException {
    ParseQuery<ParseObject> imageQuery = ParseQuery.getQuery(UPLOAD_CLASS);
    imageQuery.whereEqualTo("deviceId", deviceId);
    List<ParseObject> images = imageQuery.find();
    assertNotNull(images);
    assertTrue(images.size() > 0);
    for (ParseObject uploadObject : images) {
      assertNotNull(uploadObject.get("primaryCluster"));
    }
  }

  @Test
  public void testAutoEvent() throws ParseException {
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put("deviceId", deviceId);
    String response = ParseCloud.callFunction("autoEvent", parameters);

    ParseQuery<ParseObject> eventQuery = ParseQuery.getQuery(EVENT_CLASS);
    List<ParseObject> eventList = eventQuery.whereEqualTo("deviceId", deviceId).find();
    for (ParseObject event: eventList) {
      assertNotNull(event.get("mostRecentImageTakenTs"));
      ParseRelation<ParseObject> images = event.getRelation("media");
      assertEquals("Expected image count", 3, images.getQuery().count()); //We should have 3 images in each event, as 1 is too recent to be included (90 minute threshold)
    }
    assertTrue("Wrong number of events. ", eventList.size() == 2);
  }

  @Test
  public void testRecentActivity() throws ParseException {
    for (int i = 0; i < 30; i++) {
      getUploadObject(i % 3, USA_NV_INCLINE_VILLAGE, false, UUID.randomUUID().toString()).save();
    }
    Map<String, String> params = new HashMap<String, String>();
    params.put("deviceId", deviceId);
    JSONArray recentImages = ParseCloud.callFunction("recent", params);
    assertEquals("Recent activity length.", 25, recentImages.length());
    long lastAdded = Long.MAX_VALUE;
    for (int i = 0; i < recentImages.length(); i++) {
      JSONObject image = recentImages.getJSONObject(i);
      long added = image.getLong("dateImageTakenTs");
      assertTrue("Images out of order", added <= lastAdded);
      lastAdded = added;
    }
  }

  @AfterClass
  public static void tearDownClass() throws InterruptedException {
    ParseQuery<ParseObject> imageQuery = ParseQuery.getQuery(UPLOAD_CLASS);
    imageQuery.whereEqualTo("deviceId", deviceId);
//    imageQuery.findInBackground(backgroundDeleteCallback);

    ParseQuery<ParseObject> clusterQuery = ParseQuery.getQuery(CLUSTER_CLASS);
    clusterQuery.whereEqualTo("deviceId", deviceId);
//    clusterQuery.findInBackground(backgroundDeleteCallback);

    ParseQuery<ParseObject> eventQuery = ParseQuery.getQuery(EVENT_CLASS);
    eventQuery.whereEqualTo("deviceId", deviceId);
//    eventQuery.findInBackground(backgroundDeleteCallback);

    Thread.sleep(10000);

  }

  @Test
  public void testJeffsEvents() throws ParseException {
    Comparator<ParseObject> takenComparator = (img1, img2) -> Long.valueOf(img1.getLong("dateImageTakenTs")).compareTo(img2.getLong("dateImageTakenTs"));
    ParseQuery<ParseObject> eventQuery = ParseQuery.getQuery(EVENT_CLASS);
    List<ParseObject> events = eventQuery.whereEqualTo("deviceId", "f14d5a3fb07331ab").find();
    events.stream().forEach(event -> {
      System.out.println(event.getObjectId());
      ParseRelation<ParseObject> imgs = event.getRelation("media");
      try {
        imgs.getQuery().find().stream().sorted(takenComparator).forEach(img -> {
          Instant takenInstant = Instant.ofEpochMilli(img.getLong("dateImageTakenTs"));
          System.out.println(String.format("%s -> %s", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'hh:mm:ss").format(takenInstant.atZone(ZoneId.of("America/Los_Angeles"))), img.getString("filePath")));
        });
      } catch (ParseException e) {
        e.printStackTrace();
      }
    });
  }

}
