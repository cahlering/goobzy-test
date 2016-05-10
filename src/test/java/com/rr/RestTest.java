package com.rr;

import static com.jayway.restassured.RestAssured.*;
import static com.jayway.restassured.matcher.RestAssuredMatchers.*;
import static org.hamcrest.Matchers.*;
import com.jayway.restassured.RestAssured;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.parsing.Parser;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by cahlering on 1/13/15.
 */
public class RestTest {

    @BeforeClass
    public static void configureRestAssured() {
        RestAssured.baseURI = "http://qa.richrelevance.com";
        RestAssured.port = 80;
//        RestAssured.registerParser(ContentType.HTML.toString(), Parser.JSON);
    }
    @Test
    public void testRecsWithStrategy() {
        given()
            .param("apiKey", "60a1c6334ee7122a")
            .param("apiClientKey", "006c605872d752f2")
            .param("strategyName", "TopSellers")
            .param("seed", "2534374302086515")
        .when()
            .get("/rrserver/api/rrPlatform/recsUsingStrategy")
        .then()
            .assertThat()
                .statusCode(200)
                .body("recommendedProducts.size()", greaterThanOrEqualTo(0));
    }
}
