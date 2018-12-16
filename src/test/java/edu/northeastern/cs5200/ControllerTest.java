package edu.northeastern.cs5200;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * 
 * @author sanketsaurav
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ControllerTest {
	

	/**
	 * 				!!!!!------IMPORTANT-------!!!
	 * Delete Tables before and after tests
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@BeforeClass
	@AfterClass
	public static void t101_deleteActorMovie() throws SQLException, ClassNotFoundException {
		System.out.println("\n----------------DELETING TABLES--------");
		GenericController con = new GenericController();
//		if (con.getRecords("actor_movie") != null)
//			con.deleteTable("actor_movie");
		if (con.getRecords("actor") != null)
			con.deleteTable("actor");
		if (con.getRecords("movie") != null)
			con.deleteTable("movie");
	}

	@Test
	public void t102_testTableDoesNotExists() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		String expected = null;
		assertEquals(expected, con.getRecords("actor"));
	}

	@Test
	public void t103_testNewTableCreate() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		String requestBody = "{\"name\":\"Harrison\"}";
		String actualResponse = con.createTable("actor", requestBody);
		String expectedJson = "{\"id\":\"1\",\"name\":\"Harrison\"}";
		assertEquals(expectedJson, actualResponse);
	}

	@Test
	public void t104_testNewTableCreate() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		String json = "{\"name\":\"Joe\"}";
		String expectedJson = "{\"id\":\"2\",\"name\":\"Joe\"}";
		String actualResponse = con.createTable("actor", json);
		assertEquals(expectedJson, actualResponse);
	}

	@Test
	public void t105_testAlterTable() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		String json = "{\"last\":\"Goslyn\",\"first\":\"Ryan\"}";
		String res = "{\"id\":\"3\",\"name\":\"null\",\"last\":\"Goslyn\",\"first\":\"Ryan\"}";
		assertEquals(con.createTable("actor", json), res);
	}

	@Test
	public void t106_testGetAllActors() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		String expectedRes = "[{\"id\":\"1\",\"name\":\"Harrison\",\"last\":\"null\",\"first\":\"null\"},{\"id\":\"2\",\"name\":\"Joe\",\"last\":\"null\",\"first\":\"null\"},{\"id\":\"3\",\"name\":\"null\",\"last\":\"Goslyn\",\"first\":\"Ryan\"}]";
		assertEquals(expectedRes, con.getRecords("actor"));
	}


	@Test
	public void t108_testMovieTableDoesNotExists() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		String json = "{\"name\":\"Joseph\"}";
		assertEquals(con.updateRecordsbyid("movie", "432", json), null);
	}

	@Test
	public void t109_testUpdateIfRecordDoesNotExit() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		String json = "{\"name\":\"Joseph\"}";
		assertEquals(con.updateRecordsbyid("actor", "432", json), null);
	}

	@Test
	public void t110_testUpdateRecord() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		String json = "{\"name\":\"Joseph\"}";
		String res = "{\"id\":\"2\",\"name\":\"Joseph\",\"last\":\"null\",\"first\":\"null\"}";
		String actualResponse = con.updateRecordsbyid("actor", "2", json);
		assertEquals(actualResponse, res);
	}

	@Test
	public void t111_updatesSchemaandRecord() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		String json = "{\"nombre\":\"Joseph\"}";
		String res = "{\"id\":\"2\",\"name\":\"Joseph\",\"last\":\"null\",\"first\":\"null\",\"nombre\":\"Joseph\"}";
		assertEquals(con.updateRecordsbyid("actor", "2", json), res);
	}

	@Test
	public void t112_testDeleteActorNoTExist() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		assertEquals(con.deleteRecordsByid("actor", "111"), null);
	}

	@Test
	public void t113_testDeleteTableDoesnotExist() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		assertEquals(con.deleteRecordsByid("movie", "111"), null);
	}

	@Test
	public void t114_deleteRecords() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		assertEquals(con.deleteRecordsByid("actor", "2"), new Integer(1));
	}

	@Test
	public void t115_returnsJsonOnNewTableCreate() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		String json = "{\"title\":\"Blade Runner\"}";
		String expectedRes = "{\"id\":\"1\",\"title\":\"Blade Runner\"}";
		assertEquals(con.createTable("movie", json), expectedRes);
	}

	@Test
	public void t116_returnsJsonOnNewTableCreate2() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		String expectedRes = "{\"id\":\"2\",\"title\":\"La La Land\"}";
		String json = "{\"title\":\"La La Land\"}";
		assertEquals(con.createTable("movie", json), expectedRes);
	}
	
	@Test
	public void t221_returnsAllActors() throws SQLException, ClassNotFoundException {
		GenericController con = new GenericController();
		System.out.println("\t\t\t ---------TESTING NEW---- 4646");
		String expectedRes = "[{\"id\":\"3\",\"name\":\"null\",\"last\":\"Goslyn\",\"first\":\"Ryan\",\"nombre\":\"null\"}]";
		String response = con.getRecords("actor","first=Ryan");
		System.out.println("RESPONSE ="+response);
		assertEquals(expectedRes,response);
	}
	
	
}
