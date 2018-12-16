package edu.northeastern.cs5200;

import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 
 * @author sanketsaurav
 *
 */
@CrossOrigin(origins = "*")
@RestController
public class GenericController {

	/**
	 * 
	 * @param table
	 * @param id
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@RequestMapping(value = "/api/{table}/{id}", method = RequestMethod.GET)
	public String getRecordsByid(@PathVariable("table") String table, @PathVariable("id") String id)
			throws SQLException, ClassNotFoundException {
		GenericDao dao = new GenericDao();
		return dao.getByid(table, id);
	}

	/**
	 * 
	 * @param table
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@RequestMapping(value = "/api/{table}", method = RequestMethod.GET)
	public String getRecords(@PathVariable("table") String table) throws SQLException, ClassNotFoundException {
		GenericDao dao = new GenericDao();
		return dao.getFromTable(table);
	}

	/**
	 * 
	 * @param table
	 * @param predicates
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@RequestMapping(value = "/api/{table}?{predicates}", method = RequestMethod.GET)
	public String getRecords(@PathVariable("table") String table, @RequestParam("predicates") String predicates)
			throws SQLException, ClassNotFoundException {
		GenericDao dao = new GenericDao();
		return dao.getFromTable(table, predicates);
	}

	/**
	 * 
	 * @param table
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@RequestMapping(value = "/api/{table}", method = RequestMethod.DELETE)
	public Integer deleteTable(@PathVariable("table") String table) throws SQLException, ClassNotFoundException {
		GenericDao dao = new GenericDao();
		return dao.deleteTable(table);
	}

	/**
	 * 
	 * @param table
	 * @param id
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@RequestMapping(value = "/api/{table}/{id}", method = RequestMethod.DELETE)
	public Integer deleteRecordsByid(@PathVariable("table") String table, @PathVariable("id") String id)
			throws SQLException, ClassNotFoundException {
		GenericDao dao = new GenericDao();
		return dao.deleteByid(table, id);

	}

	/**
	 * 
	 * @param table
	 * @param id
	 * @param body
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@RequestMapping(value = "/api/{table}/{id}", method = RequestMethod.PUT)
	public String updateRecordsbyid(@PathVariable("table") String table, @PathVariable("id") String id,
			@RequestBody String body) throws SQLException, ClassNotFoundException {
		GenericDao dao = new GenericDao();
		String res = dao.update(table, body, id);
		return res;
	}

	/**
	 * 
	 * @param table
	 * @param body
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@RequestMapping(value = "/api/{table}", method = RequestMethod.POST)
	public String createTable(@PathVariable("table") String table, @RequestBody String body)
			throws SQLException, ClassNotFoundException {
		GenericDao dao = new GenericDao();
		return dao.create(table, body);
	}

	/**
	 * 
	 * @param table
	 * @param body
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@RequestMapping(value = "/api/{table1}/{id1}/{table2}/{id2}", method = RequestMethod.POST)
	public String createMapping(@PathVariable("table1") String table1, @PathVariable("id1") String id1,
			@PathVariable("table2") String table2, @PathVariable("id2") String id2)
			throws SQLException, ClassNotFoundException {
		GenericDao dao = new GenericDao();
		return dao.createMapping(table1, id1, table2, id2);
	}


	/**
	 * 
	 * @param table
	 * @param body
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@RequestMapping(value = "/api/{table1}/{id1}/{table2}", method = RequestMethod.GET)
	public String getFromMappingPred(@PathVariable("table1") String table1, @PathVariable("id1") String id1,
			@PathVariable("table2") String table2, @RequestParam Map<String, String> predicates)
			throws SQLException, ClassNotFoundException {
		System.out.println("new new new ");
		GenericDao dao = new GenericDao();
		StringBuilder sb = new StringBuilder();
		for(Entry<String,String> entry:  predicates.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			sb.append(key+"="+value+"&");
		}
		if(predicates.size() > 0 )
			return dao.getFromMapping(table1, id1, table2,sb.toString());
		else
			return dao.getFromMapping(table1, id1, table2);
	}

	/**
	 * 
	 * @param table
	 * @param id
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@RequestMapping(value = "/api/{table1}/{id1}/{table2}/{id2}", method = RequestMethod.DELETE)
	public Integer deleteMappingByid(@PathVariable("table1") String table1, @PathVariable("id1") String id1,
			@PathVariable("table2") String table2, @PathVariable("id2") String id2)
			throws SQLException, ClassNotFoundException {
		GenericDao dao = new GenericDao();
		return dao.deleteMappingByid(table1, id1,table2, id2);

	}
	
	/**
	 * 
	 * @param table
	 * @param id
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@RequestMapping(value = "/api/{table1}/{id1}/{table2}", method = RequestMethod.DELETE)
	public Integer deleteMappingByid(@PathVariable("table1") String table1, @PathVariable("id1") String id1,
			@PathVariable("table2") String table2)
			throws SQLException, ClassNotFoundException {
		GenericDao dao = new GenericDao();
		return dao.deleteMappingByid(table1, id1,table2);

	}
}
