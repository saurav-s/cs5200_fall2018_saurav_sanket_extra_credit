package edu.northeastern.cs5200;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.util.CollectionUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class GenericDao {
	private Connection connection = null;
	private JsonParser parser = new JsonParser();
	public static final String EQ = "=";
	public static final String NEQ = "!=";
	public static final String NEQS = "<>";
	public static final String LT = "<";
	public static final String GT = ">";
	public static final String LTE = "<=";
	public static final String GTE = ">=";

	/**
	 * Constructor
	 */
	public GenericDao() {
		try {
			this.connection = ConnectionFactory.getConnection();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param tName
	 * @param body
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public String update(String tName, String body, String id) throws SQLException {
		List<String> columns = new ArrayList<>();
		List<String> values = new ArrayList<>();
		JsonObject jsonObj = parser.parse(body).getAsJsonObject();
		for (Map.Entry<String, JsonElement> entry : jsonObj.entrySet()) {
			columns.add(entry.getKey());
			values.add(entry.getValue().toString());
		}
		boolean tableExists = handleTableSchema(tName, columns, false);
		if (isTableReadyForUpdate(tableExists, tName, id)) {
			updateRecords(tName, columns, values, id);
			return findRecord(tName, columns, values);
		} else {
			return null;
		}
	}

	/**
	 * 
	 * @param tableExists
	 * @param tName
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	private boolean isTableReadyForUpdate(boolean tableExists, String tName, String id) throws SQLException {
		return (tableExists && getByid(tName, id) != null);
	}

	/**
	 * 
	 * @param tName
	 * @param columns
	 * @return if table exists or not
	 * @throws SQLException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean handleTableSchema(String tName, List columns, boolean createTableFlag) throws SQLException {
		ResultSet rs = null;
		ResultSetMetaData metaData = null;
		boolean tableExists = false;
		try (Statement stmt = connection.createStatement();) {
			rs = stmt.executeQuery("SELECT * FROM " + tName);
			tableExists = true;
			metaData = rs.getMetaData();
		} catch (SQLException e) {
			System.out.println("Table does not exists : possibly expected behavior ");
		}
		if (tableExists) {
			Set<String> remainingColumns = filterNewColumns(columns, metaData);
			if (!CollectionUtils.isEmpty(remainingColumns)) {
				alterTable(tName, remainingColumns);
			}
		} else {
			if (createTableFlag) {
				createTable(tName, columns);
				tableExists = true;
			}
		}
		return tableExists;
	}

	/**
	 * 
	 * @param tName
	 * @param columns
	 * @throws SQLException
	 */
	public void createTable(String tName, List<String> columns) throws SQLException {
		String createSql = "CREATE TABLE " + tName + " (id INT(10) NOT NULL AUTO_INCREMENT ";
		for (String col : columns) {
			createSql += ", " + col + " VARCHAR(256)";
		}
		createSql += " ,PRIMARY KEY (id) )";
		try (Statement stmt = connection.createStatement();) {
			stmt.executeUpdate(createSql);
		}
	}

	/**
	 * 
	 * @param tName
	 * @param keys
	 * @param values
	 * @throws SQLException
	 */
	public void insert(String tName, List<String> keys, List<String> values) throws SQLException {
		String insertSql = "INSERT INTO " + tName + "( ";
		insertSql = updateColumns(keys, insertSql);
		insertSql += ") VALUES ( ";
		insertSql = updateColumns(values, insertSql);
		insertSql += ")";
		try (Statement stmt = connection.createStatement();) {
			stmt.executeUpdate(insertSql);
		}
	}

	/**
	 * 
	 * @param tName
	 * @param body
	 * @return
	 * @throws SQLException
	 */
	public String create(String tName, String body) throws SQLException {
		JsonObject jsonObj = parser.parse(body).getAsJsonObject();
		List<String> columns = new ArrayList<>();
		List<String> values = new ArrayList<>();
		for (Map.Entry<String, JsonElement> entry : jsonObj.entrySet()) {
			columns.add(entry.getKey());
			values.add(entry.getValue().toString());
		}
		handleTableSchema(tName, columns, true);
		insert(tName, columns, values);
		return findRecord(tName, columns, values);
	}

	/**
	 * 
	 * @param tName
	 * @param columns
	 * @param values
	 * @return
	 * @throws SQLException
	 */
	public String findRecord(String tName, List<String> columns, List<String> values) throws SQLException {
		Statement stmt = connection.createStatement();
		ResultSet rs = null;
		String find = "select * from " + tName + " where ";

		for (int i = 0; i < columns.size(); i++) {
			if (i != columns.size() - 1)
				find += columns.get(i) + "=" + values.get(i) + " and ";
			else
				find += columns.get(i) + "=" + values.get(i);
		}
		System.out.println("find query= " + find);
		try {
			rs = stmt.executeQuery(find);
		} catch (Exception e) {
			return null;
		}
		ResultSetMetaData rsmd = rs.getMetaData();
		JsonArray res = new JsonArray();
		parseJson(rs, rsmd, res);
		return res.size() == 0 ? null : res.get(0).toString();
	}

	/**
	 * 
	 * @param rs
	 * @param rsmd
	 * @param res
	 * @throws SQLException
	 */
	private void parseJson(ResultSet rs, ResultSetMetaData rsmd, JsonArray res) throws SQLException {
		while (rs.next()) {
			String temp = "{";
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				if (i != rsmd.getColumnCount())
					temp += "\"" + rsmd.getColumnName(i) + "\":\"" + rs.getString(rsmd.getColumnName(i)) + "\",";
				else
					temp += "\"" + rsmd.getColumnName(i) + "\":\"" + rs.getString(rsmd.getColumnName(i)) + "\"";
			}
			temp += "}";
			System.out.println(temp);
			res.add(parser.parse(temp).getAsJsonObject());
		}
	}

	/**
	 * 
	 * @param tName
	 * @param newColumns
	 * @return
	 * @throws SQLException
	 */
	public int alterTable(String tName, Set<String> newColumns) throws SQLException {
		String ALTER_TABLE_QUERY = "ALTER TABLE " + tName;
		for (String k : newColumns) {
			ALTER_TABLE_QUERY += " ADD COLUMN " + k + "  VARCHAR(256),";
		}
		ALTER_TABLE_QUERY = ALTER_TABLE_QUERY.substring(0, ALTER_TABLE_QUERY.length() - 1);
		Statement stmt = connection.createStatement();
		return stmt.executeUpdate(ALTER_TABLE_QUERY);
	}

	/**
	 * 
	 * @param tName
	 * @param keys
	 * @param values
	 * @param id
	 * @throws SQLException
	 */
	public void updateRecords(String tName, List<String> keys, List<String> values, String id) throws SQLException {
		String insertIntoTable = "UPDATE " + tName + " set ";
		for (int i = 0; i < keys.size(); i++) {
			insertIntoTable += keys.get(i) + "= " + values.get(i) + ",";
		}
		insertIntoTable = insertIntoTable.substring(0, insertIntoTable.length() - 1);
		insertIntoTable += " where id = " + id;
		System.out.println("insert query generated = " + insertIntoTable);
		Statement stmt = connection.createStatement();
		stmt.executeUpdate(insertIntoTable);
	}

	/**
	 * 
	 * @param tName
	 * @return
	 * @throws SQLException
	 */
	public String getFromTable(String tName) throws SQLException {
		Statement stmt = connection.createStatement();
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery("select * from " + tName);
		} catch (Exception e) {
			return null;
		}
		ResultSetMetaData rsmd = rs.getMetaData();
		JsonArray res = new JsonArray();
		parseJson(rs, rsmd, res);
		return res.toString();
	}



	/**
	 * 
	 * @param rs
	 * @param columns
	 * @param res
	 * @throws SQLException
	 */
	private void parseJsonAllKey(ResultSet rs, Set<String> columns, JsonArray res) throws SQLException {
		while (rs.next()) {
			String temp = "{";
			for (String column : columns) {
				temp += "\"" + column + "\" : \"" + rs.getString(column) + "\",";
			}
			temp = temp.substring(0, temp.length() - 1);
			temp += "}";
			System.out.println(temp);
			JsonObject jsonObj = parser.parse(temp).getAsJsonObject();
			res.add(jsonObj);
		}
	}

	/**
	 * 
	 * @param tName
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public String getByid(String tName, String id) throws SQLException {
		Statement stmt = connection.createStatement();
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery("select * from " + tName + " where id =  " + id);
		} catch (Exception e) {
			return null;
		}
		ResultSetMetaData rsmd = rs.getMetaData();
		Set<String> e = new HashSet<>();
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			e.add(rsmd.getColumnName(i));
		}
		JsonArray res = new JsonArray();
		parseJsonAllKey(rs, e, res);
		return res.size() == 0 ? null : res.get(0).toString();
	}

	/**
	 * 
	 * @param tName
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public Integer deleteByid(String tName, String id) throws SQLException {
		int rs = -1;
		try (Statement stmt = connection.createStatement();) {
			rs = stmt.executeUpdate("DELETE  from " + tName + " where id =  " + id);
		} catch (Exception e) {
			return null;
		}
		return rs == 0 ? null : rs;
	}

	/**
	 * 
	 * @param tName
	 * @return
	 * @throws SQLException
	 */
	public Integer deleteTable(String tName) throws SQLException {
		try (Statement stmt = connection.createStatement();) {
			return stmt.executeUpdate("DROP table " + tName);
		}
	}

	/**
	 * 
	 * @param tName
	 * @return
	 * @throws SQLException
	 */
	public Integer deleteAllFromTable(String tName) throws SQLException {
		try (Statement stmt = connection.createStatement();) {
			return stmt.executeUpdate("DELETE from " + tName);
		}
	}

	/**
	 * 
	 * @param tName
	 * @param predicates
	 * @return
	 * @throws SQLException
	 */
	public String getFromTable(String tName, String predicates) throws SQLException {
		ResultSetMetaData rsmd=null;
		ResultSet rs=null;
		String[] preds = predicates.split("&");
		Statement stmt = connection.createStatement();
		try {
			String query = "select * from " + tName + " where ";
			System.out.println("Query = " + query);
			for (int i = 0; i < preds.length; i++) {
				String predicate = preds[i];
				String operator = getOperator(predicate);
				String[] params = predicate.split(operator);
				if (i != preds.length - 1) {
					query = query + " " + params[0] + operator + " '" + params[1] + "'" + " AND ";
				} else {
					query = query + " " + params[0] + operator + " '" + params[1] + "'";
				}
			}
			System.out.println("\n\n\nNew Query = " + query);
			rs = stmt.executeQuery(query);
			rsmd = rs.getMetaData();

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		JsonArray res = new JsonArray();
		parseJson(rs, rsmd, res);
		rs.close();
		stmt.close();
		return res.toString();
	}

	/**
	 * 
	 * @param s
	 * @return
	 */
	private String getOperator(String s) {
		if (s.contains(LTE)) {
			return LTE;
		} else if (s.contains(GTE)) {
			return GTE;
		} else if (s.contains(NEQS)) {
			return NEQS;
		} else if (s.contains(NEQ)) {
			return NEQ;
		} else if (s.contains(EQ)) {
			return EQ;
		} else if (s.contains(LT)) {
			return LT;
		} else if (s.contains(GT)) {
			return GT;
		} else {
			throw new RuntimeException("operation not supported");
		}
	}

	/**
	 * 
	 * @param columns
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Set<String> filterNewColumns(List columns, ResultSetMetaData rsmd) throws SQLException {
		Set<String> colSet = new HashSet<>(columns);
		for (int i = 2; i <= rsmd.getColumnCount(); i++) {
			if (colSet.contains(rsmd.getColumnName(i)))
				colSet.remove(rsmd.getColumnName(i));
		}
		return colSet;
	}

	/**
	 * 
	 * @param keys
	 * @param insertSql
	 * @return
	 */
	private String updateColumns(List<String> keys, String insertSql) {
		for (int i = 0; i < keys.size(); i++) {
			if (i != keys.size() - 1)
				insertSql = insertSql + keys.get(i) + ", ";
			else
				insertSql = insertSql + keys.get(i);
		}
		return insertSql;
	}

	/**
	 * 
	 * @param table1
	 * @param id1
	 * @param table2
	 * @param id2
	 * @param body
	 * @return
	 * @throws SQLException 
	 */
	public String createMapping(String table1, String id1, String table2, String id2) throws SQLException {
		try {
			handleMappingTableSchema(table1, table2);
			return insertInMapping(table1, id1, table2, id2);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 
	 * @param table1
	 * @param id1
	 * @param table2
	 * @param id2
	 * @throws SQLException
	 */
	private String insertInMapping(String table1, String id1, String table2, String id2) throws SQLException {
		String mapTabName = getMappingTableName(table1, table2);
		// find logic
		String mapping = findMapping(table1, id1, table2, id2);
		if ( mapping == null) {
			String insertSql = "INSERT INTO " + mapTabName + "( ";
			insertSql += getMappingColumnName(table1) + ", " + getMappingColumnName(table2);
			insertSql += ") VALUES ( ";
			insertSql += id1 + "," + id2;
			insertSql += " )";
			try (Statement stmt = connection.createStatement();) {
				System.out.println("insert sql = "+insertSql);
				stmt.executeUpdate(insertSql);
			}catch(SQLException e) {
				e.printStackTrace();
				throw e;
			}
			return findMapping(table1, id1, table2, id2);
		}else {
			return mapping;
		}
	}
	
	
	
	private String findMapping(String table1, String id1, String table2, String id2) throws SQLException{
		String mapTabName = getMappingTableName(table1,table2);
		String findMapping="select * from "+mapTabName+" where "+getMappingColumnName(table1)+"="+id1+" and "+getMappingColumnName(table2)+"="+id2;
		ResultSet rs = null;
		ResultSetMetaData rsmd=null;
		Statement stmt =connection.createStatement();
		try{
		 System.out.println("find query  ="+findMapping);
		 rs = stmt.executeQuery(findMapping);
		 rsmd = rs.getMetaData();
		}catch(SQLException e) {
			e.printStackTrace();
			throw e;
		}
		JsonArray res = new JsonArray();
		parseJson(rs, rsmd, res);
		return res.size() == 0 ? null : res.get(0).toString();
	}

//	public static void main(String[] args) throws SQLException {
//		GenericDao dao = new GenericDao();
//		try {
//			//dao.handleMappingTableSchema("movie", "actor");
//			dao.getFromMapping("actor", "4", "movie");
//		}catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
	
	private void handleMappingTableSchema(String table1, String table2) throws SQLException {
		boolean tablesExists = tablesExists(table1, table2);
		if (tablesExists) {
			String mappingTable =  findMappingTable(table1, table2);
			if(mappingTable.equals("none")) {
				//create table (ascending order)
				createOrderedMappingTable(table1,table2);
			}else {
				//mapping table exists	
				System.out.println("mapping table exits");
			}
		} 
	}
	


	private int createOrderedMappingTable(String table1, String table2) throws SQLException {
		if(isAscending(table1, table2)) {
			return createMappingTable(table1,table2);
		}else {
			return createMappingTable(table2,table1);
		}
	}

	private boolean isAscending(String table1, String table2) {
		return table1.compareTo(table2) <= 0;
	}
	
	
	

	private int createMappingTable(String table1, String table2) throws SQLException {
		/**
		 * 
		 * CREATE TABLE Mapping ( actor_id INT NOT NULL, movie_id INT NOT NULL,
		 * CONSTRAINT pk_act_mov PRIMARY KEY (actor_id, movie_id), CONSTRAINT fk_actor
		 * FOREIGN KEY (actor_id) REFERENCES actor(id), CONSTRAINT fk_movie FOREIGN KEY
		 * (movie_id) REFERENCES movie(id) )
		 * 
		 */
		String id1 = getMappingColumnName(table1);
		String id2 = getMappingColumnName(table2);
		String createSql = "CREATE TABLE " + table1 + "_" + table2 + " (" + 
				id1 + " INT NOT NULL, " 
				+ id2 + " INT NOT NULL, " +
				"CONSTRAINT pk_" + table1 + "_" + table2 +
				" PRIMARY KEY (" + id1 + "," + id2 + "), " +
				" CONSTRAINT fk_" + table1 + "\n" +
				"  FOREIGN KEY (" + id1 + ")\n" +
				"  REFERENCES "+ table1 + "(id) ON DELETE CASCADE,\n" +
				" CONSTRAINT fk_" + table2 + "\n" +
				"  FOREIGN KEY (" + id2 + ")\n" +
				"	REFERENCES " + table2 + "(id) ON DELETE CASCADE"+
				" )";

		try (Statement stmt = connection.createStatement();) {
			System.out.println(createSql);
			return stmt.executeUpdate(createSql);
		}
	}

	private String getMappingColumnName(String table1) {
		String id1 = table1 + "_id";
		return id1;
	}

	private boolean tablesExists(String table1, String table2) throws SQLException {
		boolean tablesExists = false;
		try (Statement stmt = connection.createStatement();) {
			stmt.executeQuery("SELECT * FROM " + table1);
			stmt.executeQuery("SELECT * FROM " + table2);
			tablesExists = true;
		} catch (SQLException e) {
			System.out.println("One of the Table does not exists.");
			throw e;
		}
		return tablesExists;
	}

	private String findMappingTable(String table1, String table2) throws SQLException {
		Statement stmt = connection.createStatement();
		
		try{
			stmt.executeQuery("SELECT * FROM " + getMappingTableName(table1, table2));
			return table1+"_"+table2;
		} catch (SQLException e) {
			//check if it exists other way
//			try {
//				Statement stmt1 = connection.createStatement();
//				stmt1.executeQuery("SELECT * FROM " + table2+"_"+table1);
//				return table2+"_"+table1;
//			}catch(SQLException e2) {
				return "none";
			//}
		}
	}
	
	private String getMappingTableName(String table1,String table2) {
		if(isAscending(table1,table2)) {
			return table1+"_"+table2;
		}else {
			return table2+"_"+table1;
		}
	}

	public String getFromMapping(String table1, String id1, String table2) throws SQLException {
		String mapTabName = getMappingTableName(table1, table2);
		String sql = "select t.* from " + table2 +" t"+
				" JOIN " + mapTabName + " map  ON map." + getMappingColumnName(table2)+"=t.id "+
				" JOIN " + table1 + " tt ON tt.id=map."
				+ getMappingColumnName(table1)+
				" where tt.id="+id1;

		System.out.println("get mappin query = "+sql);
		Statement stmt = connection.createStatement();
		JsonArray res = new JsonArray();
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(sql);

		} catch (Exception e) {
			e.printStackTrace();
			return res.toString();
		}
		ResultSetMetaData rsmd = rs.getMetaData();
		parseJson(rs, rsmd, res);
		return res.toString();
	}

	public String getFromMapping(String table1, String id1, String table2, String predicates) throws SQLException {
		String[] preds = predicates.split("&");
		String mapTabName = getMappingTableName(table1, table2);
		String query = "select t.* from " + table2 +" t"+
				" JOIN " + mapTabName + " map  ON map." + getMappingColumnName(table2)+"=t.id "+
				" JOIN " + table1 + " tt ON tt.id=map."
				+ getMappingColumnName(table1)+
				" where tt.id="+id1+" AND ";

		//add predicate
		for (int i = 0; i < preds.length; i++) {
			String predicate = preds[i];
			String operator = getOperator(predicate);
			String[] params = predicate.split(operator);
			if (i != preds.length - 1) {
				query = query + " " + params[0] + operator + " '" + params[1] + "'" + " AND ";
			} else {
				query = query + " " + params[0] + operator + " '" + params[1] + "'";
			}
		}
		System.out.println("get predicate mapping query = "+query);
		Statement stmt = connection.createStatement();
		JsonArray res = new JsonArray();
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(query);

		} catch (Exception e) {
			e.printStackTrace();
			return res.toString();
		}
		ResultSetMetaData rsmd = rs.getMetaData();
		parseJson(rs, rsmd, res);
		return res.toString();
	}

	public Integer deleteMappingByid(String table1, String id1, String table2, String id2) {
		int rs = -1;
		String mapTabName = getMappingTableName(table1, table2);
		try (Statement stmt = connection.createStatement();) {
			rs = stmt.executeUpdate("DELETE  from " + mapTabName + " where "
					+getMappingColumnName(table1)+" =  " + id1 + " AND "+ 
					getMappingColumnName(table2)+" = "+ id2);
		} catch (Exception e) {
			return null;
		}
		return rs == 0 ? null : rs;
	}
	
	public Integer deleteMappingByid(String table1, String id1, String table2) {
		int rs = -1;
		String mapTabName = getMappingTableName(table1, table2);
		try (Statement stmt = connection.createStatement();) {
			rs = stmt.executeUpdate("DELETE  from " + mapTabName + " where "
					+getMappingColumnName(table1)+" =  " + id1);
		} catch (Exception e) {
			return null;
		}
		return rs == 0 ? null : rs;
	}
	

	

}
