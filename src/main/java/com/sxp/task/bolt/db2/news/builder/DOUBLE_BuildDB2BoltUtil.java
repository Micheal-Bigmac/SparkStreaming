package com.sxp.task.bolt.db2.news.builder;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.hsae.rdbms.db2.DB2ConnectionPool;
import com.sxp.task.protobuf.generated.DeviceParmsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class DOUBLE_BuildDB2BoltUtil {
	private static final Logger LOG = LoggerFactory.getLogger(DOUBLE_BuildDB2BoltUtil.class);
	static String path = "D:\\EclipseWorkspace\\EclipseWorkspace4Hadoop2\\tsp-storm\\src\\main\\java\\com\\hsae\\storm\\bolt\\db2\\news\\";
	static String packageName = "com.hsae.storm.bolt.db2.news";
	static DatabaseMetaData metaData = null;
	static {
		try {
			metaData = DB2ConnectionPool.getInstance().getConnection().getMetaData();
		} catch (SQLException e) {
			LOG.error("init connection error", e);
		}
	}

	public static void main(String[] args) {
		FileDescriptor descriptor = DeviceParmsInfo.getDescriptor();
		String tableName = "T_VEHICLECONFIGSELECTLOG";
		String subTableName = "T_VEHICLECONFIGSELECTLOGDETAILS";
		int messageTypeIndex = 1;
		int messageTypeIndexSub = 0;
		try {
			buildDB2BoltJavaSource(descriptor, tableName, subTableName, messageTypeIndex, messageTypeIndexSub);
		} catch (Exception e) {
			LOG.error("build DB2Bolt Java Source error", e);
		}
	}

	public static void buildDB2BoltJavaSource(FileDescriptor descriptor, String tableName, String subTableName, int MessageTypeIndex, int MessageTypeIndexSub) throws Exception {
		String pbJavaClassName = descriptor.getOptions().getJavaOuterClassname();// GpsInfo
		String pbPackageName = descriptor.getOptions().getJavaPackage();// com.hsae.storm.proto

		// 父类

		Descriptor pbClass = descriptor.getMessageTypes().get(MessageTypeIndex);
		String pbClassName = pbClass.getFullName();// Can
		String pbClassNameLower = pbClassName.toLowerCase();// can
		String className = pbClassName + "DB2Bolt";
		List<FieldDescriptor> fields = pbClass.getFields();
		ResultSet colRet = metaData.getColumns(null, "DB2INST1", tableName, "%");
		List<String[]> columnss = new ArrayList<String[]>();
		while (colRet.next()) {
			String columnName = colRet.getString("COLUMN_NAME");
			String columnType = colRet.getString("TYPE_NAME");
			int datasize = colRet.getInt("COLUMN_SIZE");
			columnss.add(new String[] { columnName, columnType, datasize + "" });
		}
		if (columnss.size() > fields.size()) {
			// throw new Exception("sql pb is not same,columnss.size():" + columnss.size() + " != fields.size():" + fields.size());
		}

		// 子类
		Descriptor pbSubClass = descriptor.getMessageTypes().get(MessageTypeIndexSub);
		String pbSubClassName = pbSubClass.getFullName();// CanDetail
		String pbSubClassNameLower = pbSubClassName.toLowerCase();// canDetail
		List<FieldDescriptor> fieldsSub = pbSubClass.getFields();
		ResultSet colRetSub = metaData.getColumns(null, "DB2INST1", subTableName, "%");
		List<String[]> columnssSub = new ArrayList<String[]>();
		while (colRetSub.next()) {
			String columnNameSub = colRetSub.getString("COLUMN_NAME");
			String columnTypeSub = colRetSub.getString("TYPE_NAME");
			int datasizeSub = colRetSub.getInt("COLUMN_SIZE");
			columnssSub.add(new String[] { columnNameSub, columnTypeSub, datasizeSub + "" });
		}

		StringBuilder sb = new StringBuilder();
		// head
		sb.append("package " + packageName + ";" + "\n" + //
				"import java.sql.Types;" + "\n" + //
				"import java.util.ArrayList;" + "\n" + //
				"import java.util.HashMap;" + "\n" + //
				"import java.util.List;" + "\n" + //
				"import java.util.Map;" + "\n\n" + //
				"import org.slf4j.Logger;" + "\n" + //
				"import org.slf4j.LoggerFactory;" + "\n\n" + //
				"import com.google.protobuf.InvalidProtocolBufferException;" + "\n" + //
				"import com.hsae.storm.bolt.db2.AbstractDB2Bolt;" + "\n" + //
				"import com.hsae.storm.db2.Column;" + "\n" + //
				"import com.hsae.storm.db2.DB2Utils;" + "\n" + //
				"import com.hsae.storm.db2.PreparedInsertSqlAndValues;" + "\n" + //
				"import " + pbPackageName + "." + pbJavaClassName + "." + pbClassName + ";\n" + //
				"import " + pbPackageName + "." + pbJavaClassName + "." + pbSubClassName + ";\n\n");//
		// fields
		sb.append("public class " + className + " extends AbstractDB2Bolt {" + "\n\n" + //
				"private static final Logger LOG = LoggerFactory.getLogger(" + className + ".class);" + "\n" + //
				"static String TABLE_NAME = \"").append(tableName).append("\";\n");//

		// COLUMNS
		sb.append("public static Column[] COLUMNS = new Column[] {");
		for (int i = 0; i < columnss.size(); i++) {
			String[] c = columnss.get(i);
			if (i < columnss.size() - 1) {
				if (needScaleOrLength(c[1], c[2])) {
					sb.append("new Column(\"" + c[0] + "\", Types." + c[1] + ", " + c[2] + "),// " + (i + 1));
				} else {
					sb.append("new Column(\"" + c[0] + "\", Types." + c[1] + "),// " + (i + 1));
				}
				sb.append("\n");
			} else {
				if (needScaleOrLength(c[1], c[2])) {
					sb.append("new Column(\"" + c[0] + "\", Types." + c[1] + ", " + c[2] + ")// " + (i + 1) + "\n};\n");
				} else {
					sb.append("new Column(\"" + c[0] + "\", Types." + c[1] + ")// " + (i + 1) + "\n};\n");
				}
			}
		}

		// 父类
		sb.append("public static final String INSERT_PREPARED_SQL = DB2Utils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);\n");
		// 子类
		sb.append("static String SUB_TABLE_NAME = \"").append(subTableName).append("\";\n");// 子表名
		// COLUMNSSUB
		sb.append("public static Column[] COLUMNS_SUB = new Column[] {");
		for (int i = 0; i < columnssSub.size(); i++) {
			String[] c = columnssSub.get(i);
			if (i < columnssSub.size() - 1) {
				if (needScaleOrLength(c[1], c[2])) {
					sb.append("new Column(\"" + c[0] + "\", Types." + c[1] + ", " + c[2] + "),// " + (i + 1));
				} else {
					sb.append("new Column(\"" + c[0] + "\", Types." + c[1] + "),// " + (i + 1));
				}
				sb.append("\n");
			} else {
				if (needScaleOrLength(c[1], c[2])) {
					sb.append("new Column(\"" + c[0] + "\", Types." + c[1] + ", " + c[2] + ")// " + (i + 1) + "\n};\n");
				} else {
					sb.append("new Column(\"" + c[0] + "\", Types." + c[1] + ")// " + (i + 1) + "\n};\n");
				}
			}
		} // 子类
		sb.append("public static final String SUB_INSERT_PREPARED_SQL = DB2Utils.buildPreparedInsertSql(SUB_TABLE_NAME, COLUMNS_SUB);\n");

		// function buildPreparedSqlAndValuesList
		sb.append("@Override" + "\n" + //
				"protected List<PreparedInsertSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> " + pbClassNameLower + "List) throws InvalidProtocolBufferException {"
				+ "\n" + //
				"List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();" + "\n" + //
				"List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();" + "\n" + //
				"for (int i = 0, size = " + pbClassNameLower + "List.size(); i < size; i++) {" + "\n" + //
				"try {" + "\n" + //
				"" + pbClassName + " " + pbClassNameLower + " = " + pbClassName + ".parseFrom(" + pbClassNameLower + "List.get(i));" + "\n" + //
				"s.add(buildSqlObjectListFrom" + pbClassName + "(" + pbClassNameLower + "));" + "\n" + //
				"sD.addAll(buildSqlObjectListFrom" + pbSubClassName + "(" + pbClassNameLower + "));" + "\n" + //
				"} catch (InvalidProtocolBufferException e) {" + "\n" + //
				"LOG.error(\"Protobuf parse " + pbClassName + " error\", e);" + "\n" + //
				"}" + "\n" + //
				"}" + "\n" + //
				"List<PreparedInsertSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedInsertSqlAndValues>();" + "\n" + //
				"preparedSqlAndValuesList.add(new PreparedInsertSqlAndValues(INSERT_PREPARED_SQL, COLUMNS, s));" + "\n" + //
				"preparedSqlAndValuesList.add(new PreparedInsertSqlAndValues(SUB_INSERT_PREPARED_SQL, COLUMNS_SUB, sD));" + "\n" + //
				"return preparedSqlAndValuesList;" + "\n" + //
				"}" + "\n");
		// function buildSqlObjectListFromGps
		sb.append("	private static Map<Integer, Object> buildSqlObjectListFrom" + pbClassName + "(" + pbClassName + " " + pbClassNameLower + ") {" + "\n" + //
				"Map<Integer, Object> m = new HashMap<Integer, Object>();");
		for (int i = 0; i < Math.min(columnss.size(), fields.size()); i++) {
			FieldDescriptor field = fields.get(i);
			if (!field.isOptional()) {
				if (field.getType().name().equals("BOOL")) {
					sb.append("m.put(" + field.getNumber() + ", " + pbClassNameLower + ".get" + field.getName() + "() ? 1 : 0);");
				} else if (columnss.get(i)[1].equals("TIMESTAMP")) {
					sb.append("m.put(" + field.getNumber() + ", new java.sql.Date(" + pbClassNameLower + ".get" + field.getName() + "()));");
				} else {
					sb.append("m.put(" + field.getNumber() + ", " + pbClassNameLower + ".get" + field.getName() + "());");
				}
			} else {
				if (field.getType().name().equals("BOOL")) {
					sb.append("m.put(" + field.getNumber() + ", " + pbClassNameLower + ".has" + field.getName() + "() ? " + pbClassNameLower + ".get" + field.getName()
							+ "() ? 1 : 0 : null);");
				} else if (columnss.get(i)[1].equals("TIMESTAMP")) {
					sb.append("m.put(" + field.getNumber() + ", " + pbClassNameLower + ".has" + field.getName() + "() ? new java.sql.Date(" + pbClassNameLower + ".get"
							+ field.getName() + "()) : null);");
				} else {
					sb.append("m.put(" + field.getNumber() + ", " + pbClassNameLower + ".has" + field.getName() + "() ? " + pbClassNameLower + ".get" + field.getName()
							+ "() : null);");
				}
			}
			sb.append("\n");
		}
		sb.append("return m;\n}\n");

		// function buildSqlObjectListFromCanDetails
		sb.append("	private static List<Map<Integer, Object>> buildSqlObjectListFrom" + pbSubClassName + "(" + pbClassName + " " + pbClassNameLower + ") {" + "\n" + //
				"List<" + pbSubClassName + "> itemsList = " + pbClassNameLower + ".getItemsList();\nList<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();");
		sb.append("for (" + pbSubClassName + " " + pbSubClassNameLower + " : itemsList) {\nMap<Integer, Object> m = new HashMap<Integer, Object>();\n");
		for (int i = 0; i < Math.min(columnssSub.size(), fieldsSub.size()); i++) {
			FieldDescriptor field = fieldsSub.get(i);
			if (!field.isOptional()) {
				if (field.getType().name().equals("BOOL")) {
					sb.append("m.put(" + field.getNumber() + ", " + pbSubClassNameLower + ".get" + field.getName() + "() ? 1 : 0);");
				} else if (columnssSub.get(i)[1].equals("TIMESTAMP")) {
					sb.append("m.put(" + field.getNumber() + ", new java.sql.Date(" + pbSubClassNameLower + ".get" + field.getName() + "()));");
				} else {
					sb.append("m.put(" + field.getNumber() + ", " + pbSubClassNameLower + ".get" + field.getName() + "());");
				}
			} else {
				if (field.getType().name().equals("BOOL")) {
					sb.append("m.put(" + field.getNumber() + ", " + pbSubClassNameLower + ".has" + field.getName() + "() ? " + pbSubClassNameLower + ".get" + field.getName()
							+ "() ? 1 : 0 : null);");
				} else if (columnssSub.get(i)[1].equals("TIMESTAMP")) {
					sb.append("m.put(" + field.getNumber() + ", " + pbSubClassNameLower + ".has" + field.getName() + "() ? new java.sql.Date(" + pbSubClassNameLower + ".get"
							+ field.getName() + "()) : null);");
				} else {
					sb.append("m.put(" + field.getNumber() + ", " + pbSubClassNameLower + ".has" + field.getName() + "() ? " + pbSubClassNameLower + ".get" + field.getName()
							+ "() : null);");
				}
			}
		}
		sb.append("sD.add(m);\n}\n");
		sb.append("return sD;\n}\n}");
		write(path + className + ".java", sb.toString());
		LOG.info("writed java source to " + path + className + ".java");
	}

	private static boolean needScaleOrLength(String type, String size) {
		int s = Integer.parseInt(size);
		if ((type.equals("TINYINT") && s < 3) || //
				(type.equals("SMALLINT") && s < 5) || //
				(type.equals("INTEGER") && s < 10) || //
				(type.equals("BIGINT") && s < 19) || //
				(type.equals("FLOAT") && s < 16) || //
				(type.equals("DOUBLE") && s < 53) || //
				(type.equals("CHAR")) || //
				(type.equals("VARCHAR")) || //
				(type.equals("LONGVARCHAR")) || //
				(type.equals("BINARY")) || //
				(type.equals("VARBINARY")) || //
				(type.equals("LONGVARBINARY"))) {
			return true;
		}
		return false;
	}

	private static void write(String file, String content) {
		FileWriter fw = null;
		try {
			fw = new FileWriter(file);
			fw.write(content);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static int sqlType(String ss) {
		int i = Types.NULL;
		switch (ss) {
		case "BIT":
			i = Types.BIT;
			break;
		case "TINYINT":
			i = Types.TINYINT;
			break;
		case "SMALLINT":
			i = Types.SMALLINT;
			break;
		case "INTEGER":
			i = Types.INTEGER;
			break;
		case "BIGINT":
			i = Types.BIGINT;
			break;
		case "FLOAT":
			i = Types.FLOAT;
			break;
		case "REAL":
			i = Types.REAL;
			break;
		case "DOUBLE":
			i = Types.DOUBLE;
			break;
		case "NUMERIC":
			i = Types.NUMERIC;
			break;
		case "DECIMAL":
			i = Types.DECIMAL;
			break;
		case "CHAR":
			i = Types.CHAR;
			break;
		case "VARCHAR":
			i = Types.VARCHAR;
			break;
		case "LONGVARCHAR":
			i = Types.LONGVARCHAR;
			break;
		case "DATE":
			i = Types.DATE;
			break;
		case "TIME":
			i = Types.TIME;
			break;
		case "TIMESTAMP":
			i = Types.TIMESTAMP;
			break;
		case "BINARY":
			i = Types.BINARY;
			break;
		case "VARBINARY":
			i = Types.VARBINARY;
			break;
		case "LONGVARBINARY":
			i = Types.LONGVARBINARY;
			break;
		case "NULL":
			i = Types.NULL;
			break;
		case "OTHER":
			i = Types.OTHER;
			break;
		case "JAVA_OBJECT":
			i = Types.JAVA_OBJECT;
			break;
		case "DISTINCT":
			i = Types.DISTINCT;
			break;
		case "STRUCT":
			i = Types.STRUCT;
			break;
		case "ARRAY":
			i = Types.ARRAY;
			break;
		case "BLOB":
			i = Types.BLOB;
			break;
		case "CLOB":
			i = Types.CLOB;
			break;
		case "REF":
			i = Types.REF;
			break;
		case "DATALINK":
			i = Types.DATALINK;
			break;
		case "BOOLEAN":
			i = Types.BOOLEAN;
			break;
		case "ROWID":
			i = Types.ROWID;
			break;
		default:
			break;
		}
		return i;
	}
}
