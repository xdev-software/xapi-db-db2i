/*
 * SqlEngine Database Adapter DB2i - XAPI SqlEngine Database Adapter for DB2i
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package xdev.db.db2i.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;

import xdev.db.ColumnMetaData;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.Result;
import xdev.db.StoredProcedure;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCDataSource;
import xdev.db.jdbc.JDBCMetaData;
import xdev.db.sql.Functions;
import xdev.db.sql.SELECT;
import xdev.db.sql.Table;
import xdev.util.ProgressMonitor;



public class DB2iJDBCMetaData extends JDBCMetaData
{
	private static final long	serialVersionUID	= 2862594319338582561L;
	
	/**
	 * Client property used to verify the primary key of the table<br>
	 */
	public static Object		DDS_TABLE			= "DDS_Table";
	/**
	 * Client property used to verify the primary key of the view<br>
	 */
	public static Object		DDS_VIEW			= "DDS_View";
	
	public DB2iJDBCMetaData(final DB2iJDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	@Override
	protected String getCatalog(final JDBCDataSource dataSource)
	{
		return null;
	}
	
	@Override
	public TableInfo[] getTableInfos(final ProgressMonitor monitor, final EnumSet<TableType> types)
		throws DBException
	{
		monitor.beginTask("", ProgressMonitor.UNKNOWN);
		
		final List<TableInfo> list = new ArrayList();
		
		final JDBCConnection jdbcConnection = (JDBCConnection)this.dataSource.openConnection();
		
		try
		{
			final String schema = this.getSchema(this.dataSource);
			
			final String tableTypeStatement = this.getTableTypeStatement(types);
			
			final String sql;
			if(schema != null && schema.length() > 0)
			{
				
				sql = "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM QSYS2.SYSTABLES WHERE SYSTEM_TABLE = 'N' AND TABLE_SCHEMA = '"
						+ schema + "' AND " + tableTypeStatement + " FOR READ ONLY";
			}
			else
			{
				sql = "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM QSYS2.SYSTABLES WHERE SYSTEM_TABLE = 'N' "
						+ " AND " + tableTypeStatement + " FOR READ ONLY";
			}
			
			final Result rs = jdbcConnection.query(sql);
			
			while(rs.next() && !monitor.isCanceled())
			{
				final String type = rs.getString("TABLE_TYPE");
				
				final TableType tableType;
				
				// SQL-Tables
				if(type.equalsIgnoreCase("T"))
				{
					tableType = TableType.TABLE;
				}
				// DDS-Tables
				else if(type.equalsIgnoreCase("P"))
				{
					tableType = TableType.TABLE;
				}
				// SQL-View
				else if(type.equalsIgnoreCase("V"))
				{
					tableType = TableType.VIEW;
				}
				// DDS-View
				else if(type.equalsIgnoreCase("L"))
				{
					tableType = TableType.VIEW;
				}
				else
				{
					tableType = TableType.OTHER;
				}
				
				if(types.contains(tableType))
				{
					final TableInfo tInfo = new TableInfo(tableType, rs.getString("TABLE_SCHEMA"),
						rs.getString("TABLE_NAME"));
					
					// DDS-Tables
					if(type.equalsIgnoreCase("P"))
					{
						tInfo.putClientProperty(DDS_TABLE,true);
					}
					// DDS-View
					else if(type.equalsIgnoreCase("L"))
					{
						tInfo.putClientProperty(DDS_VIEW,true);
					}
					
					list.add(tInfo);
				}
				
			}
			
			rs.close();
			
		}
		catch(final DBException ex)
		{
			throw ex;
		}
		finally
		{
			jdbcConnection.close();
		}
		
		monitor.done();
		
		final TableInfo[] tables = list.toArray(new TableInfo[list.size()]);
		Arrays.sort(tables);
		return tables;
	}
	
	private String getTableTypeStatement(final EnumSet<TableType> types)
	{
		
		if(types == null || types.isEmpty())
		{
			return "";
		}
		
		String tableStatement = "(";
		
		if(types.contains(TableType.TABLE))
		{
			tableStatement += "TABLE_TYPE = 'T' OR TABLE_TYPE ='P'";
		}
		
		if(types.contains(TableType.TABLE) && types.contains(TableType.VIEW))
		{
			tableStatement += " OR ";
		}
		
		if(types.contains(TableType.VIEW))
		{
			tableStatement += "TABLE_TYPE = 'V' OR TABLE_TYPE ='L'";
		}
		
		tableStatement += ")";
		return tableStatement;
	}
	
	
	@Override
	protected TableMetaData getTableMetaData(
		final JDBCConnection jdbcConnection,
		final DatabaseMetaData meta,
		final int flags,
		final TableInfo table
	) throws DBException, SQLException
	{
		final String catalog = this.getCatalog(this.dataSource);
		final String schema = table.getSchema();
		
		final String tableName = table.getName();
		final Table tableIdentity = new Table(schema, tableName, null);
		
		final Map<String, Object> defaultValues = new HashMap<>();
		final Map<String, Boolean> autoIncrements = new HashMap<>();
		
		ResultSet rs = meta.getColumns(catalog, schema, tableName, null);
		fillDefaultValuesAndAutoIncrements(defaultValues, autoIncrements, rs);
		
		final Map<String, ColumnMetaData> columnMap = new HashMap<>();
		
		final SELECT select = new SELECT().FROM(tableIdentity).WHERE("1 = 0");
		
		Result result = jdbcConnection.query(select);
		int cc = result.getColumnCount();
		final ColumnMetaData[] columns = new ColumnMetaData[cc];
		this.initColumns(tableName, defaultValues, autoIncrements, columnMap, result, cc, columns);
		
		final StringBuilder sb = new StringBuilder();
		buildContentOfStringBuilder(defaultValues, columnMap, sb);
		
		if(sb.length() > 0)
		{
			try
			{
				final String defaultValueQuery = "SELECT " + sb.toString()
					+ " FROM SYSIBM.TABLES FETCH FIRST 1 ROWS ONLY";
				result = jdbcConnection.query(defaultValueQuery);
				if(result.next())
				{
					cc = result.getColumnCount();
					for(int i = 0; i < cc; i++)
					{
						final String columnName = result.getMetadata(i).getName();
						final ColumnMetaData column = columnMap.get(columnName);
						if(column != null)
						{
							if(column.isAutoIncrement())
							{
								column.setDefaultValue(null);
							}
							else
							{
								Object defaultValue = result.getObject(i);
								defaultValue = this.checkDefaultValue(defaultValue, column);
								column.setDefaultValue(defaultValue);
							}
						}
					}
				}
				result.close();
			}
			catch(final DBException e)
			{
				throw e;
			}
		}
		
		final Map<IndexInfo, Set<String>> indexMap = new Hashtable<>();
		int count = UNKNOWN_ROW_COUNT;
		
		if(table.getType() == TableType.TABLE)
		{
			final Set<String> primaryKeyColumns = new HashSet<>();
			rs = meta.getPrimaryKeys(catalog,schema,tableName);
			while(rs.next())
			{
				primaryKeyColumns.add(rs.getString("COLUMN_NAME"));
			}
			rs.close();
			
			if((flags & INDICES) != 0)
			{
				if(!primaryKeyColumns.isEmpty())
				{
					indexMap.put(new IndexInfo("PRIMARY_KEY", Index.IndexType.PRIMARY_KEY),
							primaryKeyColumns);
				}
				
				rs = meta.getIndexInfo(catalog,schema,tableName,false,true);
				while(rs.next())
				{
					final String indexName = rs.getString("INDEX_NAME");
					final String columnName = rs.getString("COLUMN_NAME");
					if(indexName != null && columnName != null
						&& !primaryKeyColumns.contains(columnName))
					{
						final boolean unique = !rs.getBoolean("NON_UNIQUE");
						
						IndexInfo info = null;
						// If the table is a dds table and a unique index then
						// defined as PK
						// Impl. 2012-05-14 (FHAE)
						if(table.getClientProperty(DDS_TABLE) != null
								&& (Boolean)table.getClientProperty(DDS_TABLE))
						{
							if(unique)
							{
								info = new IndexInfo(indexName, Index.IndexType.PRIMARY_KEY);
							}
							else
							{
								info = new IndexInfo(indexName, Index.IndexType.NORMAL);
							}
						}
						else
						{
							info = new IndexInfo(indexName,unique ? Index.IndexType.UNIQUE
									: Index.IndexType.NORMAL);
						}
						
						Set<String> columnNames = indexMap.get(info);
						if(columnNames == null)
						{
							columnNames = new HashSet<>();
							indexMap.put(info,columnNames);
						}
						columnNames.add(columnName);
					}
				}
				rs.close();
			}
			
			if((flags & ROW_COUNT) != 0)
			{
				try
				{
					result = jdbcConnection.query(new SELECT().columns(Functions.COUNT()).FROM(
							tableIdentity));
					if(result.next())
					{
						count = result.getInt(0);
					}
					result.close();
				}
				catch(final DBException e)
				{
					throw e;
				}
			}
		}
		
		final Index[] indices = new Index[indexMap.size()];
		fillIndices(indexMap, indices);
		
		final TableMetaData tableMeta = new TableMetaData(table, columns, indices, count);
		cutStartingAndEndingBackslashFromColumn(tableMeta);
		
		return tableMeta;
	}
	
	private static void buildContentOfStringBuilder(
		final Map<String, Object> defaultValues,
		final Map<String, ColumnMetaData> columnMap,
		final StringBuilder sb)
	{
		for(final String columnName : defaultValues.keySet())
		{
			final Object defaultValue = defaultValues.get(columnName);
			final ColumnMetaData column = columnMap.get(columnName);
			if(column.isAutoIncrement())
			{
				continue;
			}
			if("NULL".equalsIgnoreCase(String.valueOf(defaultValue)))
			{
				column.setDefaultValue(null);
				continue;
			}
			
			// Impl. 2012-05-14 (FHAE)
			if("".equalsIgnoreCase(String.valueOf(defaultValue)))
			{
				column.setDefaultValue("");
				continue;
			}
			
			if(sb.length() > 0)
			{
				sb.append(", ");
			}
			sb.append(defaultValue);
			sb.append(" AS \"");
			sb.append(columnName);
			sb.append("\"");
		}
	}
	
	private void initColumns(
		final String tableName,
		final Map<String, Object> defaultValues,
		final Map<String, Boolean> autoIncrements,
		final Map<String, ColumnMetaData> columnMap,
		final Result result,
		final int cc,
		final ColumnMetaData[] columns) throws DBException
	{
		for(int i = 0; i < cc; i++)
		{
			final ColumnMetaData column = result.getMetadata(i);
			final String name = column.getName();
			
			Object defaultValue = column.getDefaultValue();
			if(defaultValue == null && defaultValues.containsKey(name))
			{
				defaultValue = defaultValues.get(name);
			}
			defaultValue = this.checkDefaultValue(defaultValue, column);
			
			Boolean autoIncrement = autoIncrements.get(name);
			if(autoIncrement == null)
			{
				autoIncrement = column.isAutoIncrement();
			}
			
			columns[i] = new ColumnMetaData(tableName,name,column.getCaption(),column.getType(),
					column.getLength(),column.getScale(),defaultValue,column.isNullable(),
					autoIncrement);
			columnMap.put(name, columns[i]);
		}
		result.close();
	}
	
	private static void fillDefaultValuesAndAutoIncrements(
		final Map<String, Object> defaultValues,
		final Map<String, Boolean> autoIncrements,
		final ResultSet rs)
		throws SQLException
	{
		while(rs.next())
		{
			final String columnName = rs.getString("COLUMN_NAME");
			final Object defaultValue = rs.getObject("COLUMN_DEF");
			String autoIncrement = "";
			try
			{
				autoIncrement = rs.getString("IS_AUTOINCREMENT");
			}
			catch(final SQLException ignored)
			{
			}
			defaultValues.put(columnName, defaultValue);
			autoIncrements.put(columnName, "YES".equalsIgnoreCase(autoIncrement));
		}
		rs.close();
	}
	
	private static void fillIndices(final Map<IndexInfo, Set<String>> indexMap, final Index[] indices)
	{
		int i = 0;
		for(final IndexInfo indexInfo : indexMap.keySet())
		{
			final Set<String> columnList = indexMap.get(indexInfo);
			final String[] indexColumns = columnList.toArray(new String[columnList.size()]);
			indices[i++] = new Index(indexInfo.name, indexInfo.type, indexColumns);
		}
	}
	
	/**
	 * Iterates over given default values of the columns. Finally, removes the \ on the beginning and ending of the
	 * value if there are some.
	 */
	private static void cutStartingAndEndingBackslashFromColumn(final TableMetaData tableMeta)
	{
		for(final ColumnMetaData column : tableMeta.getColumns())
		{
			final Object def = column.getDefaultValue();
			if(def instanceof String)
			{
				final String str = (String)def;
				final int length = str.length();
				if(length >= 2)
				{
					if(str.charAt(0) == '\'' && str.charAt(length - 1) == '\'')
					{
						column.setDefaultValue(str.substring(1, length - 1));
					}
				}
			}
		}
	}
	
	@Override
	public StoredProcedure[] getStoredProcedures(final ProgressMonitor monitor) throws DBException
	{
		monitor.beginTask("", ProgressMonitor.UNKNOWN);
		
		final List<StoredProcedure> list = new ArrayList<>();
		
		try
		{
			final ConnectionProvider connectionProvider = this.dataSource.getConnectionProvider();
			
			try(final Connection connection = connectionProvider.getConnection())
			{
				final DatabaseMetaData meta = connection.getMetaData();
				final String catalog = this.getCatalog(this.dataSource);
				final String schema = this.getSchema(this.dataSource);
				
				final ResultSet rs = meta.getProcedures(catalog, schema, null);
				while(rs.next() && !monitor.isCanceled())
				{
					// skip system procedures
					if(isSystemProcedure(rs))
					{
						continue;
					}
					
					final String name = rs.getString("PROCEDURE_NAME");
					final String description = rs.getString("REMARKS");
					
					DataType returnType = null;
					StoredProcedure.ReturnTypeFlavor returnTypeFlavor = getReturnTypeFlavor(rs);
					
					final List<StoredProcedure.Param> params = new ArrayList<>();
					final ResultSet rsp = meta.getProcedureColumns(catalog, schema, name, null);
					while(rsp.next())
					{
						final DataType dataType = DataType.get(rsp.getInt("DATA_TYPE"));
						final String columnName = rsp.getString("COLUMN_NAME");
						switch(rsp.getInt("COLUMN_TYPE"))
						{
							case DatabaseMetaData.procedureColumnReturn:
								returnTypeFlavor = StoredProcedure.ReturnTypeFlavor.TYPE;
								returnType = dataType;
								break;
							
							case DatabaseMetaData.procedureColumnResult:
								returnTypeFlavor = StoredProcedure.ReturnTypeFlavor.RESULT_SET;
								break;
							
							case DatabaseMetaData.procedureColumnIn:
								params.add(new StoredProcedure.Param(StoredProcedure.ParamType.IN,
									columnName,
									dataType));
								break;
							
							case DatabaseMetaData.procedureColumnOut:
								params.add(new StoredProcedure.Param(StoredProcedure.ParamType.OUT,
									columnName,
									dataType));
								break;
							
							case DatabaseMetaData.procedureColumnInOut:
								params.add(new StoredProcedure.Param(StoredProcedure.ParamType.IN_OUT,
									columnName,
									dataType));
								break;
							default:
								break;
						}
					}
					rsp.close();
					
					list.add(new StoredProcedure(returnTypeFlavor, returnType, name, description,
						params.toArray(new StoredProcedure.Param[params.size()])));
				}
				rs.close();
			}
		}
		catch(final SQLException e)
		{
			throw new DBException(this.dataSource,e);
		}
		
		monitor.done();
		
		return list.toArray(new StoredProcedure[list.size()]);
	}
	
	private static StoredProcedure.ReturnTypeFlavor getReturnTypeFlavor(final ResultSet rs) throws SQLException
	{
		final StoredProcedure.ReturnTypeFlavor returnTypeFlavor;
		final int procedureType = rs.getInt("PROCEDURE_TYPE");
		if(procedureType == DatabaseMetaData.procedureNoResult)
		{
			returnTypeFlavor = StoredProcedure.ReturnTypeFlavor.VOID;
		}
		else
		{
			returnTypeFlavor = StoredProcedure.ReturnTypeFlavor.UNKNOWN;
		}
		return returnTypeFlavor;
	}
	
	private static boolean isSystemProcedure(final ResultSet rs) throws SQLException
	{
		final String procSchem = rs.getString("PROCEDURE_SCHEM");
		return procSchem != null
			&& (procSchem.startsWith("SYS") || procSchem.startsWith("SQLJ"));
	}
	
	@Override
	protected void createTable(final JDBCConnection jdbcConnection, final TableMetaData table)
		throws DBException, SQLException
	{
	}
	
	@Override
	protected void addColumn(
		final JDBCConnection jdbcConnection, final TableMetaData table,
		final ColumnMetaData column, final ColumnMetaData columnBefore, final ColumnMetaData columnAfter)
		throws DBException, SQLException
	{
	}
	
	@Override
	protected void alterColumn(
		final JDBCConnection jdbcConnection, final TableMetaData table,
		final ColumnMetaData column, final ColumnMetaData existing) throws DBException, SQLException
	{
	}
	
	@Override
	public boolean equalsType(final ColumnMetaData clientColumn, final ColumnMetaData dbColumn)
	{
		return false;
	}
	
	@Override
	protected void dropColumn(
		final JDBCConnection jdbcConnection, final TableMetaData table,
		final ColumnMetaData column) throws DBException, SQLException
	{
	}
	
	@Override
	protected void createIndex(final JDBCConnection jdbcConnection, final TableMetaData table, final Index index)
		throws DBException, SQLException
	{
	}
	
	@Override
	protected void dropIndex(final JDBCConnection jdbcConnection, final TableMetaData table, final Index index)
		throws DBException, SQLException
	{
	}
}
