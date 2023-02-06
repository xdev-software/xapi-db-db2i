package xdev.db.db2i.jdbc;

/*-
 * #%L
 * DB2i
 * %%
 * Copyright (C) 2003 - 2023 XDEV Software
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-3.0.html>.
 * #L%
 */


import com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation;
import xdev.db.ConnectionInformation;

import java.sql.Connection;
public class DB2iConnectionInformation extends ConnectionInformation<DB2iDbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	/**
	 * @param user
	 *            the user
	 * @param password
	 *            the password
	 * @param schema
	 *            the schema
	 * @param urlExtension
	 *            the extended url properties
	 * @param dbmsAdaptor
	 *            the dbms adaptor
	 */
	public DB2iConnectionInformation(final String host, final int port, final String user,
			final String password, final String schema, final String urlExtension,
			final DB2iDbms dbmsAdaptor)
	{
		super(host,port,user,password,schema,urlExtension,dbmsAdaptor);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// getters //
	// ///////////////////
	/**
	 * Gets the schema.
	 * 
	 * @return the schema
	 */
	public String getSchema()
	{
		return this.getCatalog();
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// setters //
	// ///////////////////
	/**
	 * Sets the database.
	 * 
	 * @param schema
	 *            the schema to set
	 */
	public void setSchema(final String schema)
	{
		this.setCatalog(schema);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	/**
	 * @see DbmsConnectionInformation#createJdbcConnectionUrl()
	 */
	@Override
	public String createJdbcConnectionUrl()
	{
		String url = "jdbc:as400://" + getHost() + ":" + getPort() + "/" + getSchema();
		return appendUrlExtension(url);
	}
	
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation#getJdbcDriverClassName()
	 */
	@Override
	public String getJdbcDriverClassName()
	{
		return "com.ibm.as400.access.AS400JDBCDriver";
	}
	
	
	@Override
	public boolean isConnectionValid(Connection connection)
	{
		try
		{
			return !connection.isClosed();
		}
		catch(Exception ignored)
		{
			// We'll just ignore the exception here.
			// If the try throws an exception, we know, that we have no valid connection.
			// Therefore, we can simply return false instead of handling the error.
		}
		
		return false;
	}
}
