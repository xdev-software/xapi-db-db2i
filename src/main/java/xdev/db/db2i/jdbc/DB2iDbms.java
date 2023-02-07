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

import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.internal.DatabaseGateway;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;



public class DB2iDbms
		extends
		DbmsAdaptor.Implementation<DB2iDbms, DB2iDMLAssembler, DB2iDDLMapper, DB2iRetrospectionAccessor, DB2iSyntax>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	
	/** The Constant MAX_VARCHAR_LENGTH. */
	protected static final int		MAX_VARCHAR_LENGTH		= Integer.MAX_VALUE;
	
	protected static final char		IDENTIFIER_DELIMITER	= '"';
	
	public static final DB2iSyntax	SYNTAX					= new DB2iSyntax();
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	public DB2iDbms()
	{
		this(new SQLExceptionParser.Body());
	}
	
	
	/**
	 * @param sqlExceptionParser
	 *            the sql exception parser
	 */
	public DB2iDbms(final SQLExceptionParser sqlExceptionParser)
	{
		super(sqlExceptionParser,false);
		this.setRetrospectionAccessor(new DB2iRetrospectionAccessor(this));
		this.setDMLAssembler(new DB2iDMLAssembler(this));
		this.setSyntax(SYNTAX);
	}
	
	
	/**
	 * @see DbmsAdaptor#createConnectionInformation(String, int, String, String, String, String)
	 */
	@Override
	public DB2iConnectionInformation createConnectionInformation(final String host, final int port,
			final String user, final String password, final String schema, final String properties)
	{
		return new DB2iConnectionInformation(host,port,user,password,schema,properties, this);
	}
	
	
	/**
	 * @param table
	 *            the table
	 * @return the object
	 */
	@Override
	public Object updateSelectivity(final SqlTableIdentity table)
	{
		return null;
	}
	
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#assembleTransformBytes(byte[],
	 *      java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder assembleTransformBytes(final byte[] bytes, final StringBuilder sb)
	{
		return null;
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor.Implementation#getRetrospectionAccessor()
	 */
	@Override
	public DB2iRetrospectionAccessor getRetrospectionAccessor()
	{
		throw new RuntimeException("HSQL Retrospection not implemented yet!");
	}
	
	
	/**
	 * @param dbc
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#initialize(com.xdev.jadoth.sqlengine.internal.DatabaseGateway)
	 */
	@Override
	public void initialize(final DatabaseGateway<DB2iDbms> dbc)
	{
	}
	
	
	/**
	 * @param fullQualifiedTableName
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#rebuildAllIndices(java.lang.String)
	 */
	@Override
	public Object rebuildAllIndices(final String fullQualifiedTableName)
	{
		return null;
	}
	
	
	@Override
	public boolean supportsOFFSET_ROWS()
	{
		return false;
	}
	
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#getMaxVARCHARlength()
	 */
	@Override
	public int getMaxVARCHARlength()
	{
		return MAX_VARCHAR_LENGTH;
	}
	
	
	@Override
	public char getIdentifierDelimiter()
	{
		return IDENTIFIER_DELIMITER;
	}
}
