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

import xdev.db.DBException;
import xdev.db.jdbc.JDBCDataSource;


public class DB2iJDBCDataSource extends JDBCDataSource<DB2iJDBCDataSource, DB2iDbms>
{
	public DB2iJDBCDataSource()
	{
		super(new DB2iDbms());
	}
	
	
	@Override
	public Parameter[] getDefaultParameters()
	{
		// DB2 for i has no catalogs, only schemas (= folders) -> file based
		return new Parameter[]{HOST.clone(),PORT.clone(50000),USERNAME.clone("db2admin"),
				PASSWORD.clone(),SCHEMA.clone(),URL_EXTENSION.clone(),IS_SERVER_DATASOURCE.clone(),
				SERVER_URL.clone(),AUTH_KEY.clone()};
	}
	
	
	@Override
	protected DB2iConnectionInformation getConnectionInformation()
	{
		return new DB2iConnectionInformation(getHost(),getPort(),getUserName(),getPassword()
				.getPlainText(),getSchema(),getUrlExtension(),getDbmsAdaptor());
	}
	
	
	@Override
	public DB2iJDBCConnection openConnectionImpl() throws DBException
	{
		return new DB2iJDBCConnection(this);
	}
	
	
	@Override
	public DB2iJDBCMetaData getMetaData() throws DBException
	{
		return new DB2iJDBCMetaData(this);
	}
	
	
	@Override
	public boolean canExport()
	{
		return false;
	}
}
