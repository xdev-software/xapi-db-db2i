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

import com.xdev.jadoth.sqlengine.SELECT;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler;


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
public class DB2iDMLAssembler extends StandardDMLAssembler<DB2iDbms>
{
	protected static final String	_FETCH_FIRST_	= " FETCH FIRST ";
	
	
	public DB2iDMLAssembler(final DB2iDbms dbms)
	{
		super(dbms);
	}
	
	
	/**
	 * @see StandardDMLAssembler#assembleSelectRowLimit(SELECT, StringBuilder, int, String, String, int)
	 */
	@Override
	protected StringBuilder assembleSelectRowLimit(final SELECT query, final StringBuilder sb,
			final int flags, final String clauseSeperator, final String newLine,
			final int indentLevel)
	{
		final Integer top = query.getFetchFirstRowCount();
		if(top != null)
		{
			sb.append(_FETCH_FIRST_).append(top).append(_ROWS_ONLY);
		}
		return sb;
	}
}
