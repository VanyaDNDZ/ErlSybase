#include "SybStatement.h"


SysLogger* SybStatement::log = NULL;

SybStatement::SybStatement(CS_CONNECTION* conn, ErlNifEnv* env) :
		conn_(conn), sql_(NULL), row_count_(0), desc_dfmt_(NULL), param_count_(
				0), is_prepare_(false), executed_(false), env_(env) {
	id_[0] = '\0';
	if (ct_cmd_alloc(conn_, &cmd_) != CS_SUCCEED) {
		cmd_ = NULL;
	}
	if (!SybStatement::log) {
		SybStatement::log = new SysLogger();
	}
}

SybStatement::SybStatement(CS_CONNECTION* conn, const char* sql, ErlNifEnv* env) :
		conn_(conn), row_count_(0), desc_dfmt_(NULL), param_count_(0), is_prepare_(
				false), executed_(false), env_(env) {

	int len = strlen(sql);
	id_[0] = '\0';
	if (ct_cmd_alloc(conn_, &cmd_) != CS_SUCCEED) {
		cmd_ = NULL;
	}

	sql_ = new char[(len + 1) * sizeof(char)];
	if (sql_) {
		strcpy(sql_, sql);
	}

}

SybStatement::~SybStatement() {
	if (cmd_) {
		/**  Clean up the command handle used */
		(void) ct_cmd_drop(cmd_);
		cmd_ = NULL;
	}
	if (desc_dfmt_) {
		free(desc_dfmt_);
		desc_dfmt_ = NULL;
	}
	if (sql_) {
		free(sql_);
		sql_ = NULL;
	}
}

bool SybStatement::execute_cmd() {
	return execute_cmd(sql_);
}

bool SybStatement::execute_cmd(const char* sql) {
	/** execute_cmd does not support prepare statement */
	if (is_prepare_ || cmd_ == NULL) {
		return false;
	}
	reset();

	/* Store the command string in it, and send it to the server.*/
	if (ct_command(cmd_, CS_LANG_CMD, (CS_CHAR*) sql, CS_NULLTERM,
			CS_UNUSED) != CS_SUCCEED) {
		return false;
	}
	if (ct_send(cmd_) != CS_SUCCEED) {
		return false;
	}

	/** Handle the result, only return success or failure */
	if (handle_command_result(cmd_) != CS_SUCCEED) {
		return false;
	}

	return true;
}

CS_RETCODE SybStatement::handle_command_result() {
	CS_RETCODE retcode;
	CS_INT restype;
	CS_RETCODE query_code = CS_SUCCEED;
	row_count_ = 0;

	/** Examine the results coming back. If any errors are seen, the query
	 * result code (which we will return from this function) will be set to FAIL.
	 */

	while ((retcode = ct_results(cmd_, &restype)) == CS_SUCCEED) {

		switch (restype) {
		case CS_CMD_SUCCEED:
		case CS_CMD_DONE:
			break;

		default:
			/** Unexpected result type. */
			query_code = CS_FAIL;
			break;
		}
		if (query_code == CS_FAIL) {

			/** Terminate results processing and break out of the results loop */
			if (ct_cancel(NULL, cmd_, CS_CANCEL_ALL) != CS_SUCCEED) {
				SysLogger::error("handle_command_result: ct_cancel() failed");
			}
			break;
		}
	}

	if ((int)retcode != (int)CS_END_RESULTS || (int)query_code != (int)CS_SUCCEED) {
		return CS_FAIL;
	}

	return CS_SUCCEED;
}


CS_RETCODE SybStatement::handle_command_result(CS_COMMAND *cmd) {
	
	CS_SMALLINT msg_id;
	CS_RETCODE retcode;
	CS_INT restype;
	CS_RETCODE query_code = CS_SUCCEED;
	row_count_ = 0;

	/** Examine the results coming back. If any errors are seen, the query
	 * result code (which we will return from this function) will be set to FAIL.
	 */

	while ((retcode = ct_results(cmd, &restype)) == CS_SUCCEED) {
		switch ((int)restype) {
		case CS_CMD_SUCCEED:
			break;
		case CS_CMD_DONE:
			break;
		case CS_MSG_RESULT:
			/*
			**  Retrieve and print the message ID.
			*/
			retcode = ct_res_info(cmd, CS_MSGTYPE,
					(CS_VOID *)&msg_id, CS_UNUSED, NULL);
			if (retcode != CS_SUCCEED)
			{
				query_code=CS_FAIL;

			}
			break;

		default:
			/** Unexpected result type. */
			query_code = CS_FAIL;
			break;
		}
		if (query_code == CS_FAIL) {

			/** Terminate results processing and break out of the results loop */
			if (ct_cancel(NULL, cmd, CS_CANCEL_ALL) != CS_SUCCEED) {
			}
			break;
		}
	}
	if ((int)retcode != (int)CS_END_RESULTS || (int)query_code != (int)CS_SUCCEED) {
		return CS_FAIL;
	}

	return CS_SUCCEED;
}

/**
 * If this is a prepare statement then we will use ct_dynamic to execute it
 * or use ct_command
 */
bool SybStatement::execute_sql(ERL_NIF_TERM** result) {
	if (is_prepare_) {
		if (!executed_) {
			if (ct_dynamic(cmd_, CS_EXECUTE, id_, CS_NULLTERM, NULL,
					CS_UNUSED) != CS_SUCCEED) {
				SysLogger::error("execute_sql:ct_dynamic() failed");
				return false;
			}
			executed_ = true;
		}
		if (ct_send(cmd_) != CS_SUCCEED) {
			SysLogger::error("execute_sql:ct_send() failed");
			return false;
		}

		/** Handle the result and encode to ei_x_buff */
		if (handle_sql_result(result) != CS_SUCCEED) {
			return CS_FAIL;
		}
		executed_ = false;
		return CS_SUCCEED;
	} else {
		return execute_sql(result, sql_);
	}
}

int SybStatement::get_param_count() {
	return param_count_;
}

int SybStatement::get_param_type(int index) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return -1;
	} else {
		return desc_dfmt_[index - 1].datatype;
	}
}

/** Execute common sql command */
bool SybStatement::execute_sql(ERL_NIF_TERM** result, const char* sql) {
	if (cmd_ == NULL) {
		return false;
	}
	reset();

	/* Store the command string in it, and send it to the server.*/
	if (ct_command(cmd_, CS_LANG_CMD, (CS_CHAR*) sql, CS_NULLTERM,
			CS_UNUSED) != CS_SUCCEED) {
		SysLogger::error("execute_sql: ct_command() failed");
		return false;
	}
	if (ct_send(cmd_) != CS_SUCCEED) {
		SysLogger::error("execute_sql: ct_send() failed");
		return false;
	}

	/** Handle the result and encode to ei_x_buff */
	if (handle_sql_result(result) != CS_SUCCEED) {
		SysLogger::error("execute_sql: handle_sql_result failed");
		return false;
	}

	return true;
}

bool SybStatement::prepare_init(const char* id) {
	return prepare_init(id, sql_);
}

bool SybStatement::prepare_init(const char* id, const char* sql) {
	CS_RETCODE retcode;
	if (cmd_ == NULL) {
		return false;
	}

	reset();
	if (strlen(id) + 1 > CS_MAX_CHAR) {
		return false;
	} else {
		strcpy(id_, id);
		is_prepare_ = true;
	}

	if (ct_dynamic(cmd_, CS_PREPARE, (CS_CHAR*) id, CS_NULLTERM, (CS_CHAR*) sql,
			CS_NULLTERM) != CS_SUCCEED) {
		SysLogger::debug("prepare_init: ct_dynamic(CS_PREPARE) failed");
		return false;
	}

	if (ct_send(cmd_) != CS_SUCCEED) {
		SysLogger::debug("prepare_init: ct_send() failed");
		return false;
	}


	if ((retcode=handle_command_result(cmd_)) != CS_SUCCEED) {
		SysLogger::debug("prepare_init:handle_command_result error");
		return false;
	}
	
	if (ct_dynamic(cmd_, CS_DESCRIBE_INPUT, (CS_CHAR*) id, CS_NULLTERM, NULL,
			CS_UNUSED) != CS_SUCCEED) {
		SysLogger::debug("prepare_init: ct_dynamic(CS_DESCRIBE_INPUT) failed");
		return false;
	}

	if (ct_send(cmd_) != CS_SUCCEED) {
		SysLogger::debug("prepare_init: ct_send() failed");
		return false;
	}
	
	if (handle_describe_result(cmd_) != CS_SUCCEED) {
		return false;
	}

	return true;
}

CS_RETCODE SybStatement::handle_describe_result() {
	CS_RETCODE retcode;
	CS_INT restype;
	CS_RETCODE query_code = CS_SUCCEED;

	/** Examine the results coming back. If any errors are seen, the query
	 * result code (which we will return from this function) will be set to FAIL.
	 */
	while ((retcode = ct_results(cmd_, &restype)) == CS_SUCCEED) {

		switch (restype) {
		case CS_DESCRIBE_RESULT:
			if (process_describe_reslut() != CS_SUCCEED) {
				return CS_FAIL;
			}
			break;

		case CS_CMD_SUCCEED:
		case CS_CMD_DONE:
			break;

		default:
			/** Unexpected result type. */
			query_code = CS_FAIL;
			break;
		}

		if (query_code == CS_FAIL) {

			/** Terminate results processing and break out of the results loop */
			if (ct_cancel(NULL, cmd_, CS_CANCEL_ALL) != CS_SUCCEED) {
				SysLogger::error("handle_describe_result: ct_cancel() failed");
			}
			break;
		}
	}

	if (retcode != CS_END_RESULTS || query_code != CS_SUCCEED) {
		return CS_FAIL;
	}

	return CS_SUCCEED;
}


CS_RETCODE SybStatement::handle_describe_result(CS_COMMAND *cmd) {
	CS_RETCODE retcode;
	CS_INT restype;
	CS_RETCODE query_code = CS_SUCCEED;

	/** Examine the results coming back. If any errors are seen, the query
	 * result code (which we will return from this function) will be set to FAIL.
	 */
	while ((retcode = ct_results(cmd, &restype)) == CS_SUCCEED) {

		switch ((int)restype) {
		case CS_DESCRIBE_RESULT:
			if (process_describe_reslut() != CS_SUCCEED) {
				return CS_FAIL;
			}
			break;

		case CS_CMD_SUCCEED:
		case CS_CMD_DONE:
			break;

		default:
			/** Unexpected result type. */
			query_code = CS_FAIL;
			break;
		}

		if (query_code == CS_FAIL) {

			/** Terminate results processing and break out of the results loop */
			if (ct_cancel(NULL, cmd, CS_CANCEL_ALL) != CS_SUCCEED) {
				SysLogger::error("handle_describe_result: ct_cancel() failed");
			}
			break;
		}
	}

	if ((int)retcode != (int)CS_END_RESULTS || (int)query_code != (int)CS_SUCCEED) {
		return CS_FAIL;
	}

	return CS_SUCCEED;
}

CS_RETCODE SybStatement::handle_sql_result(ERL_NIF_TERM** result) {
	CS_RETCODE retcode;
	CS_INT restype;
	CS_RETCODE query_code = CS_SUCCEED;
	row_count_ = 0;
	CS_INT is_query = 0;
	ERL_NIF_TERM out;
	/** Examine the results coming back. If any errors are seen, the query
	 * result code (which we will return from this function) will be set to FAIL.
	 */
	while ((retcode = ct_results(cmd_, &restype)) == CS_SUCCEED) {
		switch ((int)restype) {
		case CS_COMPUTE_RESULT:
		case CS_CURSOR_RESULT:
		case CS_PARAM_RESULT:
		case CS_STATUS_RESULT:
		case CS_ROW_RESULT:
			is_query = 1;
			
			out = process_row_result();
			*result = &out;
	 		break;

		case CS_CMD_SUCCEED:
			break;

		case CS_CMD_DONE:
			row_count_ = get_row_count();
			if((int)is_query==0){
				out = enif_make_tuple2(env_,enif_make_atom(env_,"rowupdated"),enif_make_uint(env_,(unsigned int ) row_count_));
				*result = &out;	
			}
			break;

		default:
			/** Unexpected result type. */
			query_code = CS_FAIL;
			break;
		}

		if (query_code == CS_FAIL) {

			/** Terminate results processing and break out of the results loop */
			if (ct_cancel(NULL, cmd_, CS_CANCEL_ALL) != CS_SUCCEED) {
				SysLogger::error("handle_sql_result: ct_cancel() failed");
			}
			break;
		}
	}

	if ((int)retcode != (int)CS_END_RESULTS || (int)query_code != (int)CS_SUCCEED) {
		return CS_FAIL;
	}

	if (is_query == 0) {
		return encode_update_result(*result, row_count_);
	}
	return CS_SUCCEED;
}

CS_RETCODE SybStatement::encode_update_result(ERL_NIF_TERM* result,
		CS_INT row_count) {
	ERL_NIF_TERM out = enif_make_tuple2(env_, enif_make_atom(env_, "ok"),
			enif_make_long(env_, row_count));
	result = &out;
	return CS_SUCCEED;
}

ERL_NIF_TERM SybStatement::process_row_result() {
	
	/** Find out how many columns there are in this result set.*/
	CS_INT column_count = get_column_count();

	/** Make sure we have at least one column. */
	if (column_count <= 0) {
		SysLogger::error("process_row_result: have no columns");
		return cancel_current();
	}

	/** Allocate memory for the data element to process. */
	COLUMN_DATA* columns = (COLUMN_DATA *)malloc(column_count
            * sizeof (COLUMN_DATA));
	if (columns == NULL) {
		SysLogger::error("process_row_result: allocate COLUMN_DATA failed");
		return cancel_current();
	}

	for (CS_INT i = 0; i < column_count; ++i) {

		/**
		 * Get the column description.  ct_describe() fills the
		 * datafmt parameter with a description of the column.
		 */
		CS_DATAFMT *dfmt = &columns[i].dfmt;

		memset((char*)dfmt, 0, sizeof(CS_DATAFMT));
		if (ct_describe(cmd_, (i + 1), dfmt) != CS_SUCCEED) {
			SysLogger::error("process_row_result: ct_describe failed");
			free_column_data(columns, i);
			cancel_current();
			return CS_FAIL;
		}

		columns[i].value = alloc_column_value(dfmt);
		if (columns[i].value == NULL) {
			SysLogger::error("process_row_result: alloc_column_value() failed");
			free_column_data(columns, i);
			cancel_current();
			return CS_FAIL;
		}

		if (ct_bind(cmd_, i + 1, dfmt, (CS_VOID *) columns[i].value,
				&columns[i].valuelen, &columns[i].indicator) != CS_SUCCEED) {
			SysLogger::error("process_row_result: ct_bind() failed");
			free_column_data(columns, i);
			cancel_current();
			return CS_FAIL;
		}
	}
	
	return encode_query_result(columns, column_count);
}

bool SybStatement::set_param(CS_DATAFMT* dfmt, CS_VOID* data, CS_INT len) {
	if (!executed_) {
		if (ct_dynamic(cmd_, CS_EXECUTE, id_, CS_NULLTERM, NULL,
				CS_UNUSED) != CS_SUCCEED) {
			SysLogger::error("set_param: ct_dynamic() failed");
			return false;
		}
		executed_ = true;
	}

	dfmt->status = CS_INPUTVALUE;

	if (ct_param(cmd_, dfmt, (CS_VOID *) data, len, 0) != CS_SUCCEED) {
		SysLogger::error("set_param: ct_param() failed");
		return false;
	}

	return true;
}

ERL_NIF_TERM SybStatement::encode_query_result(COLUMN_DATA* columns, CS_INT column_count) {
	CS_RETCODE retcode;
	CS_INT rows_read;
	CS_INT row_count = 0;

	ERL_NIF_TERM rows = enif_make_list(env_, 0);

	/** Fetch the rows.  Loop while ct_fetch() returns CS_SUCCEED or
	 * CS_ROW_FAIL
	 */
	while (((retcode = ct_fetch(cmd_, CS_UNUSED, CS_UNUSED, CS_UNUSED,
			&rows_read)) == CS_SUCCEED) || (retcode == CS_ROW_FAIL)) {

		/** Increment our row count by the number of rows just fetched. */
		row_count = row_count + rows_read;
		/** Check if we hit a recoverable error. */
		if ((int)retcode == (int)CS_ROW_FAIL) {
			SysLogger::error("encode_query_result: Error on row %d", row_count);
		} else {
			
			/**
			 * We have a row. Loop through the columns encode the
			 * column values.
			 */
			ERL_NIF_TERM* row = new ERL_NIF_TERM[column_count];

			for (CS_INT i = 0; i < column_count; ++i) {
				row[i]=encode_column_data(columns+i);
			}
			rows = enif_make_list_cell(env_,  enif_make_list_from_array(env_,row,column_count),rows);
		}
	}

	
	switch ((int)retcode) {
	case CS_END_DATA:
		retcode = CS_SUCCEED;
		break;

	case CS_FAIL:
		SysLogger::error("encode_query_result: ct_fetch() failed");
		break;

	default:
		/** We got an unexpected return value. */
		SysLogger::error("encode_query_result: ct_fetch() returned an "
				"expected retcode:%d", retcode);
		break;
	}

	return rows;
}

ERL_NIF_TERM SybStatement::encode_binary(CS_DATAFMT* dfmt,
		CS_BINARY* v, CS_INT len) {
	ErlNifBinary* bin = NULL;
	enif_alloc_binary(len, bin);
	bin->data = (unsigned char*) v;
	return enif_make_binary(env_, bin);
}

ERL_NIF_TERM SybStatement::encode_longbinary(CS_DATAFMT* dfmt,
		CS_LONGBINARY* v, CS_INT len) {
	ErlNifBinary* bin = NULL;
	enif_alloc_binary((long) len, bin);
	bin->data = (unsigned char*) v;
	return enif_make_binary(env_, bin);
}

ERL_NIF_TERM SybStatement::encode_varbinary(CS_DATAFMT* dfmt,
		CS_VARBINARY* v) {
	ErlNifBinary* bin = NULL;
	enif_alloc_binary((long) v->len, bin);
	bin->data = (unsigned char*) v->array;
	return enif_make_binary(env_, bin);
}

ERL_NIF_TERM SybStatement::encode_bit(CS_DATAFMT* dfmt,
		CS_BIT* v) {
	return enif_make_string(env_, (char*) v, ERL_NIF_LATIN1);
}

ERL_NIF_TERM SybStatement::encode_char(CS_DATAFMT* dfmt,
		CS_CHAR* v, CS_INT len) {
	return enif_make_string_len(env_, (const char*) v,len, ERL_NIF_LATIN1);
}

ERL_NIF_TERM SybStatement::encode_longchar(CS_DATAFMT* dfmt,
		CS_LONGCHAR* v, CS_INT len) {
	return enif_make_string_len(env_, (const char*) v,len, ERL_NIF_LATIN1);
}

ERL_NIF_TERM SybStatement::encode_varchar( CS_DATAFMT* dfmt,
		CS_VARCHAR* v) {
	return enif_make_string_len(env_, (const char*) v->str,(int)v->len, ERL_NIF_LATIN1);
}

ERL_NIF_TERM SybStatement::encode_unichar(CS_DATAFMT* dfmt,
		CS_UNICHAR* v, CS_INT len) {
	return enif_make_string(env_, (const char*) "UNICHAR", ERL_NIF_LATIN1);
	/*    CS_INT i;

	 len = len / sizeof(CS_UNICHAR);
	 if (ei_x_encode_list_header(x, (long)len)) {
	 return CS_FAIL;
	 }
	 for (i = 0; i < len; ++i) {
	 if(ei_x_encode_ulong(x, (unsigned long)v[i])) {
	 return CS_FAIL;
	 }
	 }
	 ei_x_encode_empty_list(x);

	 return CS_SUCCEED;*/
}

ERL_NIF_TERM SybStatement::encode_xml(CS_DATAFMT* dfmt,
		CS_XML* v, CS_INT len) {
	return enif_make_string(env_, (const char*) v, ERL_NIF_LATIN1);
}

ERL_NIF_TERM SybStatement::encode_date(CS_DATAFMT* dfmt,
		CS_DATE* v) {
	CS_CONTEXT* context;
	CS_DATEREC daterec;

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	if (cs_dt_crack(context, dfmt->datatype, (CS_VOID*) v,
			&daterec) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return  enif_make_tuple2(env_, enif_make_atom(env_, "date"),
			enif_make_tuple3(env_, enif_make_long(env_, daterec.dateyear),
					enif_make_long(env_, daterec.datemonth + 1),
					enif_make_long(env_, daterec.datedmonth)));
}

ERL_NIF_TERM SybStatement::encode_time(CS_DATAFMT* dfmt,
		CS_TIME* v) {
	CS_CONTEXT* context;
	CS_DATEREC daterec;

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	if (cs_dt_crack(context, dfmt->datatype, (CS_VOID*) v,
			&daterec) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return  enif_make_tuple2(env_, enif_make_atom(env_, "time"),
			enif_make_tuple4(env_, enif_make_long(env_, daterec.datehour),
					enif_make_long(env_, daterec.dateminute),
					enif_make_long(env_, daterec.datesecond),
					enif_make_long(env_, daterec.datemsecond)));
}

ERL_NIF_TERM SybStatement::encode_datetime(CS_DATAFMT* dfmt,
		CS_DATETIME* v) {
	CS_CONTEXT* context;
	CS_DATEREC daterec;

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	if (cs_dt_crack(context, dfmt->datatype, (CS_VOID*) v,
			&daterec) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return enif_make_tuple2(env_, enif_make_atom(env_, "datetime"),
			enif_make_tuple2(env_,
					enif_make_tuple3(env_,
							enif_make_long(env_, daterec.dateyear),
							enif_make_long(env_, daterec.datemonth + 1),
							enif_make_long(env_, daterec.datedmonth)),
					enif_make_tuple4(env_,
							enif_make_long(env_, daterec.datehour),
							enif_make_long(env_, daterec.dateminute),
							enif_make_long(env_, daterec.datesecond),
							enif_make_long(env_, daterec.datemsecond))));
}

ERL_NIF_TERM SybStatement::encode_datetime4( CS_DATAFMT* dfmt,
		CS_DATETIME4* v) {
	CS_CONTEXT* context;
	CS_DATEREC daterec;

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	if (cs_dt_crack(context, dfmt->datatype, (CS_VOID*) v,
			&daterec) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return enif_make_tuple2(env_, enif_make_atom(env_, "datetime"),
			enif_make_tuple2(env_,
					enif_make_tuple3(env_,
							enif_make_long(env_, daterec.dateyear),
							enif_make_long(env_, daterec.datemonth + 1),
							enif_make_long(env_, daterec.datedmonth)),
					enif_make_tuple2(env_,
							enif_make_long(env_, daterec.datehour),
							enif_make_long(env_, daterec.dateminute))));
}

ERL_NIF_TERM SybStatement::encode_bigdatetime(CS_DATAFMT* dfmt,
		CS_BIGDATETIME* v) {
	CS_CONTEXT* context;
	CS_DATEREC daterec;

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	if (cs_dt_crack(context, dfmt->datatype, (CS_VOID*) v,
			&daterec) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return enif_make_tuple2(env_,
			enif_make_atom(env_, "bigdatetime"),
			enif_make_tuple2(env_,
					enif_make_tuple3(env_,
							enif_make_long(env_, daterec.dateyear),
							enif_make_long(env_, daterec.datemonth + 1),
							enif_make_long(env_, daterec.datedmonth)),
					enif_make_tuple5(env_,
							enif_make_long(env_, daterec.datehour),
							enif_make_long(env_, daterec.dateminute),
							enif_make_long(env_, daterec.datesecond),
							enif_make_long(env_, daterec.datesecfrac / 1000),
							enif_make_long(env_, daterec.datesecfrac % 1000))));
}

ERL_NIF_TERM SybStatement::encode_bigtime(CS_DATAFMT* dfmt,
		CS_BIGTIME* v) {
	CS_CONTEXT* context;
	CS_DATEREC daterec;

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	if (cs_dt_crack(context, dfmt->datatype, (CS_VOID*) v,
			&daterec) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return enif_make_tuple2(env_, enif_make_atom(env_, "bigtime"),
			enif_make_tuple5(env_, enif_make_long(env_, daterec.datehour),
					enif_make_long(env_, daterec.dateminute),
					enif_make_long(env_, daterec.datesecond),
					enif_make_long(env_, daterec.datesecfrac / 1000),
					enif_make_long(env_, daterec.datesecfrac % 1000)));
}

ERL_NIF_TERM SybStatement::encode_tinyint(CS_DATAFMT* dfmt,
		CS_TINYINT* v) {
	return enif_make_int(env_, (int) *v);
}

ERL_NIF_TERM SybStatement::encode_smallint(CS_DATAFMT* dfmt,
		CS_SMALLINT* v) {
	return enif_make_int(env_, (int) *v);
}

ERL_NIF_TERM SybStatement::encode_int(CS_DATAFMT* dfmt,
		CS_INT* v) {
	return enif_make_long(env_, (long) *v);
}

ERL_NIF_TERM SybStatement::encode_bigint( CS_DATAFMT* dfmt,
		CS_BIGINT* v) {
	return enif_make_long(env_, (long) *v);
}

ERL_NIF_TERM SybStatement::encode_usmallint(CS_DATAFMT* dfmt,
		CS_USMALLINT* v) {
	return enif_make_ulong(env_, (unsigned long) *v);
}

ERL_NIF_TERM SybStatement::encode_uint(CS_DATAFMT* dfmt,
		CS_UINT* v) {
	return enif_make_ulong(env_, (unsigned long) *v);
}

ERL_NIF_TERM SybStatement::encode_ubigint(CS_DATAFMT* dfmt,
		CS_UBIGINT* v) {
	return enif_make_ulong(env_, (unsigned long) *v);
}

ERL_NIF_TERM SybStatement::encode_decimal( CS_DATAFMT* dfmt,
		CS_DECIMAL* v) {
	CS_CONTEXT* context;
	CS_DATAFMT destfmt;
	CS_CHAR dest[79];
	CS_INT destlen;

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	memset(&destfmt, 0, sizeof(CS_DATAFMT));
	destfmt.datatype = CS_CHAR_TYPE;
	destfmt.maxlength = sizeof(dest);
	destfmt.locale = NULL;

	if (cs_convert(context, dfmt, (CS_VOID *) v, &destfmt, dest,
			&destlen) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return enif_make_tuple2(env_, enif_make_atom(env_, "number"),
					enif_make_string_len(env_, (const char*) dest,destlen,
							ERL_NIF_LATIN1));
}

ERL_NIF_TERM SybStatement::encode_numeric(CS_DATAFMT* dfmt,
		CS_NUMERIC* v) {
	CS_CONTEXT* context;
	CS_DATAFMT destfmt;
	CS_CHAR dest[79];
	CS_INT destlen;

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	memset(&destfmt, 0, sizeof(CS_DATAFMT));
	destfmt.datatype = CS_CHAR_TYPE;
	destfmt.maxlength = sizeof(dest);
	destfmt.locale = NULL;

	if (cs_convert(context, dfmt, (CS_VOID *) v, &destfmt, dest,
			&destlen) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return	enif_make_tuple2(env_, enif_make_atom(env_, "number"),
					enif_make_string_len(env_, (const char*) dest,destlen,
							ERL_NIF_LATIN1));
}

ERL_NIF_TERM SybStatement::encode_float(CS_DATAFMT* dfmt,
		CS_FLOAT* v) {
	return enif_make_double(env_, *v);
}

ERL_NIF_TERM SybStatement::encode_real(CS_DATAFMT* dfmt,
		CS_REAL* v) {
		return enif_make_double(env_, *v);
	
}

ERL_NIF_TERM SybStatement::encode_money(CS_DATAFMT* dfmt,
		CS_MONEY* v) {
	CS_CONTEXT* context;
	CS_DATAFMT destfmt;
	CS_CHAR dest[24];
	CS_INT destlen;

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	memset(&destfmt, 0, sizeof(CS_DATAFMT));
	destfmt.datatype = CS_CHAR_TYPE;
	destfmt.maxlength = sizeof(dest);
	destfmt.locale = NULL;

	if (cs_convert(context, dfmt, (CS_VOID *) v, &destfmt, dest,
			&destlen) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return enif_make_tuple2(env_, enif_make_atom(env_, "number"),
					enif_make_string_len(env_, (char*) dest,destlen, ERL_NIF_LATIN1));
}

ERL_NIF_TERM SybStatement::encode_money4(CS_DATAFMT* dfmt,
		CS_MONEY4* v) {
	CS_CONTEXT* context;
	CS_DATAFMT destfmt;
	CS_FLOAT dest;
	CS_INT destlen;

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	memset(&destfmt, 0, sizeof(CS_DATAFMT));
	destfmt.datatype = CS_FLOAT_TYPE;
	destfmt.maxlength = sizeof(CS_FLOAT);
	destfmt.locale = NULL;

	if (cs_convert(context, dfmt, (CS_VOID *) v, &destfmt, &dest,
			&destlen) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return	enif_make_double(env_, dest);
}

ERL_NIF_TERM SybStatement::encode_text( CS_DATAFMT* dfmt,
		CS_TEXT* v, CS_INT len) {
	return enif_make_string_len(env_, (const char*) v,len, ERL_NIF_LATIN1);
}

ERL_NIF_TERM SybStatement::encode_image(CS_DATAFMT* dfmt,
		CS_IMAGE* v, CS_INT len) {
	ErlNifBinary* bin = NULL;
	enif_alloc_binary((long) len, bin);
	bin->data = (unsigned char*) v;
	
	return enif_make_binary(env_, bin);;
}

ERL_NIF_TERM SybStatement::encode_unitext( CS_DATAFMT* dfmt,
		CS_UNITEXT* v, CS_INT len) {
	return enif_make_atom(env_,"error");
	/*CS_INT i;

	 len = len / sizeof(CS_UNICHAR);
	 if (ei_x_encode_list_header(x, (long)len)) {
	 return CS_FAIL;
	 }
	 for (i = 0; i < len; ++i) {
	 if(ei_x_encode_ulong(x, (unsigned long)v[i])) {
	 return CS_FAIL;
	 }
	 }
	 ei_x_encode_empty_list(x);

	 return CS_SUCCEED;*/
}

ERL_NIF_TERM SybStatement::encode_unknown() {
	return enif_make_atom(env_, "unknown");
}

ERL_NIF_TERM SybStatement::encode_null() {
	return enif_make_atom(env_, "undefined");
}

ERL_NIF_TERM SybStatement::encode_overflow() {
	return enif_make_atom(env_, "overflow");
}

ERL_NIF_TERM SybStatement::encode_column_data(COLUMN_DATA *column) {
	CS_DATAFMT *dfmt = &column->dfmt;
	ERL_NIF_TERM encoded;
	if (column->indicator == 0) {
		switch ((int)dfmt->datatype) {

		/** Binary types */
		case CS_BINARY_TYPE:
			encoded = encode_binary(dfmt, (CS_BINARY*) column->value,column->valuelen);
			break;

		case CS_LONGBINARY_TYPE:
			encoded = encode_longbinary(dfmt,(CS_LONGBINARY*) column->value, column->valuelen);
			break;

		case CS_VARBINARY_TYPE:
			encoded = encode_varbinary(dfmt,
					(CS_VARBINARY*) column->value);
			break;

			/** Bit types */
		case CS_BIT_TYPE:
			encoded = encode_bit(dfmt, (CS_BIT*) column->value);
			break;

			/** Character types */
		case CS_CHAR_TYPE:
			encoded = encode_char( dfmt, (CS_CHAR*) column->value,
					column->valuelen);
			
			break;

		case CS_LONGCHAR_TYPE:
			encoded = encode_longchar(dfmt,
					(CS_LONGCHAR*) column->value, column->valuelen);

			break;

		case CS_VARCHAR_TYPE:
			encoded = encode_varchar( dfmt, (CS_VARCHAR*) column->value);
			break;
		case CS_UNICHAR_TYPE:
			encoded = encode_unichar( dfmt, (CS_UNICHAR*) column->value,
					column->valuelen);
	
			break;

		case CS_XML_TYPE:
			encoded = encode_xml( dfmt, (CS_XML*) column->value,
					column->valuelen);

			break;

			/** Datetime types */
		case CS_DATE_TYPE:
			encoded = encode_date( dfmt, (CS_DATE*) column->value);
			break;

		case CS_TIME_TYPE:
			encoded = encode_time( dfmt, (CS_TIME*) column->value);
			break;

		case CS_DATETIME_TYPE:
			encoded = encode_datetime(dfmt,
					(CS_DATETIME*) column->value);
			break;

		case CS_DATETIME4_TYPE:
			encoded = encode_datetime4(dfmt,
					(CS_DATETIME4*) column->value);

			break;

		case CS_BIGDATETIME_TYPE:
			encoded = encode_bigdatetime( dfmt,
					(CS_BIGDATETIME*) column->value);
			break;

		case CS_BIGTIME_TYPE:
			encoded = encode_bigtime( dfmt, (CS_BIGTIME*) column->value);
			break;

			/** Numeric types */
		case CS_TINYINT_TYPE:
			encoded = encode_tinyint( dfmt, (CS_TINYINT*) column->value);
			break;

		case CS_SMALLINT_TYPE:
			encoded = encode_smallint( dfmt,
					(CS_SMALLINT*) column->value);
			break;

		case CS_INT_TYPE:
			encoded = encode_int( dfmt, (CS_INT*) column->value);
			break;

		case CS_BIGINT_TYPE:
			encoded = encode_bigint( dfmt, (CS_BIGINT*) column->value);
			break;

		case CS_USMALLINT_TYPE:
			encoded = encode_usmallint( dfmt,
					(CS_USMALLINT*) column->value);
			break;

		case CS_UINT_TYPE:
			encoded = encode_uint( dfmt, (CS_UINT*) column->value);
			break;

		case CS_UBIGINT_TYPE:
			encoded = encode_ubigint( dfmt, (CS_UBIGINT*) column->value);
			break;

		case CS_DECIMAL_TYPE:
			encoded = encode_decimal( dfmt, (CS_DECIMAL*) column->value);

			break;

		case CS_NUMERIC_TYPE:
			encoded = encode_numeric( dfmt, (CS_NUMERIC*) column->value);
			break;

		case CS_FLOAT_TYPE:
			encoded = encode_float( dfmt, (CS_FLOAT*) column->value);
			break;

		case CS_REAL_TYPE:
			encoded = encode_real(dfmt, (CS_REAL*) column->value);
	
			break;

			/** Money types */
		case CS_MONEY_TYPE:
			encoded = encode_money( dfmt, (CS_MONEY*) column->value);
			break;

		case CS_MONEY4_TYPE:
			encoded = encode_money4( dfmt, (CS_MONEY4*) column->value);
			break;

			/** Text and image types */
		case CS_TEXT_TYPE:
			encoded = encode_text( dfmt, (CS_TEXT*) column->value,
					column->valuelen);
			break;

		case CS_IMAGE_TYPE:
			encoded = encode_image( dfmt, (CS_IMAGE*) column->value,
					column->valuelen);
			break;

		case CS_UNITEXT_TYPE:
			encoded = encode_unitext( dfmt, (CS_UNITEXT*) column->value,
					column->valuelen);
			break;

		default:
			encoded = encode_unknown();
			break;
		}
	} else if (column->indicator == -1) {
		encoded = encode_null();
	} else {

		/* Buffer overflow */
		encoded = encode_overflow();
	}

	return encoded;
}

void SybStatement::reset() {
	if (desc_dfmt_) {
		free(desc_dfmt_);
		desc_dfmt_ = NULL;
	}
	row_count_ = 0;
	param_count_ = 0;
	is_prepare_ = false;
	executed_ = false;
}

CS_RETCODE SybStatement::cancel_current() {
	return ct_cancel(NULL, cmd_, CS_CANCEL_CURRENT);
}

bool SybStatement::set_binary(int index, unsigned char* data,
		unsigned int len) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) data, len);
}

bool SybStatement::decode_and_set_binary(int index, ERL_NIF_TERM data){
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	ErlNifBinary bin;
	enif_inspect_binary(env_,data,&bin);
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	return set_param(dfmt, (CS_VOID*) bin.data, bin.size);
}

bool SybStatement::set_longbinary(int index, unsigned char* data,
		unsigned int len) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) data, len);
}

bool SybStatement::decode_and_set_longbinary(int index, ERL_NIF_TERM data){
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	ErlNifBinary bin;
	enif_inspect_binary(env_,data,&bin);
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	return set_param(dfmt, (CS_VOID*) bin.data, bin.size);
}

bool SybStatement::decode_and_set_varbinary(int index, ERL_NIF_TERM data){
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	CS_VARBINARY varbinary;
	ErlNifBinary bin;
	if(!enif_inspect_binary(env_,data,&bin)){
		return false;
	}
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	if (bin.size > CS_MAX_CHAR) {
		return false;
	} else {
		varbinary.len = bin.size;
		memcpy(varbinary.array, bin.data, bin.size);
	}
	return set_param(dfmt, (CS_VOID*) &varbinary, sizeof(CS_VARBINARY));
}

bool SybStatement::set_varbinary(int index, unsigned char* data,
		unsigned int len) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_VARBINARY varbinary;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	if (len > CS_MAX_CHAR) {
		return false;
	} else {
		varbinary.len = len;
		memcpy(varbinary.array, data, len);
	}

	return set_param(dfmt, (CS_VOID*) &varbinary, sizeof(CS_VARBINARY));
}

bool SybStatement::decode_and_set_bit(int index, ERL_NIF_TERM data){
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	unsigned int size;
	enif_get_list_length(env_,data,&size);
	char* str_buf = (char*) malloc((size+1)*sizeof(CS_CHAR));
	if(!enif_get_string(env_,data,str_buf,size+1,ERL_NIF_LATIN1))
	{
		SysLogger::error("Error get string data");
		return false;
	}
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	return set_param(dfmt, (CS_VOID*) str_buf, CS_NULLTERM);
}

bool SybStatement::set_bit(int index, unsigned char data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::set_char(int index, char* data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) data, CS_NULLTERM);
}


bool SybStatement::decode_and_set_char(int index, ERL_NIF_TERM char_data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	unsigned int size;
	enif_get_list_length(env_,char_data,&size);
	char* str_buf = (char*) malloc((size+1)*sizeof(CS_CHAR));
	if(!enif_get_string(env_,char_data,str_buf,size+1,ERL_NIF_LATIN1))
	{
		SysLogger::error("Error get string data");
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	return set_param(dfmt, (CS_VOID*) str_buf, CS_NULLTERM);
}

bool SybStatement::decode_and_set_longchar(int index, ERL_NIF_TERM char_data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	unsigned int size;
	enif_get_list_length(env_,char_data,&size);
	char* str_buf = (char*) malloc((size+1)*sizeof(CS_CHAR));
	if(!enif_get_string(env_,char_data,str_buf,size+1,ERL_NIF_LATIN1))
	{
		SysLogger::error("Error get string data");
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	return set_param(dfmt, (CS_VOID*) str_buf, CS_NULLTERM);
}


bool SybStatement::set_char(int index, char* data, unsigned int len) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) data, len);
}

bool SybStatement::set_longchar(int index, unsigned char* data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) data, CS_NULLTERM);
}

bool SybStatement::set_longchar(int index, unsigned char* data,
		unsigned int len) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) data, len);
}

bool SybStatement::decode_and_set_varchar(int index, ERL_NIF_TERM char_data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	CS_VARCHAR varchar;
	unsigned int size;
	enif_get_list_length(env_,char_data,&size);
	char* str_buf = (char*) malloc((size+1)*sizeof(CS_CHAR));
	if(!enif_get_string(env_,char_data,str_buf,size+1,ERL_NIF_LATIN1))
	{
		SysLogger::error("Error get string data");
		return false;
	}
	if (size > CS_MAX_CHAR) {
		return false;
	} else {
		varchar.len = size;
		memcpy(varchar.str, str_buf, size);
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	return set_param(dfmt, (CS_VOID*) &varchar, 1);
}

bool SybStatement::set_varchar(int index, unsigned char* data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_VARCHAR varchar;
	CS_INT len = strlen((const char*) data);
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	if (len > CS_MAX_CHAR) {
		return false;
	} else {
		varchar.len = len;
		memcpy(varchar.str, data, len);
	}

	return set_param(dfmt, (CS_VOID*) &varchar, 1);
}

bool SybStatement::set_varchar(int index, unsigned char* data,
		unsigned int len) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_VARCHAR varchar;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	if (len > CS_MAX_CHAR) {
		return false;
	} else {
		varchar.len = len;
		memcpy(varchar.str, data, len);
	}

	return set_param(dfmt, (CS_VOID*) &varchar, 1);
}

bool SybStatement::decode_and_set_unichar(int index, ERL_NIF_TERM char_data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	unsigned int size;
	enif_get_list_length(env_,char_data,&size);
	char* str_buf = (char*) malloc((size+1)*sizeof(CS_CHAR));
	if(!enif_get_string(env_,char_data,str_buf,size+1,ERL_NIF_LATIN1))
	{
		SysLogger::error("Error get string data");
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	return set_param(dfmt, (CS_VOID*) str_buf, CS_NULLTERM);
}

bool SybStatement::set_unichar(int index, unsigned short* data,
		unsigned int len) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) data, len * sizeof(unsigned short));
}

bool SybStatement::decode_and_set_xml(int index, ERL_NIF_TERM char_data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	unsigned int size;
	enif_get_list_length(env_,char_data,&size);
	char* str_buf = (char*) malloc((size+1)*sizeof(CS_CHAR));
	if(!enif_get_string(env_,char_data,str_buf,size+1,ERL_NIF_LATIN1))
	{
		SysLogger::error("Error get string data");
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	return set_param(dfmt, (CS_VOID*) str_buf, CS_NULLTERM);
}

bool SybStatement::set_xml(int index, unsigned char* data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) data, CS_NULLTERM);
}

bool SybStatement::set_xml(int index, unsigned char* data, unsigned int len) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) data, len);
}

bool SybStatement::set_date(int index, int data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::decode_and_set_date(int index, ERL_NIF_TERM data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	
	int size,days;
	
	long int day,month,year;
	
	const ERL_NIF_TERM* tuple;
	
	
	const ERL_NIF_TERM* date;
	

	
	enif_get_tuple(env_,data,&size,&tuple);
	
	
	if(!enif_is_atom(env_,tuple[0])){
		return false;	
	}
	
	
	if(!enif_get_tuple(env_,tuple[1],&size,&date)){
		SysLogger::error("decode_and_set_datetime: cant get date tuple ");
		return false;			

	}

	
	if(!enif_get_long(env_,date[0],&year)){
		return false;
	}
	
	if(!enif_get_long(env_,date[1],&month)){
		return false;
	}
	
	if(!enif_get_long(env_,date[2],&day)){
		return false;
	}
	
	days = date_to_days(year, month, day) - 693961;
	
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	
	return set_param(dfmt, (CS_VOID*) &days, CS_UNUSED);
}

bool SybStatement::set_date(int index, int year, int month, int day) {
	int days;

	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	days = date_to_days(year, month, day) - 693961;

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &days, CS_UNUSED);
}

bool SybStatement::set_time(int index, int data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}


bool SybStatement::decode_and_set_time(int index, ERL_NIF_TERM data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	
	int size,mseconds;
	
	long int hour, minutes, seconds,ms;
	
	const ERL_NIF_TERM* tuple;
	
	const ERL_NIF_TERM* times;
	
	
	enif_get_tuple(env_,data,&size,&tuple);
	
	
	if(!enif_is_atom(env_,tuple[0])){
		return false;	
	}
	
	
	if(!enif_get_tuple(env_,tuple[1],&size,&times)){
		return false;					
	}
	
	if(!enif_get_long(env_,times[0],&hour)){
		return false;
	}
	
	if(!enif_get_long(env_,times[1],&minutes)){
		return false;
	}
	
	if(!enif_get_long(env_,times[2],&seconds)){
		return false;
	}
	
	if(!enif_get_long(env_,times[3],&ms)){
		return false;
	}

	
	mseconds = ((hour * 60 + minutes) * 60 + seconds) * 1000 + ms;
	mseconds = mseconds * 3 / 10;
	
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	
	return set_param(dfmt, (CS_VOID*) &mseconds, CS_UNUSED);
}

bool SybStatement::set_time(int index, int hour, int minutes, int seconds,
		int ms) {
	int mseconds;

	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	mseconds = ((hour * 60 + minutes) * 60 + seconds) * 1000 + ms;

	mseconds = mseconds * 3 / 10;

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &mseconds, CS_UNUSED);
}

bool SybStatement::set_datetime(int index, int days, int time) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATETIME datetime;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	datetime.dtdays = days;
	datetime.dttime = time;

	return set_param(dfmt, (CS_VOID*) &datetime, CS_UNUSED);
}

bool SybStatement::decode_and_set_datetime(int index, ERL_NIF_TERM data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	
	CS_DATETIME datetime;
	
	int size,mseconds,days;
	
	long int day,month,year,hour, minutes, seconds,ms;
	
	const ERL_NIF_TERM* tuple;
	
	const ERL_NIF_TERM* dt_tuple;
	
	const ERL_NIF_TERM* date;
	
	const ERL_NIF_TERM* times;
	
	
	enif_get_tuple(env_,data,&size,&tuple);
	
	
	if(!enif_is_atom(env_,tuple[0])){
		return false;	
	}
	if(!enif_get_tuple(env_,tuple[1],&size,&dt_tuple)){
		return false;		
	}
	
	
	if(!enif_get_tuple(env_,dt_tuple[0],&size,&date)){
		return false;			

	}

	
	if(!enif_get_tuple(env_,dt_tuple[1],&size,&times)){
		return false;					
	}
	
	if(!enif_get_long(env_,date[0],&year)){
		return false;
	}
	
	if(!enif_get_long(env_,date[1],&month)){
		return false;
	}
	
	if(!enif_get_long(env_,date[2],&day)){
		return false;
	}
	
	if(!enif_get_long(env_,times[0],&hour)){
		return false;
	}
	
	if(!enif_get_long(env_,times[1],&minutes)){
		return false;
	}
	
	if(!enif_get_long(env_,times[2],&seconds)){
		return false;
	}
	
	if(!enif_get_long(env_,times[3],&ms)){
		return false;
	}

	days = date_to_days(year, month, day) - 693961;
	
	mseconds = ((hour * 60 + minutes) * 60 + seconds) * 1000 + ms;
	
	datetime.dtdays = days;
	
	datetime.dttime = mseconds * 3 / 10;
	
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	
	return set_param(dfmt, (CS_VOID*) &datetime, CS_UNUSED);
}

bool SybStatement::set_datetime(int index, int year, int month, int day,
		int hour, int minutes, int seconds, int ms) {
	int days;
	int mseconds;

	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATETIME datetime;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	days = date_to_days(year, month, day) - 693961;
	mseconds = ((hour * 60 + minutes) * 60 + seconds) * 1000 + ms;
	datetime.dtdays = days;
	datetime.dttime = mseconds * 3 / 10;

	return set_param(dfmt, (CS_VOID*) &datetime, CS_UNUSED);
}

bool SybStatement::set_datetime4(int index, unsigned short days,
		unsigned short minutes) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATETIME4 datetime4;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	datetime4.days = days;
	datetime4.minutes = minutes;

	return set_param(dfmt, (CS_VOID*) &datetime4, CS_UNUSED);
}

bool SybStatement::set_datetime4(int index, int year, int month, int day,
		int hour, int minutes) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATETIME4 datetime4;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	datetime4.days = date_to_days(year, month, day) - 693961;
	datetime4.minutes = hour * 60 + minutes;

	return set_param(dfmt, (CS_VOID*) &datetime4, CS_UNUSED);
}

bool SybStatement::set_bigdatetime(int index, unsigned long data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::set_bigdatetime(int index, int year, int month, int day,
		int hour, int minutes, int seconds, int ms, int Ms) {
	unsigned long bigdatetime;

	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	bigdatetime = (unsigned long) date_to_days(year, month, day);
	bigdatetime = bigdatetime * 24 + hour;
	bigdatetime = bigdatetime * 60 + minutes;
	bigdatetime = bigdatetime * 60 + seconds;
	bigdatetime = bigdatetime * 1000 + ms;
	bigdatetime = bigdatetime * 1000 + Ms;

	return set_param(dfmt, (CS_VOID*) &bigdatetime, CS_UNUSED);
}

bool SybStatement::set_bigtime(int index, unsigned long data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::set_bigtime(int index, int hour, int minutes, int seconds,
		int ms, int Ms) {
	unsigned long bigtime;

	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	bigtime = hour * 60 + minutes;
	bigtime = bigtime * 60 + seconds;
	bigtime = bigtime * 1000 + ms;
	bigtime = bigtime * 1000 + Ms;

	return set_param(dfmt, (CS_VOID*) &bigtime, CS_UNUSED);
}

bool SybStatement::set_tinyint(int index, unsigned char data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::decode_and_set_tinyint(int index, ERL_NIF_TERM data){

	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	
	int l;
	
	if(!enif_get_int(env_,data,&l)){
		return false;
	}
	if(l>255 || l<0){
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &l, CS_UNUSED);	
}

bool SybStatement::decode_and_set_smallint(int index, ERL_NIF_TERM data){

	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	
	int l;
	
	if(!enif_get_int(env_,data,&l)){
		return false;
	}

	if(l>32767 || l<-32768){
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &l, CS_UNUSED);	
}

bool SybStatement::decode_and_set_real(int index, ERL_NIF_TERM data){

	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	
	double l;
	
	if(!enif_get_double(env_,data,&l)){
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &l, CS_UNUSED);	
}


bool SybStatement::decode_and_set_int(int index, ERL_NIF_TERM data){

	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	
	int l;
	
	if(!enif_get_int(env_,data,&l)){
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &l, CS_UNUSED);	
}

bool SybStatement::decode_and_set_long(int index, ERL_NIF_TERM data){

	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	long l;
	if(!enif_get_long(env_,data,&l)){
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &l, CS_UNUSED);	
}

bool SybStatement::set_smallint(int index, short data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::set_int(int index, int data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::set_bigint(int index, long data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::set_usmallint(int index, unsigned short data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::set_uint(int index, unsigned int data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::set_ubigint(int index, unsigned long data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::set_decimal(int index, char* data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_CONTEXT* context;
	CS_DATAFMT srcfmt;
	CS_DECIMAL dest;
	CS_INT destlen;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	memset(&srcfmt, 0, sizeof(CS_DATAFMT));
	srcfmt.datatype = CS_CHAR_TYPE;
	srcfmt.maxlength = strlen(data);
	srcfmt.format = CS_FMT_NULLTERM;
	srcfmt.locale = NULL;

	if (cs_convert(context, &srcfmt, (CS_VOID *) data, dfmt, &dest,
			&destlen) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return set_param(dfmt, (CS_VOID*) &dest, CS_UNUSED);
}

bool SybStatement::set_decimal(int index, unsigned char precision,
		unsigned char scale, unsigned char* data, unsigned int len) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DECIMAL decimal;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	if (len > CS_MAX_NUMLEN) {
		return false;
	} else {
		memcpy(decimal.array, data, len);
		decimal.precision = precision;
		decimal.scale = scale;
	}

	return set_param(dfmt, (CS_VOID*) &decimal, CS_UNUSED);
}

bool SybStatement::set_numeric(int index, char* data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_CONTEXT* context;
	CS_DATAFMT srcfmt;
	CS_DECIMAL dest;
	CS_INT destlen;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	memset(&srcfmt, 0, sizeof(CS_DATAFMT));
	srcfmt.datatype = CS_CHAR_TYPE;
	srcfmt.maxlength = strlen(data);
	srcfmt.format = CS_FMT_NULLTERM;
	srcfmt.locale = NULL;

	if (cs_convert(context, &srcfmt, (CS_VOID *) data, dfmt, &dest,
			&destlen) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return set_param(dfmt, (CS_VOID*) &dest, CS_UNUSED);
}

bool SybStatement::set_numeric(int index, unsigned char precision,
		unsigned char scale, unsigned char* data, unsigned int len) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_NUMERIC numeric;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	if (len > CS_MAX_NUMLEN) {
		return false;
	} else {
		memcpy(numeric.array, data, len);
		numeric.precision = precision;
		numeric.scale = scale;
	}

	return set_param(dfmt, (CS_VOID*) &numeric, CS_UNUSED);
}

bool SybStatement::decode_and_set_numeric(int index,ERL_NIF_TERM data){
	
	CS_CONTEXT* context;
	CS_DATAFMT srcfmt;
	CS_DECIMAL dest;
	CS_INT destlen;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
	const ERL_NIF_TERM* tuple;
	char* buff;
	unsigned buff_size;
	int tuple_size;

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}
	
	if(!enif_is_tuple(env_,data)){
		return false;
	}

	enif_get_tuple(env_,data,&tuple_size,&tuple);

	if (tuple_size!=2){
		return false;
	}

	enif_get_list_length(env_,tuple[1],&buff_size);
	
	if(!enif_is_list(env_,tuple[1])){
		return false;
	}
	
	buff = (char*) malloc(buff_size+1);

	enif_get_string(env_,tuple[1],buff,buff_size+1,ERL_NIF_LATIN1);


	memset(&srcfmt, 0, sizeof(CS_DATAFMT));
	srcfmt.datatype = CS_CHAR_TYPE;
	srcfmt.maxlength = buff_size;
	srcfmt.format = CS_FMT_NULLTERM;
	srcfmt.locale = NULL;

	if (cs_convert(context, &srcfmt, (CS_VOID *) buff, dfmt, &dest,
			&destlen) != CS_SUCCEED) {
		return CS_FAIL;
	}


	return set_param(dfmt, (CS_VOID*) &dest, CS_UNUSED);
}

bool SybStatement::set_float(int index, double data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::set_real(int index, float data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	return set_param(dfmt, (CS_VOID*) &data, CS_UNUSED);
}

bool SybStatement::set_money(int index, char* data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_CONTEXT* context;
	CS_DATAFMT srcfmt;
	CS_MONEY dest;
	CS_INT destlen;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE, &context, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	memset(&srcfmt, 0, sizeof(CS_DATAFMT));
	srcfmt.datatype = CS_CHAR_TYPE;
	srcfmt.maxlength = strlen(data);
	srcfmt.format = CS_FMT_NULLTERM;
	srcfmt.locale = NULL;

	if (cs_convert(context, &srcfmt, (CS_VOID *) data, dfmt, &dest,
			&destlen) != CS_SUCCEED) {
		return CS_FAIL;
	}

	return set_param(dfmt, (CS_VOID*) &dest, CS_UNUSED);
}

bool SybStatement::set_money(int index, int high, unsigned int low) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_MONEY money;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	money.mnyhigh = high;
	money.mnylow = low;

	return set_param(dfmt, (CS_VOID*) &money, CS_UNUSED);
}

bool SybStatement::set_money4(int index, double data) {
	if (!is_prepare_ || index < 1 || index > param_count_) {
		return false;
	}

	CS_MONEY4 money;
	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	money.mny4 = (int) (data * 10000);

	return set_param(dfmt, (CS_VOID*) &money, CS_UNUSED);
}

bool SybStatement::set_null(int index) {
	if (!is_prepare_ || index < 1 || index > param_count_) {

		return false;
	}

	CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

	if (dfmt->datatype == CS_BIT_TYPE) {
		CS_BIT bit = 0;
		return set_param(dfmt, &bit, CS_UNUSED);
	}

	return set_param(dfmt, NULL, CS_UNUSED);
}

bool SybStatement::prepare_release() {
	if (!is_prepare_) {
		return false;
	}

	/** Deallocate the prepared statement */
	if (ct_dynamic(cmd_, CS_DEALLOC, (CS_CHAR*) id_, CS_NULLTERM, NULL,
			CS_UNUSED) != CS_SUCCEED) {
		return false;
	}

	if (ct_send(cmd_) != CS_SUCCEED) {
		return false;
	}

	if (handle_command_result() != CS_SUCCEED) {
		return false;
	}

	return true;
}

unsigned int SybStatement::get_affected_rows() {
	return (unsigned int) row_count_;
}

CS_RETCODE SybStatement::process_describe_reslut() {
	if (ct_res_info(cmd_, CS_NUMDATA, &param_count_, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return CS_FAIL;
	}

	if (param_count_ <= 0) {
		return CS_SUCCEED;
	}

	desc_dfmt_ = (CS_DATAFMT*) malloc(param_count_ * sizeof(CS_DATAFMT));
	if (desc_dfmt_ == NULL) {
		return CS_FAIL;
	}

	for (CS_INT i = 0; i < param_count_; ++i) {
		if (ct_describe(cmd_, i + 1, desc_dfmt_ + i) != CS_SUCCEED) {
			free(desc_dfmt_);
			desc_dfmt_ = NULL;
			return CS_FAIL;
		}
	}

	return CS_SUCCEED;
}

CS_VOID* SybStatement::alloc_column_value(CS_DATAFMT *dfmt) {
	CS_VOID *value;
	switch ((int)dfmt->datatype) {
	/** Binary types */
	case CS_BINARY_TYPE:
		value = (CS_VOID*) malloc(dfmt->maxlength * sizeof(CS_BINARY));
		break;

	case CS_LONGBINARY_TYPE:
		value = (CS_VOID*) malloc(dfmt->maxlength * sizeof(CS_LONGBINARY));
		break;

	case CS_VARBINARY_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_VARBINARY));
		break;

		/** Bit types */
	case CS_BIT_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_BIT));
		break;

		/** Character types */
	case CS_CHAR_TYPE:
		value = (CS_VOID*) malloc(dfmt->maxlength* sizeof(CS_CHAR));
		break;

	case CS_LONGCHAR_TYPE:
		value = (CS_VOID*) malloc(dfmt->maxlength * sizeof(CS_LONGCHAR));
		break;

	case CS_VARCHAR_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_VARCHAR));
		break;

	case CS_UNICHAR_TYPE:
		value = (CS_VOID*) malloc(dfmt->maxlength * sizeof(CS_UNICHAR));
		break;

	case CS_XML_TYPE:
		value = (CS_VOID*) malloc(dfmt->maxlength * sizeof(CS_XML));
		break;

		/** Datetime types */
	case CS_DATE_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_DATE));
		break;

	case CS_TIME_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_TIME));
		break;

	case CS_DATETIME_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_DATETIME));
		break;

	case CS_DATETIME4_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_DATETIME4));
		break;
	case CS_BIGDATETIME_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_BIGDATETIME));
		break;

	case CS_BIGTIME_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_BIGTIME));
		break;
		/** Numeric types */
	case CS_TINYINT_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_TINYINT));
		break;

	case CS_SMALLINT_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_SMALLINT));
		break;

	case CS_INT_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_INT));
		break;

	case CS_BIGINT_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_BIGINT));
		break;

	case CS_USMALLINT_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_USMALLINT));
		break;

	case CS_UINT_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_UINT));
		break;

	case CS_UBIGINT_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_UBIGINT));
		break;

	case CS_DECIMAL_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_DECIMAL));
		break;

	case CS_NUMERIC_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_NUMERIC));
		break;

	case CS_FLOAT_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_FLOAT));
		break;

	case CS_REAL_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_REAL));
		break;

		/** Money types */
	case CS_MONEY_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_MONEY));
		break;

	case CS_MONEY4_TYPE:
		value = (CS_VOID*) malloc(sizeof(CS_MONEY4));
		break;

		/** Text and image types */
	case CS_TEXT_TYPE:
		value = (CS_VOID*) malloc(dfmt->maxlength * sizeof(CS_TEXT));
		break;

	case CS_IMAGE_TYPE:
		value = (CS_VOID*) malloc(dfmt->maxlength * sizeof(CS_IMAGE));
		break;

	case CS_UNITEXT_TYPE:
		value = (CS_VOID*) malloc(dfmt->maxlength * sizeof(CS_UNITEXT));
		break;
	default:
		value = NULL;
		break;
	}

	return value;
}

CS_VOID SybStatement::free_column_data(COLUMN_DATA *columns, CS_INT size) {
	if (columns) {
		for (CS_INT i = 0; i < size; ++i) {
			free(columns[i].value);
			columns[i].value = NULL;
		}
		free(columns);
	}
}

CS_INT SybStatement::get_row_count() {
	CS_INT row_count = 0;
	if (ct_res_info(cmd_, CS_ROW_COUNT, &row_count, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return 0;
	}
	return row_count;
}

CS_INT SybStatement::get_column_count() {
	CS_INT column_count = 0;
	if (ct_res_info(cmd_, CS_NUMDATA, &column_count, CS_UNUSED,
			NULL) != CS_SUCCEED) {
		return 0;
	}
	return column_count;
}

CS_RETCODE SybStatement::cancel_all() {
	return ct_cancel(NULL, cmd_, CS_CANCEL_ALL);
}

CS_INT SybStatement::get_column_length(CS_DATAFMT *dfmt) {
	CS_INT len;

	switch (dfmt->datatype) {
	case CS_CHAR_TYPE:
	case CS_LONGCHAR_TYPE:
	case CS_VARCHAR_TYPE:
	case CS_TEXT_TYPE:
	case CS_IMAGE_TYPE:
		len = MIN(dfmt->maxlength, MAX_CHAR_BUF);
		break;

	case CS_UNICHAR_TYPE:
		len = MIN((dfmt->maxlength / 2), MAX_CHAR_BUF);
		break;

	case CS_BINARY_TYPE:
	case CS_VARBINARY_TYPE:
		len = MIN((2 * dfmt->maxlength) + 2, MAX_CHAR_BUF);
		break;

	case CS_BIT_TYPE:
	case CS_TINYINT_TYPE:
		len = 3;
		break;

	case CS_SMALLINT_TYPE:
		len = 6;
		break;

	case CS_INT_TYPE:
		len = 11;
		break;

	case CS_REAL_TYPE:
	case CS_FLOAT_TYPE:
		len = 20;
		break;

	case CS_MONEY_TYPE:
	case CS_MONEY4_TYPE:
		len = 24;
		break;

	case CS_DATETIME_TYPE:
	case CS_DATETIME4_TYPE:
		len = 30;
		break;

	case CS_NUMERIC_TYPE:
	case CS_DECIMAL_TYPE:
		len = (CS_MAX_PREC + 2);
		break;

	default:
		len = 12;
		break;
	}

	return MAX((CS_INT)(strlen(dfmt->name) + 1), len);
}

CS_CHAR* SybStatement::get_agg_op_name(CS_INT op) {
	switch ((int) op) {
	case CS_OP_SUM:
		return (CS_CHAR*) "sum";
		break;

	case CS_OP_AVG:
		return (CS_CHAR*) "avg";
		break;

	case CS_OP_COUNT:
		return (CS_CHAR*) "count";
		break;

	case CS_OP_MIN:
		return (CS_CHAR*) "min";
		break;

	case CS_OP_MAX:
		return (CS_CHAR*) "max";
		break;

	default:
		return (CS_CHAR*) "unknown";
		break;
	}
	return (CS_CHAR*) "";
}

CS_RETCODE SybStatement::compute_info(CS_INT index, CS_DATAFMT *data_fmt) {
	CS_INT agg_op = 0;

	if (ct_compute_info(cmd_, CS_COMP_OP, index, &agg_op, CS_UNUSED,
			&data_fmt->namelen) != CS_SUCCEED) {
		return CS_FAIL;
	} else {
		strcpy(data_fmt->name, get_agg_op_name(agg_op));
	}

	return CS_SUCCEED;
}



