#include "sybdrv.h"
#include "SysLogger.h"

#define DEBUG 1
#define MAXBUFLEN       1024

static ERL_NIF_TERM connect(ErlNifEnv* env, int argc,
		const ERL_NIF_TERM argv[]) {
	SybConnection* conn = new SybConnection(env);
	CS_RETCODE retcode = CS_FAIL;
	char server[64];
	char pass[64];
	char user[64];
	if (!enif_get_string(env, argv[0], server, sizeof(server),
			ERL_NIF_LATIN1)) {
		return enif_make_int(env, 0);
	}

	if (!enif_get_string(env, argv[1], user, sizeof(user), ERL_NIF_LATIN1)) {
		return enif_make_int(env, 0);
	}
	if (!enif_get_string(env, argv[2], pass, sizeof(pass), ERL_NIF_LATIN1)) {
		return enif_make_int(env, 0);
	}

	retcode = conn->connect(server, user, pass, NULL, 0);
	if (retcode != CS_SUCCEED) {
		return enif_make_int(env, -1);
	}

	sybdrv_con* sybdrv_con_handle = (sybdrv_con*) enif_alloc_resource(
			sybdrv_crsr, sizeof(sybdrv_con));
	sybdrv_con_handle->connection = conn;
	ERL_NIF_TERM result = enif_make_resource(env, sybdrv_con_handle);
	enif_release_resource(sybdrv_con_handle);
	return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
}


static ERL_NIF_TERM execute(ErlNifEnv* env, int argc,
		const ERL_NIF_TERM argv[]) {
	SybStatement* stmt;
	sybdrv_con* sybdrv_con_handle;
	char* sql = NULL;
	unsigned int length;
	ERL_NIF_TERM head;
	ERL_NIF_TERM tail;
	ERL_NIF_TERM list;
	ERL_NIF_TERM* result = (ERL_NIF_TERM*)malloc(sizeof(ERL_NIF_TERM));
	CS_RETCODE retcode;
	if (!enif_get_resource(env, argv[0], sybdrv_crsr,
			(void**) &sybdrv_con_handle)) {
		return enif_make_tuple2(env, enif_make_atom(env, "error"),
				enif_make_string(env, "no connection found", ERL_NIF_LATIN1));
	}
	if (!enif_get_list_length(env, argv[1], &length)) {
		return 0;
	}

	sql = (char*) malloc(length + 1);
	if (!enif_get_string(env, argv[1], sql, length + 1, ERL_NIF_LATIN1)) {
		return enif_make_tuple2(env, enif_make_atom(env, "error"),
				enif_make_string(env, "no sql found", ERL_NIF_LATIN1));
	}

	if (enif_is_empty_list(env, argv[2])) {
		stmt = sybdrv_con_handle->connection->create_statement(sql);
		retcode=stmt->execute_sql(&result);
		if (retcode) {
			return enif_make_tuple2(env, enif_make_atom(env, "ok"),*result);
		} else {
			SysLogger::info("could not execute cmd");
			return enif_make_tuple2(env, enif_make_atom(env, "error"),
					enif_make_string(env, "could not execute cmd",
							ERL_NIF_LATIN1));
		}
	}
	if (!enif_is_list(env, argv[2])
			|| !enif_get_list_cell(env, argv[2], &head, &tail)) {
		SysLogger::error("no data bind found");
		return enif_make_tuple2(env, enif_make_atom(env, "error"),
				enif_make_string(env, "no data bind found", ERL_NIF_LATIN1));
	}
	list = enif_make_list_cell(env, head, tail);
	stmt = sybdrv_con_handle->connection->create_statement();

	unsigned int columns;
	enif_get_list_length(env, list, &columns);
	if(!stmt->prepare_init("test", sql)){
		SysLogger::error("prepare statement failed");
		return enif_make_tuple2(env, enif_make_atom(env, "error"),
						enif_make_string(env, "prepare fail", ERL_NIF_LATIN1));
	}

	stmt->set_params(list);
	retcode=stmt->execute_sql(&result);
	SysLogger::info("executed");
	stmt->prepare_release();
		if (retcode) {
			return enif_make_tuple2(env, enif_make_atom(env, "ok"),*result);
		} else {
			SysLogger::info("could not execute cmd");
			return enif_make_tuple2(env, enif_make_atom(env, "error"),
					enif_make_string(env, "could not execute cmd",
							ERL_NIF_LATIN1));
		}
}

static void unload(ErlNifEnv* env, void* arg) {
}

static int load_init(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
	ErlNifResourceFlags flags = (ErlNifResourceFlags) (ERL_NIF_RT_CREATE
			| ERL_NIF_RT_TAKEOVER);
	sybdrv_crsr = enif_open_resource_type(env, "sybdrv", "sybdrv_crsr", &unload,
			flags, NULL);
	return 0;
}

//static ERL_NIF_TERM execute_cmd(ErlNifEnv* env, int argc,const ERL_NIF_TERM argv[]){
//	SybStatement* stmt;
//    sybdrv_con* sybdrv_con_handle;
//	if(!enif_get_resource(env,argv[0],sybdrv_crsr,(void**)&sybdrv_con_handle)){
//		return enif_make_int(env,-6);
//	}
//    stmt = sybdrv_con_handle->connection->create_statement();
//    stmt->execute_cmd("delete from oper..testread");
//
//	return enif_make_int(env,77);
//}

static ErlNifFunc nif_funcs[] = { { "connect", 3, connect }, { "execute", 3,
		execute } };


ERL_NIF_INIT(sybdrv, nif_funcs, &load_init, NULL, NULL, NULL);

