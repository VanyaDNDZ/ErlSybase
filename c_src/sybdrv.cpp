#include "sybdrv.h"
#include "SysLogger.h"

#define DEBUG 1
#define MAXBUFLEN       1024

static ERL_NIF_TERM ret_nif(ErlNifEnv* env,bool result_state,ERL_NIF_TERM* result, sybdrv_con* sybdrv_con_handle,SybStatement* stmt);
static ErlNifResourceType* sybdrv_crsr;
static ErlNifResourceType* sybdrv_srsr;

static ERL_NIF_TERM connect(ErlNifEnv* env, int argc,
		const ERL_NIF_TERM argv[]) {
	SybConnection* conn = new SybConnection(env);
	CS_RETCODE retcode = CS_FAIL;
	char server[64];
	char pass[64];
	char user[64];
	if (!enif_get_string(env, argv[0], server, sizeof(server),
			ERL_NIF_LATIN1)) {
		return enif_make_badarg(env);
	}

	if (!enif_get_string(env, argv[1], user, sizeof(user), ERL_NIF_LATIN1)) {
		return enif_make_badarg(env);
	}
	if (!enif_get_string(env, argv[2], pass, sizeof(pass), ERL_NIF_LATIN1)) {
		return enif_make_badarg(env);
	}

	retcode = conn->connect(server, user, pass, NULL, 0);
	if (retcode != CS_SUCCEED) {
		return enif_make_badarg(env);
	}

	sybdrv_con* sybdrv_con_handle = (sybdrv_con*) enif_alloc_resource(
			sybdrv_crsr, sizeof(sybdrv_con));
	sybdrv_con_handle->connection = conn;
	ERL_NIF_TERM result = enif_make_resource(env, sybdrv_con_handle);
	enif_release_resource(sybdrv_con_handle);
	return enif_make_tuple2(env, sybdrv_atoms.ok, result);
}

static ERL_NIF_TERM disconnect(ErlNifEnv* env, int argc,
		const ERL_NIF_TERM argv[]) {
	sybdrv_con* sybdrv_con_handle;
	if (!enif_get_resource(env, argv[0], sybdrv_crsr,
			(void**) &sybdrv_con_handle)) {
		return enif_make_badarg(env);
	}
	if(sybdrv_con_handle->connection->close()){
		ERL_NIF_TERM* result=(ERL_NIF_TERM*)malloc(sizeof(ERL_NIF_TERM));
		ERL_NIF_TERM out = enif_make_string(env, "disconnect", ERL_NIF_LATIN1);
		result=&out;
		return ret_nif(env,true,result,sybdrv_con_handle,NULL);
	}else{
		ERL_NIF_TERM* result=(ERL_NIF_TERM*)malloc(sizeof(ERL_NIF_TERM));
		ERL_NIF_TERM out = enif_make_string(env, "faile disconnect", ERL_NIF_LATIN1);
		result=&out;
		return ret_nif(env,false,result,sybdrv_con_handle,NULL);
	}
}

static ERL_NIF_TERM execute_batch(ErlNifEnv* env,int argc,const ERL_NIF_TERM argv[]){
	if(argc !=2){
		return enif_make_badarg(env);
	}
	sybdrv_stmt* sybdrv_statement_handle;
	if (!enif_get_resource(env, argv[0], sybdrv_srsr,
			(void**) &sybdrv_statement_handle)) {
		return enif_make_badarg(env);
	}
	if (!enif_is_list(env,argv[1]))
	{
		return enif_make_badarg(env);
	}
	if(!sybdrv_statement_handle->statement->set_params_batch(argv[1])){
		return sybdrv_atoms.error;
	}
	ERL_NIF_TERM* result = (ERL_NIF_TERM*)malloc(sizeof(ERL_NIF_TERM));
	sybdrv_statement_handle->statement->handle_sql_result(&result);
	return sybdrv_atoms.ok;
}

static ERL_NIF_TERM prepare_statement(ErlNifEnv* env,int argc,const ERL_NIF_TERM argv[]){
	if(argc!=3){
		return enif_make_badarg(env);
	}
	
	sybdrv_con* sybdrv_con_handle;

	if (!enif_get_resource(env, argv[0], sybdrv_crsr,
			(void**) &sybdrv_con_handle)) {
		return enif_make_badarg(env);
	}
	
	unsigned statement_id_len,statement_len;

	if(!enif_get_list_length(env,argv[1],&statement_id_len)){
		return enif_make_badarg(env);	
	}
	char* statement_id= (char*)malloc(statement_id_len+1);
	if(!enif_get_string(env,argv[1],statement_id,statement_id_len+1,ERL_NIF_LATIN1)){
		return enif_make_badarg(env);	
	}
	
	if(!enif_get_list_length(env,argv[2],&statement_len)){
		return enif_make_badarg(env);	
	}
	char* statement= (char*)malloc(statement_len+1);
	if(!enif_get_string(env,argv[2],statement,statement_len+1,ERL_NIF_LATIN1)){
		return enif_make_badarg(env);	
	}

	
	SybStatement* stmt;
	
	stmt = sybdrv_con_handle->connection->create_statement(statement);
	
	if(!stmt->prepare_init(statement_id, statement)){
		ERL_NIF_TERM* result=(ERL_NIF_TERM*)malloc(sizeof(ERL_NIF_TERM));
		ERL_NIF_TERM out = enif_make_string(env, "faile prepare_init", ERL_NIF_LATIN1);
		result=&out;
		return ret_nif(env,false,result,sybdrv_con_handle,NULL);
	}
	sybdrv_stmt* sybdrv_statement_handle = (sybdrv_stmt*) enif_alloc_resource(
			sybdrv_srsr, sizeof(sybdrv_stmt));
	sybdrv_statement_handle->statement = stmt;
	ERL_NIF_TERM stmt_rez = enif_make_resource(env, sybdrv_statement_handle);
	enif_release_resource(sybdrv_statement_handle);
	return enif_make_tuple2(env, sybdrv_atoms.ok, stmt_rez);
}

static ERL_NIF_TERM close_statement(ErlNifEnv* env,int argc,const ERL_NIF_TERM argv[]){
	if(argc!=1){
		return enif_make_badarg(env);
	}
	sybdrv_stmt* sybdrv_statement_handle;
	if (!enif_get_resource(env, argv[0], sybdrv_srsr,
			(void**) &sybdrv_statement_handle)) {
		return enif_make_badarg(env);
	}
	if(!sybdrv_statement_handle->statement->prepare_release()){
		return sybdrv_atoms.error;
	}
	return sybdrv_atoms.ok;
}

static ERL_NIF_TERM execute(ErlNifEnv* env, int argc,
		const ERL_NIF_TERM argv[]) {
	SybStatement* stmt;
	sybdrv_con* sybdrv_con_handle;
	char* sql = NULL;
	unsigned int length;
	ERL_NIF_TERM* result = (ERL_NIF_TERM*)malloc(sizeof(ERL_NIF_TERM));
	if (!enif_get_resource(env, argv[0], sybdrv_crsr,
			(void**) &sybdrv_con_handle)) {
		 return enif_make_badarg(env);
	}
	if (!enif_get_list_length(env, argv[1], &length)) {
		 return enif_make_badarg(env);
	}

	sql = (char*) malloc(length + 1);
	if (!enif_get_string(env, argv[1], sql, length + 1, ERL_NIF_LATIN1)) {
		 return enif_make_badarg(env);
	}

	if (enif_is_empty_list(env, argv[2])) {
		stmt = sybdrv_con_handle->connection->create_statement(sql);
		if (stmt->execute_sql(&result)) {
			return enif_make_tuple2(env, sybdrv_atoms.ok,*result);
		} else {
			return enif_make_tuple2(env, sybdrv_atoms.error,
					enif_make_string(env, "could not execute cmd",
							ERL_NIF_LATIN1));
		}
	}else{
		if (!enif_is_list(env, argv[2])) {
			return enif_make_tuple2(env, sybdrv_atoms.error,
					enif_make_string(env, "no data bind found", ERL_NIF_LATIN1));
	}
	stmt = sybdrv_con_handle->connection->create_statement();

	unsigned int columns;
	
	enif_get_list_length(env, argv[2], &columns);
	
	if(!stmt->prepare_init("test", sql)){
		SysLogger::error("prepare statement failed");
		return enif_make_tuple2(env, sybdrv_atoms.error,
						enif_make_string(env, "prepare fail", ERL_NIF_LATIN1));
	}

	if(!stmt->set_params(argv[2])){
		ERL_NIF_TERM out = enif_make_string(env, "could not set params",ERL_NIF_LATIN1);
			result= &out;
		return ret_nif(env,false,result,sybdrv_con_handle,stmt);
	}
	if (stmt->execute_sql(&result) ) {
			return ret_nif(env,true,result,sybdrv_con_handle,stmt);
	} else {
			ERL_NIF_TERM out = enif_make_string(env, "could not execute cmd",ERL_NIF_LATIN1);
			result= &out;
			return ret_nif(env,false,result,sybdrv_con_handle,stmt);
		}
	}
	
}

static ERL_NIF_TERM ret_nif(ErlNifEnv* env,bool result_state,ERL_NIF_TERM* result, sybdrv_con* sybdrv_con_handle,SybStatement* stmt){

	ERL_NIF_TERM out = *result;
	if (stmt){
		stmt->prepare_release();	
		delete stmt;
	}

	if(result_state){
		return enif_make_tuple2(env, sybdrv_atoms.ok,out);
	}else{
		return enif_make_tuple2(env, sybdrv_atoms.error,out);
	}

}


static void unload_connect(ErlNifEnv* env, void* arg) {

}

static int load_init(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
	ErlNifResourceFlags flags = (ErlNifResourceFlags) (ERL_NIF_RT_CREATE
			| ERL_NIF_RT_TAKEOVER);
	sybdrv_crsr = enif_open_resource_type(env, "sybdrv", "sybdrv_crsr", &unload_connect,
			flags, NULL);
	if(!sybdrv_crsr){
		return -1;
	}
	sybdrv_srsr = enif_open_resource_type(env, "sybdrv", "sybdrv_srsr", &unload_connect,
			flags, NULL);
	if(!sybdrv_srsr){
		return -1;
	}

	sybdrv_atoms.ok = enif_make_atom(env, "ok");
	sybdrv_atoms.error = enif_make_atom(env, "error");
	sybdrv_atoms.undefined = enif_make_atom(env, "undefined");
	sybdrv_atoms.date = enif_make_atom(env, "date");
	sybdrv_atoms.times = enif_make_atom(env, "time");
	sybdrv_atoms.datetime = enif_make_atom(env, "datetime");
	sybdrv_atoms.number = enif_make_atom(env, "number");
	sybdrv_atoms.unknown = enif_make_atom(env, "unknown");

	return 0;
}


static ErlNifFunc nif_funcs[] = { 
		{ "connect", 3, connect }, 
		{ "execute", 3, execute },
		{"disconnect",1,disconnect},
		{"prepare_statement",3,prepare_statement},
		{"close_statement",1,close_statement},
		{"execute_batch",2,execute_batch}};


ERL_NIF_INIT(sybdrv, nif_funcs, &load_init, NULL, NULL, NULL);

