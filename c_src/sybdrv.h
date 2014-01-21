#include "SybConnection.h"
#include "erl_nif.h"
#include <ctpublic.h>
#include "SybConnection.h"
#include <fstream>
#include <string>
#include <iostream>
#include "SysLogger.h"
#include "SybStatement.h"


struct sybdrv_con {
   SybConnection *connection;
};

struct sybdrv_stmt {
   SybStatement *statement;
};

static struct  {
	ERL_NIF_TERM ok;
	ERL_NIF_TERM error;
	ERL_NIF_TERM undefined;
	ERL_NIF_TERM date;
	ERL_NIF_TERM times;
	ERL_NIF_TERM datetime;
	ERL_NIF_TERM number;
	ERL_NIF_TERM unknown;
} sybdrv_atoms;

typedef struct sybdrv_con sybdrv_con;


inline ERL_NIF_TERM make_atom(ErlNifEnv* env,const char* atom_str){
	ERL_NIF_TERM atom;
	if(!enif_make_existing_atom(env,atom_str,&atom,ERL_NIF_LATIN1)){
		atom = enif_make_atom(env,atom_str);
	}
	return atom;
}
