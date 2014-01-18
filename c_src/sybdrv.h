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

typedef struct sybdrv_con sybdrv_con;
static ErlNifResourceType* sybdrv_crsr;

static ERL_NIF_TERM ret_nif(ErlNifEnv* env,bool result_state,ERL_NIF_TERM* result, sybdrv_con* sybdrv_con_handle,SybStatement* stmt);
