#include "SybConnection.h"

CS_CONTEXT* SybConnection::sContext_ = NULL;
SysLogger* SybConnection::log = NULL;
SybConnection::SybConnection(ErlNifEnv* env):env_(env){
    conn_ = NULL;
    stmt_ = NULL;
}

SybConnection::~SybConnection(){
    if (conn_) {
        close();
        conn_ = NULL;
    }
    if (stmt_) {
        delete stmt_;
    }
    if (!SybConnection::log){
        SybConnection::log = new SysLogger();
    }
}

void* SybConnection::get_connection() {
    return this;
}

bool SybConnection::close() {
    return con_cleanup_(CS_SUCCEED) == CS_SUCCEED;
}

bool SybConnection::connect(const char *host, const char *user,
        const char *password, const char *db_name, unsigned int port) {
    bool retcode;
    char addr[50];
    char cmd[128];
    bool use_server_name = false;
    if (strlen(host) > 30) {
        return false;
    }

    if (port == 0) {
        sprintf(addr, "%s", host);
        use_server_name = true;
    } else {
        sprintf(addr, "%s %u", host, port);
    }

    if (sContext_ == NULL) {
        if (init_() != CS_SUCCEED) {
            return false;
        }
    }
	
    if (connect_("ewp_sybase_drv", addr, user, password, db_name, use_server_name)!=CS_SUCCEED) {
        return false;
    }

    if (stmt_) {
        delete stmt_;
        stmt_ = NULL;
    }
    stmt_ = create_statement();
    if (db_name != NULL) {
        if (strlen(db_name) > 120) {
            return false;
        }
        sprintf(cmd, "use %s", db_name);
        retcode = stmt_->execute_cmd(cmd);

        if (!retcode) {
            return false;
        }
    }

    limit_row_count_ = 0;
    is_connected = true;
    return true;
}

SybStatement* SybConnection::get_statement() {
    return stmt_;
}

SybStatement* SybConnection::create_statement() {
    return (new SybStatement(conn_,env_));
}

SybStatement* SybConnection::create_statement(const char* sql) {
    return (new SybStatement(conn_, sql,env_));
}

void terminate_statement(SybStatement* stmt) {
    delete stmt;
}


int get_limit_row_count() {
    return 0;
}

CS_RETCODE SybConnection::init_()
{
    CS_RETCODE retcode;
    CS_INT netio_type = CS_SYNC_IO;

    // Get a context handle to use.
    if ((retcode = cs_ctx_alloc(EX_CTLIB_VERSION, &sContext_)) != CS_SUCCEED) {
        return retcode;
    }

    // Initialize Open Client.
    if ((retcode = ct_init(sContext_, EX_CTLIB_VERSION)) != CS_SUCCEED) {
        cs_ctx_drop(sContext_);
        sContext_ = NULL;
        return retcode;
    }

    // Install client message handlers.
    if ((retcode = ct_callback(sContext_, NULL, CS_SET, CS_CLIENTMSG_CB,
            (CS_VOID *)clientmsg_cb)) != CS_SUCCEED) {
        ct_exit(sContext_, CS_FORCE_EXIT);
        cs_ctx_drop(sContext_);
        sContext_ = NULL;
        return retcode;
    }

    // Install server message handlers.
    if ((retcode = ct_callback(sContext_, NULL, CS_SET, CS_SERVERMSG_CB,
            (CS_VOID *)servermsg_cb)) != CS_SUCCEED) {
        ct_exit(sContext_, CS_FORCE_EXIT);
        cs_ctx_drop(sContext_);
        sContext_ = NULL;
        return retcode;
    }

    // Set the input/output type to synchronous.
    if ((retcode = ct_config(sContext_, CS_SET, CS_NETIO, &netio_type,
            CS_UNUSED, NULL)) != CS_SUCCEED) {
        ct_exit(sContext_, CS_FORCE_EXIT);
        cs_ctx_drop(sContext_);
        sContext_ = NULL;
        return retcode;
    }

    return CS_SUCCEED;
}

CS_RETCODE SybConnection::connect_(const char *app, const char *addr,
        const char *user, const char *pwd, const char *db, bool use_server_name)
{
	CS_RETCODE retcode;
    CS_BOOL hafailover = CS_TRUE;

    SybConnection::log->open("sybdriver");


    // Allocate a connection structure.
    retcode = ct_con_alloc(sContext_, &conn_);
    if (retcode != CS_SUCCEED) {
        return retcode;
    }

    // If a appname is defined, set the CS_APPNAME property.
    if (app != NULL) {
        retcode = ct_con_props(conn_, CS_SET, CS_APPNAME,
                (CS_VOID*)app, CS_NULLTERM, NULL);
        if (retcode != CS_SUCCEED) {
            ct_con_drop(conn_);
            conn_ = NULL;
            return retcode;
        }
    }

    // If a servername is defined, set the CS_SERVERNAME property.
    if (use_server_name == false && addr != NULL) {
        retcode = ct_con_props(conn_, CS_SET, CS_SERVERADDR,
                (CS_VOID*)addr, CS_NULLTERM, NULL);
        if (retcode != CS_SUCCEED) {
            ct_con_drop(conn_);
            conn_ = NULL;
            return retcode;
        }
    }

    // If a username is defined, set the CS_USERNAME property.
    if (user != NULL) {
        retcode = ct_con_props(conn_, CS_SET, CS_USERNAME,
                (CS_VOID*)user, CS_NULLTERM, NULL);
        if (retcode != CS_SUCCEED) {
            ct_con_drop(conn_);
            conn_ = NULL;
            return retcode;
        }
    }

    // If a password is defined, set the CS_PASSWORD property.
    if (pwd != NULL) {
        retcode = ct_con_props(conn_, CS_SET, CS_PASSWORD,
                (CS_VOID*)pwd, CS_NULLTERM, NULL);
        if (retcode != CS_SUCCEED) {
            ct_con_drop(conn_);
            conn_ = NULL;
            return retcode;
        }
    }

    // Set the CS_HAFAILOVER property.
    retcode = ct_con_props(conn_, CS_SET, CS_HAFAILOVER,
            &hafailover, CS_UNUSED, NULL);
    if (retcode != CS_SUCCEED) {
        ct_con_drop(conn_);
        conn_ = NULL;
        return retcode;
    }


    // Connect to the server.
    if (true == use_server_name) {
        CS_INT server_name_length = strlen(addr);
        retcode = ct_connect(conn_, (CS_CHAR *)addr, server_name_length);
    } else {
        retcode = ct_connect(conn_, NULL, CS_UNUSED);
    }
    if (retcode != CS_SUCCEED) {
        SybConnection::log->debug("failed ");
        SybConnection::log->close();
    	ct_con_drop(conn_);
        conn_ = NULL;
        return retcode;
    }

    return CS_SUCCEED;
}

CS_RETCODE SybConnection::con_cleanup_(CS_RETCODE status)
{
    if (conn_) {
        CS_RETCODE retcode;
        CS_INT close_option;

        close_option = (status != CS_SUCCEED) ? CS_FORCE_CLOSE : CS_UNUSED;
        retcode = ct_close(conn_, close_option);
        if (retcode != CS_SUCCEED) {
            return retcode;
        }
        retcode = ct_con_drop(conn_);
        if (retcode != CS_SUCCEED) {
            return retcode;
        }
        return retcode;
    } else {
        return CS_SUCCEED;
    }
}

CS_RETCODE SybConnection::ctx_cleanup_(CS_RETCODE status)
{
    return CS_SUCCEED;
    if (sContext_) {
        CS_RETCODE retcode;
        CS_INT exit_option;

        exit_option = (status != CS_SUCCEED) ? CS_FORCE_EXIT : CS_UNUSED;
        retcode = ct_exit(sContext_, exit_option);
        if (retcode != CS_SUCCEED) {
            return retcode;
        }
        retcode = cs_ctx_drop(sContext_);
        if (retcode != CS_SUCCEED) {
            return retcode;
        }
        return retcode;
    } else {
        return CS_SUCCEED;
    }
}
