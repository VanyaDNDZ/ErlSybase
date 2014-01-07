#ifndef SYBCONNECTION_H
#define SYBCONNECTION_H

#define EX_CTLIB_VERSION    CS_CURRENT_VERSION

#include "SybStatement.h"
#include "SybUtils.h"
#include <ctpublic.h>
#include <string.h>
#include <stdio.h>
#include "SysLogger.h"
class SybConnection
{
public:
    SybConnection(ErlNifEnv* env);
    virtual ~SybConnection();
    void* get_connection();

    /** @brief Disconnect sybase database.
     *  @see Connection::disconnect.
     */
    bool close();

    /**
    *   @brief Begin transaction
        @return true if sucsed
    */



    /** @brief Connect to sybase database.
     *  @see Connection::connect.
     */
    bool connect(const char *host, const char *user, const char *password,
                 const char *db_name, unsigned int port);
    SybStatement* get_statement();

    /** @brief Create a statement in this connection.
     * default is the sql in constructor.
     *  @return The point to a SybStatement object.
     */
    SybStatement* create_statement();

    /** @brief Create a statement in this connection.
     *  @param sql A pointer to a sql string.
     *  @return The point to a SybStatement object.
     */
    SybStatement* create_statement(const char* sql);

    /** @brief Terminate the statement in this connection.
     *  @param stmt A pointer to a SybStatement object.
     *  @return None.
     */
    void terminate_statement(SybStatement* stmt);

protected:
    static CS_CONTEXT *sContext_;
    CS_CONNECTION *conn_;
    SybStatement* stmt_;
    int limit_row_count_;
    static SysLogger* log;
    bool is_connected;
    SybConnection(const SybConnection &);
    SybConnection & operator=(const SybConnection &);
    ErlNifEnv* env_;

    CS_RETCODE init_();
    CS_RETCODE connect_(const char *app, const char *addr, const char *user,
                        const char *pwd, const char *db, bool use_server_name);
    CS_RETCODE con_cleanup_(CS_RETCODE status);
    CS_RETCODE ctx_cleanup_(CS_RETCODE status);
private:
};

#endif // SYBCONNECTION_H
