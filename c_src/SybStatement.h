#ifndef SYBSTATEMENT_H
#define SYBSTATEMENT_H


#define MAX(X,Y)    (((X) > (Y)) ? (X) : (Y))
#define MIN(X,Y)    (((X) < (Y)) ? (X) : (Y))
#define MAX_CHAR_BUF    1024

#define TRUNC_SUBST "***"
#include <ctpublic.h>
#include <stdlib.h>
#include "SysLogger.h"
#include "erl_nif.h"
#include "SybUtils.h"



/** @brief Sybase prepare statement class.
 */
class SybStatement {
    typedef struct _column_data
    {
        CS_DATAFMT dfmt;
        CS_VOID *value;
        CS_INT valuelen;
        CS_SMALLINT indicator;
    } COLUMN_DATA;

    typedef bool (SybStatement::*setup_callback)(CS_DATAFMT* dfmt, CS_VOID* data, CS_INT len);

public:
    /** @brief Constructor for SybStatement class.
     *  @param context A pointer to a CS_CONTEXT structure.
     *  @param sql A pointer to a sql string.
     *  @return None.
     */
    SybStatement(CS_CONNECTION* context,ErlNifEnv* env);
    SybStatement(CS_CONNECTION*, const char* sql,ErlNifEnv* env);

    /** @brief Destructor for SybStatement class.
     *  @return None.
     */
    ~SybStatement();

    /** @brief Execute a language command, only return success or failure.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool execute_cmd();
    
    /** @brief Execute a language command, only return success or failure.
     *  @param sql A pointer to a sql string, default is the sql in constructor.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool execute_cmd(const char* sql);

    /** @brief Execute a language command, encode result to the buffer and
     *      return success or failure.
     *  @param result The address of a ERL_NIF_TERM variable. execute_sql
     *      encode result to the buffer.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool execute_sql(ERL_NIF_TERM** result);
    
    /** @brief Execute a language command, encode result to the buffer and
     *      return success or failure.
     *  @param result The address of a ERL_NIF_TERM variable. execute_sql
     *      encode result to the buffer.
     *  @param sql A pointer to a sql string, default is the sql in constructor.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool execute_sql(ERL_NIF_TERM** result, const char* sql);

    /** @brief Initialize a prepare statement.
     *  @param id A pointer to the statement identifier. This identifier
     *      is defined by the application and must conform to server standards
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool prepare_init(const char* id);
    
    /** @brief Initialize a prepare statement.
     *  @param id A pointer to the statement identifier. This identifier
     *      is defined by the application and must conform to server standards
     *  @param sql A pointer to a sql string, default is the sql in constructor.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool prepare_init(const char* id, const char* sql);

    /** @brief Get the parameter type to set the parameter with the
     *      appropriate function.
     *  @param index The index of the parameters. The first parameter has
     *      an index of 1,the second an index of 2, and so forth.
     *  @return Unsigned integer to indicate the parameter type if the
     *      routine completed successfully else -1.
     */
    int get_param_type(int index);

    /** @brief Get the total number of parameters to set.
     *  @return The total number of parameters.
     */
    int get_param_count();

    /** @brief Set the parameter.
     *  @param index The index of the parameters. The first parameter has
     *      an index of 1,the second an index of 2, and so forth.
     *  @param data The data to set.
     *  @param len Data length.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool set_binary(int index, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_longbinary(int index, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_varbinary(int index, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_bit(int index, unsigned char data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_char(int index, char* data);

    /** @brief Set the parameter .
     *  @see set_binary.
     */
    bool set_char(int index, ERL_NIF_TERM char_data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_char(int index, char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_longchar(int index, unsigned char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_longchar(int index, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_varchar(int index, unsigned char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_varchar(int index, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_unichar(int index, unsigned short* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_xml(int index, unsigned char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_xml(int index, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_date(int index, int data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_date(int index, int year, int month, int day);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_time(int index, int data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_time(int index, int hour, int minutes, int seconds, int ms);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_datetime(int index, int days, int time);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_datetime(int index, int year, int month, int day,
            int hour, int minutes, int seconds, int ms);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_datetime4(int index, unsigned short days, unsigned short minutes);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_datetime4(int index, int year, int month,
            int day, int hour, int minutes);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_bigdatetime(int index, unsigned long data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_bigdatetime(int index, int year, int month, int day,
            int hour, int minutes, int seconds, int ms, int Ms);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_bigtime(int index, unsigned long data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_bigtime(int index, int hour, int minutes,
            int seconds, int ms, int Ms);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_tinyint(int index, unsigned char data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_smallint(int index, short data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_int(int index, int data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_bigint(int index, long data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_usmallint(int index, unsigned short data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_uint(int index, unsigned int data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_ubigint(int index, unsigned long data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_decimal(int index, char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_decimal(int index, unsigned char precision,
        unsigned char scale, unsigned char* data, unsigned int len);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_numeric(int index, char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_numeric(int index, unsigned char precision,
        unsigned char scale, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_float(int index, double data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_real(int index, float data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_money(int index, char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_money(int index, int high, unsigned int low);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_money4(int index, double data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_null(int index);

    /** @brief Release a prepare statement.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool prepare_release();

    /** @brief Get the number of affected rows after executing a sql command.
     *  @return The number of affected rows.
     */
    unsigned int get_affected_rows();

    bool set_params(ERL_NIF_TERM list){
        unsigned int list_index=1;
        unsigned int list_size;
        ERL_NIF_TERM list_head,list_tail;
        enif_get_list_length(env_,list,&list_size);
        while(list_size>0 && list_size>=list_index && enif_get_list_cell(env_,list,&list_head,&list_tail)) {
            if(!decode_and_set_param(list_index,list_head,false)){
                return false;
            }
            list = list_tail;
            ++list_index;
        }
        
        return true;
    }

    bool batch_initial_bind(int column_count){
        ct_dynamic(cmd_, CS_EXECUTE, id_, CS_NULLTERM, NULL,
        CS_UNUSED);
        for (int i = 0; i < column_count; ++i)
        {
            CS_DATAFMT* dfmt = desc_dfmt_ + i;
            if(!ct_setparam(cmd_, dfmt, NULL, NULL, NULL)){
                return false;
            }
        }
        return true;
    }

    bool set_params_batch(ERL_NIF_TERM list){
        unsigned int list_index=1,rows_index=1;
        unsigned int list_size,rows;
        ERL_NIF_TERM rows_head,rows_tail,list_head,list_tail;
        enif_get_list_length(env_,list,&rows);
        while(rows>0 && rows>=rows_index && enif_get_list_cell(env_,list,&rows_head,&rows_tail)){
            
            if (rows_index==1){
                if(!enif_get_list_length(env_,rows_head,&list_size)){
                    return false;
                }

                if(!batch_initial_bind(list_size)){
                    return false;   
                }

            }

            
            while(list_size>0 && list_size>=list_index && enif_get_list_cell(env_,rows_head,&list_head,&list_tail)) {
            
                if(!decode_and_set_param(list_index,list_head,true)){
                    return false;
                }
            
                rows_head = list_tail;
            
                ++list_index;
            }
            list=rows_tail;
            ++rows_index;
        }
        
        if(!ct_send(cmd_)){
            return false;
        }
        
        return true;
    }

    bool set_param( int index, unsigned char data)
        {
            bool retcode;
            int param_type;
            param_type = get_param_type(index);
            switch((int)param_type) {
                /** Bit types */
                case CS_BIT_TYPE:
                    retcode = set_bit(index, data);
                    break;

                case CS_TINYINT_TYPE:
                    retcode = set_tinyint(index, data);
                    break;

                case CS_SMALLINT_TYPE:
                    retcode = set_smallint(index, (short)data);
                    break;

                case CS_INT_TYPE:
                    retcode = set_int(index, (int)data);
                    break;

                case CS_BIGINT_TYPE:
                    retcode = set_bigint(index, (long)data);
                    break;

                case CS_USMALLINT_TYPE:
                    retcode = set_usmallint(index, (unsigned short)data);
                    break;

                case CS_UINT_TYPE:
                    retcode = set_uint(index, (unsigned int)data);
                    break;

                case CS_UBIGINT_TYPE:
                    retcode = set_ubigint(index, (unsigned long)data);
                    break;

                case CS_FLOAT_TYPE:
                    retcode = set_float(index, (double)data);
                    break;

                case CS_REAL_TYPE:
                    retcode = set_real(index, (float)data);
                    break;

                case CS_MONEY4_TYPE:
                    retcode = set_money4(index, (double)data);
                    break;
                case CS_CHAR_TYPE:
                    retcode = set_char(index,(char*)&data);

                default:
                    retcode = false;
                    break;
            }

            return retcode;
        }

    bool decode_and_set_param( int index, ERL_NIF_TERM data,bool is_bulk = false)
        {
            setup_callback callback;
            
            if(!is_bulk){
                    callback= &SybStatement::set_param;
            }
            
            bool retcode;
            
            int param_type;
            
            param_type = get_param_type(index);
            
            switch((int)param_type) {
                
                case CS_CHAR_TYPE:
                case CS_LONGCHAR_TYPE:
                case CS_BIT_TYPE:
                    retcode = decode_and_set_char(index,data,callback);
                    break;
                case CS_VARCHAR_TYPE:
                    retcode = decode_and_set_varchar(index,data,callback);
                    break;
                case CS_LONGBINARY_TYPE:
                    retcode = decode_and_set_longbinary(index,data,callback);
                    break;
                case CS_LONG_TYPE:
                    retcode = decode_and_set_long(index,data,callback); 
                    break;
                case CS_VARBINARY_TYPE:
                    retcode = decode_and_set_varbinary(index,data,callback);
                    break;
                case CS_SMALLINT_TYPE:
                    retcode = decode_and_set_smallint(index,data,callback);
                    break;
                case CS_INT_TYPE:
                    retcode = decode_and_set_int(index,data,callback);
                    break;  
                case CS_FLOAT_TYPE:
                case CS_REAL_TYPE:
                    retcode = decode_and_set_real(index,data,callback);
                    break;
                case CS_NUMERIC_TYPE:
                case CS_DECIMAL_TYPE:
                    retcode = decode_and_set_numeric(index,data,callback);
                    break;
                case CS_DATETIME_TYPE:
                    retcode = decode_and_set_datetime(index,data,callback);
                    break;
                case CS_DATE_TYPE:
                    retcode = decode_and_set_date(index,data,callback);
                    break;
                case CS_TIME_TYPE:
                    retcode = decode_and_set_time(index,data,callback);
                    break;
                default:
                    retcode = false;
                    break;
            }

            return retcode;
        }

private:
    CS_CONNECTION *conn_;
    char* sql_;
    CS_COMMAND *cmd_;
    CS_INT row_count_;
    static SysLogger* log;

    /** data format of parameters in prepare statement */
    CS_DATAFMT *desc_dfmt_;
    CS_INT param_count_;

    char id_[CS_MAX_CHAR];
    bool is_prepare_;
    bool executed_;
    ErlNifEnv* env_;


    void reset();

    CS_RETCODE handle_describe_result();
    CS_RETCODE handle_describe_result(CS_COMMAND *cmd);

    CS_RETCODE handle_command_result();
    CS_RETCODE handle_command_result(CS_COMMAND *cmd);

    CS_RETCODE handle_sql_result(ERL_NIF_TERM** result);

    CS_RETCODE process_describe_reslut();

    ERL_NIF_TERM process_row_result();

    ERL_NIF_TERM encode_query_result(COLUMN_DATA *columns, CS_INT column_count);

    CS_RETCODE encode_update_result(ERL_NIF_TERM* result, CS_INT row_count);

    ERL_NIF_TERM encode_column_data(COLUMN_DATA *column);
    CS_RETCODE encode_column_data(ERL_NIF_TERM* result, COLUMN_DATA *column);

    CS_VOID* alloc_column_value(CS_DATAFMT *dfmt);

    CS_VOID free_column_data(COLUMN_DATA *columns, CS_INT size);

    CS_INT get_row_count();

    CS_INT get_column_count();

    CS_RETCODE cancel_current();

    CS_RETCODE cancel_all();

    CS_INT get_column_length(CS_DATAFMT *dfmt);

    CS_CHAR* get_agg_op_name(CS_INT op);

    CS_RETCODE compute_info(CS_INT index, CS_DATAFMT *data_fmt);
    
    bool set_param(CS_DATAFMT* dfmt, CS_VOID* data, CS_INT len);

    ERL_NIF_TERM encode_binary( CS_DATAFMT* dfmt, CS_BINARY* v, CS_INT len);

    ERL_NIF_TERM encode_longbinary( CS_DATAFMT* dfmt, CS_LONGBINARY* v, CS_INT len);

    ERL_NIF_TERM encode_varbinary( CS_DATAFMT* dfmt, CS_VARBINARY* v);

    ERL_NIF_TERM encode_bit( CS_DATAFMT* dfmt, CS_BIT* v);

    ERL_NIF_TERM encode_char(CS_DATAFMT* dfmt, CS_CHAR* v, CS_INT len);

    ERL_NIF_TERM encode_longchar( CS_DATAFMT* dfmt, CS_LONGCHAR* v, CS_INT len);

    ERL_NIF_TERM encode_varchar( CS_DATAFMT* dfmt, CS_VARCHAR* v);

    ERL_NIF_TERM encode_unichar( CS_DATAFMT* dfmt, CS_UNICHAR* v, CS_INT len);

    ERL_NIF_TERM encode_xml( CS_DATAFMT* dfmt, CS_XML* v, CS_INT len);

    ERL_NIF_TERM encode_date( CS_DATAFMT* dfmt, CS_DATE* v);

    ERL_NIF_TERM encode_time( CS_DATAFMT* dfmt, CS_TIME* v);

    ERL_NIF_TERM encode_datetime( CS_DATAFMT* dfmt, CS_DATETIME* v);

    ERL_NIF_TERM encode_datetime4( CS_DATAFMT* dfmt, CS_DATETIME4* v);
    
    ERL_NIF_TERM encode_bigdatetime( CS_DATAFMT* dfmt, CS_BIGDATETIME* v);

    ERL_NIF_TERM encode_bigtime( CS_DATAFMT* dfmt, CS_BIGTIME* v);
    
    ERL_NIF_TERM encode_tinyint( CS_DATAFMT* dfmt, CS_TINYINT* v);

    ERL_NIF_TERM encode_smallint( CS_DATAFMT* dfmt, CS_SMALLINT* v);

    ERL_NIF_TERM encode_int( CS_DATAFMT* dfmt, CS_INT* v);

    ERL_NIF_TERM encode_bigint( CS_DATAFMT* dfmt, CS_BIGINT* v);

    ERL_NIF_TERM encode_usmallint( CS_DATAFMT* dfmt, CS_USMALLINT* v);

    ERL_NIF_TERM encode_uint( CS_DATAFMT* dfmt, CS_UINT* v);

    ERL_NIF_TERM encode_ubigint( CS_DATAFMT* dfmt, CS_UBIGINT* v);

    ERL_NIF_TERM encode_decimal( CS_DATAFMT* dfmt, CS_DECIMAL* v);

    ERL_NIF_TERM encode_numeric( CS_DATAFMT* dfmt, CS_NUMERIC* v);

    ERL_NIF_TERM encode_float( CS_DATAFMT* dfmt, CS_FLOAT* v);

    ERL_NIF_TERM encode_real( CS_DATAFMT* dfmt, CS_REAL* v);

    ERL_NIF_TERM encode_money( CS_DATAFMT* dfmt, CS_MONEY* v);

    ERL_NIF_TERM encode_money4( CS_DATAFMT* dfmt, CS_MONEY4* v);

    ERL_NIF_TERM encode_text( CS_DATAFMT* dfmt, CS_TEXT* v, CS_INT len);

    ERL_NIF_TERM encode_image( CS_DATAFMT* dfmt, CS_IMAGE* v, CS_INT len);

    ERL_NIF_TERM encode_unitext( CS_DATAFMT* dfmt, CS_UNITEXT* v, CS_INT len);

    ERL_NIF_TERM encode_unknown();

    ERL_NIF_TERM encode_overflow();

    ERL_NIF_TERM encode_null();
    bool decode_and_set_char(int index, ERL_NIF_TERM char_data,setup_callback);
    bool decode_and_set_binary(int index, ERL_NIF_TERM data,  setup_callback callback);
    bool decode_and_set_longbinary(int index, ERL_NIF_TERM data,  setup_callback callback);
    bool decode_and_set_varbinary(int index, ERL_NIF_TERM data,  setup_callback callback);
    bool decode_and_set_bit(int index, ERL_NIF_TERM data,  setup_callback callback);
    bool decode_and_set_longchar(int index, ERL_NIF_TERM char_data,  setup_callback callback);
    bool decode_and_set_varchar(int index, ERL_NIF_TERM char_data,  setup_callback callback);
    bool decode_and_set_unichar(int index, ERL_NIF_TERM char_data,  setup_callback callback);
    bool decode_and_set_xml(int index, ERL_NIF_TERM char_data,  setup_callback callback);
    bool decode_and_set_date(int index, ERL_NIF_TERM data,  setup_callback callback);
    bool decode_and_set_time(int index, ERL_NIF_TERM data,  setup_callback callback);
    bool decode_and_set_datetime(int index, ERL_NIF_TERM data,  setup_callback callback);
    bool decode_and_set_numeric(int index,ERL_NIF_TERM data,  setup_callback callback);
    bool decode_and_set_tinyint(int index, ERL_NIF_TERM data,  setup_callback callback);
    bool decode_and_set_smallint(int index, ERL_NIF_TERM data,  setup_callback callback);
    bool decode_and_set_int(int index, ERL_NIF_TERM data,  setup_callback callback);
    bool decode_and_set_long (int index, ERL_NIF_TERM data,  setup_callback callback);
    bool decode_and_set_real (int index, ERL_NIF_TERM data,  setup_callback callback);
};

#endif // SYBSTATEMENT_H
