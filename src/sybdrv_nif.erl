-module (sybdrv_nif).
-export ([init/0,connect/3,execute_batch/2,disconnect/1,execute/3,execute_cmd/2,call_proc/3,prepare_statement/2,
    close_statement/1,bind_params/2,execute2/1,next_resultset/1,fetchmany/2]).


init()->
    case code:which(sybdrv) of
        Filename when is_list(Filename) ->
            erlang:load_nif(filename:join([filename:dirname(Filename),
                                           "..","priv",
                                           "sybdrv"]), 0);
        Reason when is_atom(Reason) ->
            {error, Reason}
    end.


-spec connect(atom(),atom(),atom())->{ok,binary()}.
connect(_Server,_User,_Pass) ->
    erlang:nif_error(nif_library_not_loaded).

-spec disconnect(atom())->{ok,list()}.
disconnect(_Server) ->
    erlang:nif_error(nif_library_not_loaded).

-spec execute(binary(),list(),list())->{ok,list()}|{error,list()}.
execute(_Conn, _Sql,_Param)->
    erlang:nif_error(nif_library_not_loaded).
bind_params(_Stmt,_Param) ->
    erlang:nif_error(nif_library_not_loaded).
prepare_statement(_Conn,_Sql)->
    erlang:nif_error(nif_library_not_loaded).
execute2(_Stmt)->
    erlang:nif_error(nif_library_not_loaded).
next_resultset(_Stmt)->
    erlang:nif_error(nif_library_not_loaded).
fetchmany(_Stmt,_Size)->
    erlang:nif_error(nif_library_not_loaded).
close_statement(_Stmt)->
    erlang:nif_error(nif_library_not_loaded).

execute_batch(_Stmt, _Params)->
    erlang:nif_error(nif_library_not_loaded).

execute_cmd(Conn,Sql)->
    {Rezult, _} = execute(Conn, Sql, []),
    Rezult.
call_proc(_Conn,_Sql,_Param)->
        erlang:nif_error(nif_library_not_loaded).