-module (sybdrv).
-export ([init/0,connect/3,execute/3,execute_cmd/2,test/1]).


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

-spec execute(binary(),list(),list())->{ok,list()}|{error,list()}.
execute(_Conn, _Sql,_Param)->
    erlang:nif_error(nif_library_not_loaded).  

test(Conn)->
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]),
sybdrv:execute(Conn,"SELECT * from oper..tt",[]).

execute_cmd(Conn,Sql)->
    {Rezult,_}=execute(Conn,Sql,[]),  
    Rezult.
