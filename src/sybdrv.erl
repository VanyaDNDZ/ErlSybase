-module (sybdrv).
-export ([init/0,connect/3,disconnect/1,execute/3,execute_cmd/2,test/1,test_insert/1,test_out/1]).


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

test_insert(Conn)->
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]),
sybdrv:execute(Conn,"insert into oper..tt values(?)",[{time,{17,43,11,880}}]).

test_out(Conn)->
    {ok,Rows} = execute(Conn,"select * from oper..t3",[]),
    print_all(Rows).

print_all([])->
    ok;
print_all([Head|Tail])->
    lists:foreach(fun(Z) ->io:format("~62p ",[Z])    end, Head), 
    io:format("~n",[]),
    print_all(Tail). 

execute_cmd(Conn,Sql)->
    {Rezult,_}=execute(Conn,Sql,[]),  
    Rezult.
