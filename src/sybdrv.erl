-module (sybdrv).
-export ([init/0,test/0,connect/3,execute/3]).


init()->
    case code:which(sybdrv) of
        Filename when is_list(Filename) ->
            erlang:load_nif(filename:join([filename:dirname(Filename),
                                           "..","priv",
                                           "sybdrv"]), 0);
        Reason when is_atom(Reason) ->
            {error, Reason}
    end.

test() ->
	init(),
    {ok,Rez} = connect("UBUNTU","sa","123456"),
    io:format("This is result = ~p-n",[Rez]),
    {_,_Rr}=sybdrv:execute(Rez,"insert into oper..testread values(?,'20131212',1,1)",["76"]),
    io:format("This is result = ~w",[Rez]).

-spec connect(atom(),atom(),atom())->{ok,binary()}.
connect(_Server,_User,_Pass) ->
    erlang:nif_error(nif_library_not_loaded).

execute(_Conn, _Sql,_Param)->
    erlang:nif_error(nif_library_not_loaded).    
