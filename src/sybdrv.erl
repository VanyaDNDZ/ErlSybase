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
    case sybdrv:execute(Rez,"INSERT INTO oper..STPH ( STDATE, STPHASE )VALUES (getdate(),'78')",[]) of
        {ok,Ans} -> io:format("command executed ~w",[Ans]);
        {error,Ans} -> io:format("Error = ~w",[Ans])
    end,
    case sybdrv:execute(Rez,"SELECT convert(varchar(6),getdate(),112) as ass,'dasdsa' as rez,19.6 as cnt",[]) of
        {ok,Ansselect} -> proceed(Ansselect);
        {error,Ansselect} -> io:format("Error = ~w",[Ansselect])
    end.
    
proceed (Head) -> 
    io:format("~n", []), 
    erlang:display(Head).
    
     

-spec connect(atom(),atom(),atom())->{ok,binary()}.
connect(_Server,_User,_Pass) ->
    erlang:nif_error(nif_library_not_loaded).

execute(_Conn, _Sql,_Param)->
    erlang:nif_error(nif_library_not_loaded).    
