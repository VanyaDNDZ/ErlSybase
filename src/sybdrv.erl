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
    {ok,Rez} = connect("P48TestAB","test","123456"),
    io:format("This is result = ~p-n",[Rez]),
    %case sybdrv:execute(Rez,"INSERT INTO oper..STPH ( STDATE, STPHASE )VALUES (getdate(),'78')",[]) of
    %    {ok,Ans} -> io:format("command executed ~w",[Ans]);
    %    {error,Ans} -> io:format("Error = ~w",[Ans])
    %end,
    case sybdrv:execute(Rez,"SELECT TOP 1 branch FROM oper..tAccountCharity_buf",[]) of
        {ok,Ansselect} -> proceed(Ansselect);
        {error,Ansselect} -> io:format("Error = ~w",[Ansselect])
    end.
    
proceed ([]) -> ok;
proceed ([Head|Tail]) -> 
    io:format("arr = ~w",["ppc"]),
    is_list(Tail).
    
     

-spec connect(atom(),atom(),atom())->{ok,binary()}.
connect(_Server,_User,_Pass) ->
    erlang:nif_error(nif_library_not_loaded).

execute(_Conn, _Sql,_Param)->
    erlang:nif_error(nif_library_not_loaded).    
