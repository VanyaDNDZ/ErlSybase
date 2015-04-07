-module (sybdrv_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {conn}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    process_flag(trap_exit, true),
    Hostname = proplists:get_value(hostname, Args),
    %Database = proplists:get_value(database, Args),
    Username = proplists:get_value(username, Args),
    Password = proplists:get_value(password, Args),
    {ok, Conn} = sybdrv_nif:connect(Hostname, Username, Password),
    {ok, #state{conn=Conn}}.

handle_call({plain_sql, Sql}, _From, #state{conn=Conn}=State) ->
    {reply, sybdrv_nif:execute(Conn, binary:bin_to_list(unicode:characters_to_binary(Sql)), []), State};
handle_call({args_query, Sql, Params}, _From, #state{conn=Conn}=State) ->
    {reply, sybdrv_nif:execute(Conn, binary:bin_to_list(unicode:characters_to_binary(Sql)), Params), State};
handle_call({prepare_sql, Sql}, _From, #state{conn = Conn} = State) ->
    {reply, sybdrv_nif:prepare_statement(Conn, binary:bin_to_list(unicode:characters_to_binary(Sql))), State};
handle_call({bind_params, Stmt, Params}, _From, State) ->
    {reply, sybdrv_nif:bind_params(Stmt, Params), State};
handle_call({excute_statement, Stmt}, _From, State) ->
    {reply, sybdrv_nif:execute2(Stmt), State};
handle_call({next_resultset, Stmt}, _From, State) ->
    {reply, sybdrv_nif:next_resultset(Stmt), State};
handle_call({fetchmany, Stmt, Size}, _From, State) when is_integer(Size) ->
    {reply, sybdrv_nif:fetchmany(Stmt, Size), State};
handle_call({close_statement, Stmt}, _From, State) ->
    {reply, sybdrv_nif:close_statement(Stmt), State};
handle_call({call_proc_no_args, Sql, Params}, _From, #state{conn=Conn}=State) ->
    {reply, sybdrv_nif:call_proc(Conn, Sql, Params), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    {ok,"disconnect"} = sybdrv_nif:disconnect(Conn).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.