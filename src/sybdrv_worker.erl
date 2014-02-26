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
    {reply, sybdrv_nif:execute(Conn, Sql,[]), State};
handle_call({args_query, Sql, Params}, _From, #state{conn=Conn}=State) ->
    {reply, sybdrv_nif:execute(Conn, Sql, Params), State};
handle_call({call_proc_no_args, Sql, Params}, _From, #state{conn=Conn}=State) ->
    {reply, sybdrv_nif:call_proc(Conn, Sql, []), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    ok = sybdrv_nif:disconnect(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.