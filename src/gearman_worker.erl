-module(gearman_worker).
-author('Samuel Stauffer <samuel@lefora.com>').

-behaviour(gen_fsm).

-export([start_link/2, start/2]).

%% gen_fsm callbacks
-export([init/1, terminate/3, code_change/4, handle_event/3, handle_sync_event/4, handle_info/3]).

%% fsm events
-export([working/2, sleeping/2, dead/2]).

-record(state, {connection, modules, functions}).

start_link(Server, WorkerModules) ->
    gen_fsm:start_link(?MODULE, {self(), Server, WorkerModules}, []).

start(Server, WorkerModules) ->
    gen_fsm:start(?MODULE, {self(), Server, WorkerModules}, []).

%% gen_server callbacks

init({_PidMaster, Server, WorkerModules}) ->
	Functions = get_functions(WorkerModules),
    {ok, Connection} = gearman_connection:start_link(),
    gearman_connection:connect(Connection, Server),
    {ok, dead, #state{connection=Connection, modules=WorkerModules, functions=Functions}}.

get_functions(Modules) ->
	get_functions(Modules, []).
get_functions([], Functions) ->
	lists:flatten(Functions);
get_functions([Module|Modules], Functions) ->
	get_functions(Modules, lists:merge(Functions, Module:functions())).

%% Private Callbacks

handle_info({Connection, connected}, _StateName, #state{connection=Connection} = State) ->
    register_functions(Connection, State#state.functions),
    gearman_connection:send_request(Connection, grab_job, {}),
    {next_state, working, State};
handle_info({Connection, disconnected}, _StateName, #state{connection=Connection} = State) ->
    {next_state, dead, State};
handle_info(Other, StateName, State) ->
    ?MODULE:StateName(Other, State).

handle_event(Event, StateName, State) ->
    io:format("UNHANDLED event ~p ~p ~p~n", [Event, StateName, State]),
    {stop, {StateName, undefined_event, Event}, State}.

handle_sync_event(Event, From, StateName, State) ->
    io:format("UNHANDLED sync_event ~p ~p ~p ~p~n", [Event, From, StateName, State]),
    {stop, {StateName, undefined_event, Event}, State}.

terminate(Reason, StateName, _State) ->
    io:format("Worker terminated: ~p [~p]~n", [Reason, StateName]),
    ok.

code_change(_OldSvn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% Event handlers

working({Connection, command, noop}, #state{connection=Connection} = State) ->
    {next_state, working, State};
working({Connection, command, no_job}, #state{connection=Connection} = State) ->
    gearman_connection:send_request(Connection, pre_sleep, {}),
    {next_state, sleeping, State, 15*1000};
working({Connection, command, {job_assign, Handle, Func, Arg}}, #state{connection=Connection, functions=Functions} = State) ->

    F = fun() ->
        try dispatch_function(Functions, Func, Arg, Handle) of
            {ok, Result} ->
                gearman_connection:send_request(Connection, work_complete, {Handle, Result});
            {error, _Reason} ->
                io:format("Unknown function ~p~n", [Func]),
                gearman_connection:send_request(Connection, work_fail, {Handle})
        catch
            Exc1:Exc2 ->
                io:format("Work failed for function ~p: ~p:~p~n~p~n", [Func, Exc1, Exc2, erlang:get_stacktrace()]),
                gearman_connection:send_request(Connection, work_fail, {Handle})
        end
    end,

    spawn(F),
    
    gearman_connection:send_request(Connection, grab_job, {}),
    {next_state, working, State}.

sleeping(timeout, #state{connection=Connection} = State) ->
    gearman_connection:send_request(Connection, grab_job, {}),
    {next_state, working, State};
sleeping({Connection, command, noop}, #state{connection=Connection} = State) ->
    gearman_connection:send_request(Connection, grab_job, {}),
    {next_state, working, State}.

dead(Event, State) ->
    io:format("Received unexpected event for state 'dead': ~p ~p~n", [Event, State]),
    {next_state, dead, State}.

%%%

dispatch_function([], _Func, _Arg, _Handle) ->
    {error, invalid_function};
dispatch_function([{Name, Function}|Functions], Func, Arg, Handle) ->
    if
        Name == Func ->
            Res = Function(Handle, Func, Arg),
            {ok, Res};
        true ->
            dispatch_function(Functions, Func, Arg, Handle)
    end.

register_functions(_Connection, []) ->
    ok;
register_functions(Connection, [{Name, _Function}|Functions]) ->
    gearman_connection:send_request(Connection, can_do, {Name}),
    register_functions(Connection, Functions).
