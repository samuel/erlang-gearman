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
    process_flag(trap_exit, true),
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

handle_info({'EXIT', _Pid, shutdown = Reason}, _StateName, StateData) ->
    {stop, Reason, StateData};

handle_info({'EXIT', _Pid, _Reason}, StateName, StateData) ->
    {next_state, StateName, StateData};

handle_info({Connection, connected}, _StateName, #state{connection=Connection} = State) ->
    register_functions(Connection, State#state.functions),
    gearman_connection:send_request(Connection, grab_job, {}),
    {next_state, working, State};
handle_info({Connection, disconnected}, _StateName, #state{connection=Connection} = State) ->
    {next_state, dead, State};
handle_info(Other, StateName, State) ->
    ?MODULE:StateName(Other, State).

handle_event(Event, StateName, State) ->
    lager:error("unhandled event: ~p state: ~p", [Event, StateName]),
    {stop, {StateName, undefined_event, Event}, State}.

handle_sync_event(Event, From, StateName, State) ->
    lager:error("unheandled sync_event event: ~p from: ~p state: ~p", [Event, From, StateName]),
    {stop, {StateName, undefined_event, Event}, State}.

terminate(Reason, StateName, _State) ->
    lager:info("terminate with reason: ~p state: ~p", [Reason, StateName]),
    wait_for_childrens(self(), 2000).

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
                gearman_connection:send_request(Connection, work_fail, {Handle})
        catch
            _:_ ->
                gearman_connection:send_request(Connection, work_fail, {Handle})
        end
    end,

    spawn_link(F),
    gearman_connection:send_request(Connection, grab_job, {}),
    {next_state, working, State}.

sleeping(timeout, #state{connection=Connection} = State) ->
    gearman_connection:send_request(Connection, grab_job, {}),
    {next_state, working, State};
sleeping({Connection, command, noop}, #state{connection=Connection} = State) ->
    gearman_connection:send_request(Connection, grab_job, {}),
    {next_state, working, State}.

dead(Event, State) ->
    lager:info("Received unexpected event for state 'dead': ~p", [Event]),
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

wait_for_childrens(Pid, Timeout) ->
    {links, LinkedProcesses} = process_info(Pid, links),
    NumberChildrens = length(LinkedProcesses) -1,
    lager:info("wait_for_childrens count: ~p",[NumberChildrens]),

    if
        NumberChildrens > 0 ->
            timer:sleep(Timeout),
            wait_for_childrens(Pid, Timeout);
        true
            -> ok
    end.