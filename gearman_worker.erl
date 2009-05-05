-module(gearman_worker).
-author('Samuel Stauffer <samuel@descolada.com>').

-export([start/2]).

-include_lib("gearman.hrl").

-record(state, {connection, functions}).

start(Servers, Functions) ->
    Self = self(),
    spawn(fun() -> work(Self, Servers, Functions) end).

%% Private

work(_PidParent, Servers, Functions) ->
    Self = self(),
    _Workers = [spawn(fun() -> new_worker(Self, X, Functions) end) || X <- Servers],
    receive
        Any ->
            io:format("Worker master received message: ~p~n", [Any])
    end.

new_worker(_PidMaster, Server, Functions) ->
    Connection = gearman_connection:connect(Server),
    dead_loop(#state{connection=Connection, functions=Functions}).

dead_loop(#state{connection=Connection} = State) ->
    receive
        {Connection, connected} ->
            % io:format("Connected: ~p~n", [Connection]),
            register_functions(Connection, State#state.functions),
            gearman_connection:send_request(Connection, grab_job, {}),
            loop(State);
        Any ->
            io:format("Unhandled message: ~p~n", [Any]),
            dead_loop(State)
    end.

loop(#state{connection=Connection} = State) ->
    receive
        {Connection, disconnected} ->
            dead_loop(State);
        {Connection, command, no_job, {}} ->
            gearman_connection:send_request(Connection, pre_sleep, {}),
            sleep_loop(State);
        {Connection, command, job_assign, {Handle, Func, Arg}} ->
            % io:format("JOB_ASSIGN ~p ~p ~p~n", [Handle, Func, Arg]),
            try dispatch_function(State#state.functions, Func, #task{handle=Handle, func=Func, arg=Arg}) of
                {ok, Result} ->
                    gearman_connection:send_request(Connection, work_complete, {Handle, Result});
                {error, _Reason} ->
                    io:format("Unknown function ~p~n", [Func]),
                    gearman_connection:send_request(Connection, work_fail, {Handle})
            catch
                Exc1:Exc2 ->
                    io:format("Work failed for function ~p: ~p:~p~n~p~n", [Func, Exc1, Exc2, erlang:get_stacktrace()]),
                    gearman_connection:send_request(Connection, work_fail, {Handle})
            end,
            gearman_connection:send_request(Connection, grab_job, {}),
            loop(State);
        Any ->
            io:format("Unhandled message: ~p~n", [Any]),
            loop(State)
    end.

sleep_loop(#state{connection=Connection} = State) ->
    receive
        {Connection, disconnected} ->
            dead_loop(State);
        {Connection, command, noop, {}} ->
            gearman_connection:send_request(Connection, grab_job, {}),
            loop(State);
        Any ->
            io:format("Unhandled message: ~p~n", [Any]),
            sleep_loop(State)
    end.

dispatch_function([], _Func, _Task) ->
    {error, invalid_function};
dispatch_function([{Name, Function}|Functions], Func, Task) ->
    if
        Name == Func ->
            Res = Function(Task),
            {ok, Res};
        true ->
            dispatch_function(Functions, Func, Task)
    end.

register_functions(_Connection, []) ->
    ok;
register_functions(Connection, [{Name, _Function}|Functions]) ->
    gearman_connection:send_request(Connection, can_do, {Name}),
    register_functions(Connection, Functions).
