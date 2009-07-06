-module(gearman_connection).
-author('Samuel Stauffer <samuel@lefora.com>').

-export([connect/1, disconnect/1, send_request/3, send_response/3]).
-export([terminate/2, handle_cast/2, code_change/3]).

-define(DEFAULT_PORT, 4730).
-define(RECONNECT_DELAY, 10000). %% milliseconds

-record(state, {host, port, pidparent, socket=not_connected, buffer=[]}).


connect({Host}) ->
    connect({Host, ?DEFAULT_PORT});
connect({Host, Port}) ->
    Self = self(),
    Pid = spawn(fun() ->
        {ok, State, Timeout} = init({Self, Host, Port}),
        loop(State, Timeout)
    end),
    {ok, Pid}.

call(Pid, Arg) ->
    Ref = make_ref(),
    Pid ! {'$gen_call', {self(), Ref}, Arg},
    receive
        {Ref, Res} ->
            Res
    end.

disconnect(Pid) ->
    call(Pid, {stop}).

send_request(Pid, Command, Args) when is_pid(Pid) ->
    %% Do the packing here so errors are detected in the calling process
    Packet = pack_request(Command, Args),
    call(Pid, {send_command, Packet}).

send_response(Pid, Command, Args) when is_pid(Pid) ->
    %% Do the packing here so errors are detected in the calling process
    Packet = pack_response(Command, Args),
    call(Pid, {send_command, Packet}).

%%%

loop(State, Timeout) ->
    R = receive
        {'$gen_call', {From, Ref}, Arg} ->
            R2 = handle_call(Arg, From, State),
            case R2 of
                {reply, Response, NewState} ->
                    From ! {Ref, Response},
                    {noreply, NewState};
                {reply, Response, NewState, NewTimeout} when is_integer(NewTimeout) ->
                    From ! {Ref, Response},
                    {noreply, NewState, NewTimeout}
            end;
        Any ->
            handle_info(Any, State)
    after Timeout ->
        handle_info(timeout, State)
    end,
    case R of
        {noreply, NewState2, NewTimeout2} when is_integer(NewTimeout2) ->
            loop(NewState2, NewTimeout2);
        {noreply, NewState2} ->
            loop(NewState2, infinity)
    end.

%%%------------------------------------------------------------------------
%%% Callback functions from gen_server
%%%------------------------------------------------------------------------

init({PidParent, Host, Port}) ->
    {ok, #state{host=Host, port=Port, pidparent=PidParent}, 0}.

handle_call({send_command, Packet}, _From, State) ->
    try gen_tcp:send(State#state.socket, Packet) of
        ok ->
            {reply, ok, State};
        Any ->
            io:format("gen_tcp:send returned unhandled value ~p~n", [Any]),
            NewState = disconnect_state(State),
            {reply, {error, Any}, NewState, ?RECONNECT_DELAY}
    catch
        Exc1:Exc2 ->
            io:format("gen_tcp:send raised an exception ~p:~p~n", [Exc1, Exc2]),
            NewState = disconnect_state(State),
            {reply, {error, {Exc1, Exc2}}, NewState, ?RECONNECT_DELAY}
    end;
handle_call(stop, _From, State) ->
    {stop, normal, State}.

handle_info(timeout, #state{host=Host, port=Port, socket=OldSocket} = State) ->
    case OldSocket of
        not_connected ->
            case gen_tcp:connect(Host, Port, [binary, {packet, 0}]) of
                {ok, Socket} ->
                    State#state.pidparent ! {self(), connected},
                    NewState = State#state{socket=Socket},
                    {noreply, NewState};
                {error, econnrefused} ->
                    {noreply, State, ?RECONNECT_DELAY}
            end;
        _ ->
            io:format("Timeout while socket not disconnected: ~p~n", [State]),
            {noreply, State}
    end;
handle_info({tcp, _Socket, Bin}, State) ->
    Data = State#state.buffer ++ binary_to_list(Bin),
    {ok, NewState} = handle_command(State, Data),
    {noreply, NewState};
handle_info({tcp_closed, _Socket}, State) ->
    NewState = disconnect_state(State),
    {noreply, NewState, ?RECONNECT_DELAY};
handle_info(Info,  State) ->
    io:format("UNHANDLED handle_info ~p ~p~n", [Info, State]),
    {noreply, State}.

terminate(_Reason, #state{socket=Socket}) ->
    % io:format("~p stopping~n", [?MODULE]),
    case Socket of
        not_connected ->
            void;
        _ ->
            gen_tcp:close()
    end,
    ok.

handle_cast(_Msg, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%

disconnect_state(State) ->
    State#state.pidparent ! {self(), disconnected},
    gen_tcp:close(State#state.socket),
    State#state{socket=not_connected, buffer=[]}.


handle_command(State, Packet) ->
    {NewPacket, Command, Args} = parse_command(Packet),
    case Command of
        void ->
            {ok, State#state{buffer=NewPacket}};
        _ ->
            State#state.pidparent ! {self(), command, Command, Args},
            handle_command(State, NewPacket)
    end.

%% Parse a list for a comamdna and return {rest_of_data:list, command:atom, arguments:tuple}.
%% If no command then command and arguments are 'void'
parse_command(Data) when length(Data) >= 12 ->
    <<"\000RES", CommandID:32/big, DataLength:32/big, Rest/binary>> = list_to_binary(Data),
    if
        size(Rest) >= DataLength ->
            {ArgData, NewPacket} = split_binary(Rest, DataLength),
            {Command, ArgList} = parse_command(CommandID, binary_to_list(ArgData)),
            {binary_to_list(NewPacket), Command, ArgList};
        true ->
            {Data, void, void}
    end;
parse_command(Data) ->
    {Data, void, void}.

parse_command(22, ClientID) -> {set_client_id, {ClientID}};
parse_command(16, Text) -> {echo_req, {Text}};
parse_command(12, Data) ->
    [Handle, Numerator, Denominator] = split(Data, 0, 2),
    {work_status, {Handle, list_to_integer(Numerator), list_to_integer(Denominator)}};
parse_command(13, Data) -> {work_complete, list_to_tuple(split(Data, 0, 1))}; % Handle, Result
parse_command(14, Handle) -> {work_fail, Handle};
parse_command(17, Text) -> {echo_res, Text};
parse_command(19, Data) ->
    [Code, Text] = split(Data, 0, 1),
    {error, {list_to_integer(Code), Text}};
parse_command(1, Func) -> {can_do, {Func}};
parse_command(23, Data) ->
    [Func, Timeout] = split(Data, 0, 1),
    {can_do_timeout, {Func, list_to_integer(Timeout)}};
parse_command(2, Func) -> {cant_do, {Func}};
parse_command(3, []) -> {reset_abilities, {}};
parse_command(4, []) -> {pre_sleep, {}};
parse_command(9, []) -> {grab_job, {}};
parse_command(11, Data) -> {job_assign, list_to_tuple(split(Data, 0, 2))}; % Handle, Func, Arg
parse_command(24, []) -> {all_yours, {}};
parse_command(6, []) -> {noop, {}};
parse_command(10, []) -> {no_job, {}};
parse_command(7, Data) -> {submit_job, list_to_tuple(split(Data, 0, 2))}; % Func, Uniq, Arg
parse_command(21, Data) -> {submit_job_high, list_to_tuple(split(Data, 0, 2))}; % Func, Uniq, Arg
parse_command(18, Data) -> {submit_job_bg, list_to_tuple(split(Data, 0, 2))}; % Func, Uniq, Arg
parse_command(8, Handle) -> {job_created, {Handle}};
parse_command(15, Handle) -> {get_status, {Handle}};
parse_command(20, Data) ->
    [Handle, Known, Running, Numerator, Denominator] = split(Data, 0, 4),
    {status_res, {Handle, Known, Running, list_to_integer(Numerator), list_to_integer(Denominator)}}.


pack_request(Command, Args) when is_atom(Command), is_tuple(Args) ->
    {CommandID, ArgList} = pack_command(Command, Args),
    pack_command(CommandID, ArgList, "\000REQ").
pack_response(Command, Args) when is_atom(Command), is_tuple(Args) ->
    {CommandID, ArgList} = pack_command(Command, Args),
    pack_command(CommandID, ArgList, "\000RES").
pack_command(CommandID, Args, Magic) when is_integer(CommandID), is_list(Args), is_list(Magic) ->
    Data = list_to_binary(join(Args, [0])),
    DataLength = size(Data),
    list_to_binary([Magic, <<CommandID:32/big, DataLength:32/big>>, Data]).

pack_command(set_client_id, {ClientID}) -> {22, [ClientID]};
pack_command(echo_req, {Text}) -> {16, [Text]};
pack_command(work_status, {Handle, Numerator, Denominator}) -> {12, [Handle, integer_to_list(Numerator), integer_to_list(Denominator)]};
pack_command(work_complete, {Handle, Result}) -> {13, [Handle, Result]};
pack_command(work_fail, {Handle}) -> {14, [Handle]};
pack_command(echo_res, {Text}) -> {17, [Text]};
pack_command(error, {Code, Text}) -> {19, [integer_to_list(Code), Text]};
pack_command(can_do, {Func}) -> {1, [Func]};
pack_command(can_do_timeout, {Func, Timeout}) -> {23, [Func, integer_to_list(Timeout)]};
pack_command(cant_do, {Func}) -> {2, [Func]};
pack_command(reset_abilities, {}) -> {3, []};
pack_command(pre_sleep, {}) -> {4, []};
pack_command(grab_job, {}) -> {9, []};
pack_command(job_assign, {Handle, Func, Arg}) -> {11, [Handle, Func, Arg]};
pack_command(all_yours, {}) -> {24, []};
pack_command(noop, {}) -> {6, []};
pack_command(no_job, {}) -> {10, []};
pack_command(submit_job, {Func, Uniq, Arg}) -> {7, [Func, Uniq, Arg]};
pack_command(submit_job_high, {Func, Uniq, Arg}) -> {21, [Func, Uniq, Arg]};
pack_command(submit_job_bg, {Func, Uniq, Arg}) -> {18, [Func, Uniq, Arg]};
pack_command(job_created, {Handle}) -> {8, [Handle]};
pack_command(get_status, {Handle}) -> {15, [Handle]};
pack_command(status_res, {Handle, Known, Running, Numerator, Denominator}) -> {20, [Handle, Known, Running, integer_to_list(Numerator), integer_to_list(Denominator)]}.


%% Join a list of lists into a single list separated by Separator
join([], _) -> [];
join([Head|Lists], Separator) ->
     lists:flatten([Head | [[Separator, Next] || Next <- Lists]]).

%% Split a list into multiple lists by Separator
split(List, Separator, Count) ->
    split(List, Separator, [], [], Count).

split([], _, [], [], _) -> [];  %% empty set
split([], _, Lists, Current, _) -> Lists ++ [Current]; %% nothing left to split
split(List, _, Lists, [], 0) -> Lists ++ [List];
split([Separator|Rest], Separator, Lists, Current, Count) when Count > 0 ->
    split(Rest, Separator, Lists ++ [Current], [], Count-1);
split([Other|Rest], Separator, Lists, Current, Count) when Count > 0 ->
    split(Rest, Separator, Lists, Current ++ [Other], Count).
