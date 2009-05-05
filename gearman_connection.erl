-module(gearman_connection).
-author('Samuel Stauffer <samuel@descolada.com>').

-export([connect/1, send_request/3, send_response/3]).

-define(DEFAULT_PORT, 4730).
-define(RECHECK_DELAY, 10000). %% milliseconds

-record(state, {host, port, pidparent, socket=void, buffer=[]}).

%% Public

connect({Host}) ->
    connect({Host, ?DEFAULT_PORT});
connect({Host, Port}) ->
    % spawn(?MODULE, connect, [self(), Host, Port]).
    Self = self(),
    spawn(fun() -> connect(Self, Host, Port) end).

send_request(PidConnection, Command, Args) ->
    %% Do the packing here so errors are detected in the calling process
    Packet = pack_request(Command, Args),
    PidConnection ! {self(), send_command, Packet}.

send_response(PidConnection, Command, Args) ->
    %% Do the packing here so errors are detected in the calling process
    Packet = pack_response(Command, Args),
    PidConnection ! {self(), send_command, Packet}.


%% Private

connect(PidParent, Host, Port) ->
    connect_state(#state{host=Host, port=Port, pidparent=PidParent}).

connect_state(#state{host=Host, port=Port} = State) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, 0}]) of
        {ok, Socket} ->
            State#state.pidparent ! {self(), connected},
            alive_loop(State#state{socket=Socket});
        {error, econnrefused} ->
            dead_loop(State)
    end.

alive_loop(#state{socket=Socket, pidparent=PidParent, buffer=Buffer} = State) ->
    receive
        {tcp, Socket, Bin} ->
            Data = Buffer ++ binary_to_list(Bin),
            {ok, State} = handle_command(State, Data),
            alive_loop(State);
        {tcp_closed, Socket} ->
            mark_dead(State);
        {PidParent, send_command, Packet} ->
            ok = gen_tcp:send(Socket, Packet),
            alive_loop(State)
    end.

dead_loop(#state{pidparent=PidParent} = State) ->
    receive
        {PidParent, connect} ->
            connect_state(State)
    after ?RECHECK_DELAY ->
        connect_state(State)
    end,
    dead_loop(State).

mark_dead(State) ->
    State#state.pidparent ! {self(), disconnected},
    dead_loop(State#state{socket=void, buffer=[]}).

handle_command(State, Packet) ->
    {NewPacket, Command, Args} = parse_command(Packet),
    case Command of
        void ->
            {ok, State#state{buffer=NewPacket}};
        _ ->
            State#state.pidparent ! {self(), command, Command, Args},
            handle_command(State, NewPacket)
    end.


parse_command(Data) when length(Data) >= 12 ->
    <<"\000RES", CommandID:32/big, DataLength:32/big, Rest/binary>> = list_to_binary(Data),
    if
        size(Rest) >= DataLength ->
            {ArgData, NewPacket} = split_binary(Rest, DataLength),
            Args = split(binary_to_list(ArgData), 0),
            {Command, ArgList} = parse_command(CommandID, Args),
            {binary_to_list(NewPacket), Command, ArgList};
        true ->
            {Data, void, void}
    end;
parse_command(Data) ->
    {Data, void, void}.

parse_command(22, [ClientID]) -> {set_client_id, {ClientID}};
parse_command(16, [Text]) -> {echo_req, {Text}};
parse_command(12, [Handle, Numerator, Denominator]) -> {work_status, {Handle, list_to_integer(Numerator), list_to_integer(Denominator)}};
parse_command(13, [Handle, Result]) -> {work_complete, {Handle, Result}};
parse_command(14, [Handle]) -> {work_fail, {Handle}};
parse_command(17, [Text]) -> {echo_res, {Text}};
parse_command(19, [Code, Text]) -> {error, {list_to_integer(Code), Text}};
parse_command(1, [Func]) -> {can_do, {Func}};
parse_command(23, [Func, Timeout]) -> {can_do_timeout, {Func, list_to_integer(Timeout)}};
parse_command(2, [Func]) -> {cant_do, {Func}};
parse_command(3, []) -> {reset_abilities, {}};
parse_command(4, []) -> {pre_sleep, {}};
parse_command(9, []) -> {grab_job, {}};
parse_command(11, [Handle, Func, Arg]) -> {job_assign, {Handle, Func, Arg}};
parse_command(24, []) -> {all_yours, {}};
parse_command(6, []) -> {noop, {}};
parse_command(10, []) -> {no_job, {}};
parse_command(7, [Func, Uniq, Arg]) -> {submit_job, {Func, Uniq, Arg}};
parse_command(21, [Func, Uniq, Arg]) -> {submit_job_high, {Func, Uniq, Arg}};
parse_command(18, [Func, Uniq, Arg]) -> {submit_job_bg, {Func, Uniq, Arg}};
parse_command(8, [Handle]) -> {job_created, {Handle}};
parse_command(15, [Handle]) -> {get_status, {Handle}};
parse_command(20, [Handle, Known, Running, Numerator, Denominator]) -> {status_res, {Handle, Known, Running, list_to_integer(Numerator), list_to_integer(Denominator)}}.



pack_request(Command, Args) when is_atom(Command), is_tuple(Args) ->
    {CommandID, ArgList} = pack_command(Command, Args),
    pack_command(CommandID, ArgList, "\000REQ").
pack_response(Command, Args) when is_atom(Command), is_tuple(Args) ->
    {CommandID, ArgList} = pack_command(Command, Args),
    pack_command(CommandID, ArgList, "\000RES").
pack_command(CommandID, Args, Magic) when is_integer(CommandID), is_list(Args), is_list(Magic) ->
    Data = join(Args, [0]),
    DataLength = length(Data),
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
split(List, Separator) ->
    split(List, Separator, [], []).
split([], _, [], []) -> [];
split([], _, Lists, Current) -> Lists ++ [Current];
split([Separator|Rest], Separator, Lists, Current) ->
    split(Rest, Separator, Lists ++ [Current], []);
split([Other|Rest], Separator, Lists, Current) ->
    split(Rest, Separator, Lists, Current ++ [Other]).
