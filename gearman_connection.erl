-module(gearman_connection).
-author('Samuel Stauffer <samuel@lefora.com>').

-behaviour(gen_server).

-export([start/0, start_link/0, stop/1, connect/2, send_request/3, send_response/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DEFAULT_PORT, 4730).
-define(RECONNECT_DELAY, 10000). %% milliseconds

-record(state, {pidparent, host=null, port=null, socket=not_connected, buffer=[]}).

start() ->
    gen_server:start(?MODULE, self(), []).

start_link() ->
    gen_server:start_link(?MODULE, self(), []).

stop(Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, stop).

connect(Pid, Host) when is_list(Host) ->
    connect(Pid, {Host, ?DEFAULT_PORT});
connect(Pid, {Host, Port}) when is_pid(Pid) ->
    gen_server:call(Pid, {connect, Host, Port}).

send_request(Pid, Command, Args) when is_pid(Pid) ->
    % Do the packing here so errors are detected in the calling process
    Packet = gearman_protocol:pack_request(Command, Args),
    gen_server:call(Pid, {send_command, Packet}).

send_response(Pid, Command, Args) when is_pid(Pid) ->
    % Do the packing here so errors are detected in the calling process
    Packet = gearman_protocol:pack_response(Command, Args),
    gen_server:call(Pid, {send_command, Packet}).

%%%------------------------------------------------------------------------
%%% Callback functions from gen_server
%%%------------------------------------------------------------------------

init(PidParent) ->
    {ok, #state{pidparent=PidParent}}.

handle_call({connect, Host, Port}, _From, State) ->
    NewState = State#state{host=Host, port=Port},
    {reply, ok, NewState, 0};
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
    end.

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
handle_info({tcp, _Socket, Bin}, #state{buffer=Buffer} = State) ->
    Data = list_to_binary([Buffer, Bin]),
    {ok, NewState} = handle_command(State, Data),
    {noreply, NewState};
handle_info({tcp_closed, _Socket}, State) ->
    NewState = disconnect_state(State),
    {noreply, NewState, ?RECONNECT_DELAY};
handle_info(Info,  State) ->
    io:format("UNHANDLED handle_info ~p ~p~n", [Info, State]),
    {noreply, State}.

terminate(Reason, #state{socket=Socket}) ->
    io:format("~p stopping: ~p~n", [?MODULE, Reason]),
    case Socket of
        not_connected ->
            void;
        _ ->
            gen_tcp:close(Socket)
    end,
    ok.

handle_cast(stop, State) ->
    {stop, normal, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%

disconnect_state(State) ->
    State#state.pidparent ! {self(), disconnected},
    gen_tcp:close(State#state.socket),
    State#state{socket=not_connected, buffer=[]}.


handle_command(State, Packet) ->
    case gearman_protocol:parse_command(Packet) of
        {error, not_enough_data} ->
            {ok, State};
        {ok, NewPacket, response, Command} ->
            State#state.pidparent ! {self(), command, Command},
            handle_command(State, NewPacket)
    end.
