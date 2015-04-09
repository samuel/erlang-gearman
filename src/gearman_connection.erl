-module(gearman_connection).
-author('Samuel Stauffer <samuel@lefora.com>').

-behaviour(gen_server).

-export([start/0, start_link/0, stop/1, connect/2, send_request/3, send_response/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DEFAULT_PORT, 4730).
-define(RECONNECT_DELAY, 10000). %% milliseconds
-define(CONNECT_TIMEOUT, 30000).

-record(state, {pidparent, host=null, port=null, socket=not_connected, buffer=[]}).

start() ->
    gen_server:start(?MODULE, self(), []).

start_link() ->
    gen_server:start_link(?MODULE, self(), []).

stop(Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, stop).

connect(Pid, Host) when is_pid(Pid), is_list(Host) ->
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
            lager:error("~p gen_tcp:send returned unhandled value ~p", [?MODULE, Any]),
            NewState = disconnect_state(State),
            {reply, {error, Any}, NewState, ?RECONNECT_DELAY}
    catch
        Exc1:Exc2 ->
            lager:error("~p gen_tcp:send raised an exception ~p:~p", [?MODULE, Exc1, Exc2]),
            NewState = disconnect_state(State),
            {reply, {error, {Exc1, Exc2}}, NewState, ?RECONNECT_DELAY}
    end.

handle_info(timeout, #state{host=Host, port=Port, socket=OldSocket} = State) ->
    case OldSocket of
        not_connected ->
            case gen_tcp:connect(Host, Port, [binary, {packet, 0}], ?CONNECT_TIMEOUT) of
                {ok, Socket} ->
                    State#state.pidparent ! {self(), connected},
                    NewState = State#state{socket=Socket},
                    {noreply, NewState};
                {error, econnrefused} ->
                    {noreply, State, ?RECONNECT_DELAY};
                {error, timeout} ->
					{noreply, State, ?RECONNECT_DELAY}
            end;
        _ ->
            lager:error("~p Timeout while socket not disconnected: ~p", [?MODULE, State]),
            {noreply, State}
    end;
handle_info({tcp, _Socket, NewData}, State) ->
    {ok, NewState} = handle_command(State, NewData),
    {noreply, NewState};
handle_info({tcp_closed, _Socket}, State) ->
    NewState = disconnect_state(State),
    {noreply, NewState, ?RECONNECT_DELAY};
handle_info(Info,  State) ->
    lager:error("~p unhandled handle_info ~p ~p", [?MODULE, Info, State]),
    {noreply, State}.

terminate(Reason, #state{socket=Socket}) ->
    lager:error("~p stopping: ~p", [?MODULE, Reason]),
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


handle_command(State, NewData) ->
	Packet = list_to_binary([State#state.buffer, NewData]),
    case gearman_protocol:parse_command(Packet) of
        {error, not_enough_data} ->
            {ok, State#state{buffer=Packet}};
        {ok, NewPacket, response, Command} ->
            State#state.pidparent ! {self(), command, Command},
            handle_command(State#state{buffer=[]}, NewPacket)
    end.

