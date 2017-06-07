-module(gearman_client).
-author('Egbert Groot <egbert.groot@voiceworks.com>').

%% TODO: connect/disconnect handling
%% disconnect/stop gearman on exit
%% gearman port configurable
%% more methods (backgroud job etc)
%% time logging

-behaviour(gen_server).

%% gen_server callbacks/api
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, start_link/1, start/1]).
%% API
-export([do_normal/3,do_normal_cast/3]).

%% test
-export([timestamp_diff/2]).

%% ====================================================================
%% API functions
%% ====================================================================

start(GearmanHost) ->
    Name=list_to_atom(atom_to_list(?MODULE)++"_"++GearmanHost),
    error_logger:info_msg("gearman client start name ~p~n",[Name]),
    gen_server:start({local, Name}, ?MODULE, [GearmanHost], []).
start_link(GearmanHost) ->
    Name=list_to_atom(atom_to_list(?MODULE)++"_"++GearmanHost),
    gen_server:start_link({local, Name}, ?MODULE, [GearmanHost], []).

do_normal(GC,Function,Data) ->
    try gen_server:call(GC,{do_normal,Function,Data},4000) catch    %% timeout of 4000, this is lower than normal timeout of 5000.
                                                                    %% call should fail here first, not in calling process
        exit:{timeout,{gen_server,call,[_,{do_normal,_,_},_]}} ->
            error_logger:error_msg("timeout for gearman call to function ~p~n",[Function]),
            {error,timeout};
        Error:Reason ->
            error_logger:error_msg("error ~p with reason ~p for gearman call to function ~p~n",[Error,Reason,Function]),
            {error,{Error,Reason}}
    end.

do_normal_cast(GC,Function,Data) ->
    try gen_server:cast(GC,{do_normal,Function,Data}) catch
        Error:Reason ->
            error_logger:error_msg("error ~p with reason ~p for gearman call to function ~p~n",[Error,Reason,Function]),
            {error,{Error,Reason}}
    end.

%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state,{gearman_host, gearman_connection, current_job, job_requests=[], jobs_inprogress=[], timer}).

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
-spec init(Args :: term()) -> Result when
    Result :: {ok, State}
            | {ok, State, Timeout}
            | {ok, State, hibernate}
            | {stop, Reason :: term()}
            | ignore,
    State :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init([GearmanHost]) ->
    error_logger:info_msg("gearman client starting for host ~p~n",[GearmanHost]),
    GC=get_gearman_connection(GearmanHost),
    {ok, #state{gearman_host=GearmanHost,gearman_connection=GC}}.

%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
    Result :: {reply, Reply, NewState}
            | {reply, Reply, NewState, Timeout}
            | {reply, Reply, NewState, hibernate}
            | {noreply, NewState}
            | {noreply, NewState, Timeout}
            | {noreply, NewState, hibernate}
            | {stop, Reason, Reply, NewState}
            | {stop, Reason, NewState},
    Reply :: term(),
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().
%% ====================================================================
handle_call(Job={do_normal,Function,_Data}, From, State) ->
    error_logger:info_msg("New job received for function ~p",[Function]),
    Job_requests=[{From,os:timestamp(),Job}|State#state.job_requests],
    gen_server:cast(self(),jobsubmit),
    NewState=State#state{job_requests=Job_requests},
    log_queues(NewState),
    {noreply, NewState};

handle_call(_Request, _From, State) ->
    Reply = error,
    {reply, Reply, State}.


%% handle_cast/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
-spec handle_cast(Request :: term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
            | {noreply, NewState, Timeout}
            | {noreply, NewState, hibernate}
            | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast(jobsubmit, State=#state{current_job={_From,Time,_Job}}) ->
    TimeDiff=timestamp_to_millisecs(timestamp_diff(os:timestamp(),Time)),
    NewState=handle_gearman_timeout(TimeDiff, State),
    Timer=set_jobwaiting_timer(State#state.current_job,State#state.timer),
    {noreply, NewState#state{timer=Timer}};

handle_cast(jobsubmit, State=#state{job_requests=[]}) ->
    Timer=set_jobwaiting_timer(State#state.current_job,State#state.timer),
    {noreply, State#state{timer=Timer}};

handle_cast(jobsubmit, State) ->
    %%-> jobsubmit, check currentjob=undefined, otherwise nothing. Shift job from jobrequests, Submit job to gearman, put job in currentjob, set timeout when jobrequest not empty
    %% get last item from job_requests
    [FirstJob|Jobs]=lists:reverse(State#state.job_requests),
    {From,_Time,Job}=FirstJob,
    CurrentJob={From,os:timestamp(),Job},
    send_job(Job,State#state.gearman_connection),
    Timer=set_jobwaiting_timer(CurrentJob,State#state.timer ),
    NewState=State#state{timer=Timer,current_job=CurrentJob,job_requests=lists:reverse(Jobs)},
    {noreply, NewState};

handle_cast(Job={do_normal,Function,_Data}, State) ->
    error_logger:info_msg("New cast job received for function ~p",[Function]),
    Job_requests=[{is_cast,os:timestamp(),Job}|State#state.job_requests],
    gen_server:cast(self(),jobsubmit),
    NewState=State#state{job_requests=Job_requests},
    log_queues(NewState),
    {noreply, NewState}.

set_jobwaiting_timer(_CurrentJobs=undefined, Timer) ->
    cancel_timer(Timer),
    undefined;

set_jobwaiting_timer(_CurrentJobs, Timer) ->
    cancel_timer(Timer),
    erlang:send_after(1000, self(), jobsubmit_timer).

cancel_timer(undefined) ->
    undefined;

cancel_timer(Timer) ->
    erlang:cancel_timer(Timer),
    undefined.

send_job(_Job={do_normal,Function,Data}, GearmanConnection) ->
    gearman_connection:send_request(GearmanConnection, submit_job, {Function, "", Data});

send_job(Job,_) ->
    error_logger:error_msg("unknown job type: ~p", [Job]).


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
            | {noreply, NewState, Timeout}
            | {noreply, NewState, hibernate}
            | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_info(jobsubmit_timer, State) ->
    handle_cast(jobsubmit, State#state{timer=undefined});

%% {<0.9794.3>,command,{job_created,"H:hostname:identifier"}}
handle_info({_,command,{job_created,Handle}}, State) ->
    %%submitted_job={From, Timestamp, job, Handle}
    {From,_,Job} = State#state.current_job,
    JIP=[{From,os:timestamp(),Job,Handle}|State#state.jobs_inprogress],
    NewState=State#state{current_job=undefined,jobs_inprogress=JIP},
    gen_server:cast(self(),jobsubmit),
    {noreply,NewState};


%% {<0.9794.3>,command,{work_complete,"H:hostname:identifier","helloworld\n"}}
handle_info({_,command,{work_complete,Handle, Result}}, State) ->
    %%io:format("gearman_client, info received work_complete for: ~p with state: ~p~n",[Handle,State]),
    %%submitted_job={From, Timestamp, job, Handle}
    JobsInProgress=State#state.jobs_inprogress,
    case lists:keyfind(Handle, 4, JobsInProgress) of
        {From, _Timestamp, _Job, Handle}    ->
                          send_reply(From,Result),
                          NewJobsInProgress=lists:keydelete(Handle, 4, JobsInProgress);
        _               ->  error_logger:error_msg("Received 'work complete' for ~p with value ~p, but no pending jobs when state ~p~n",[Handle,Result,State]),
                          NewJobsInProgress=JobsInProgress
    end,
    {noreply, State#state{jobs_inprogress=NewJobsInProgress}};

%% {self(), connected} -> pending requests foutmelding returnen en afsluiten? (er komt niet altijd een disconnect bij bijv een timeout)
%%{<0.22071.3>,connected}
handle_info({_,connected}, State) ->
    %%io:format("gearman_client, info received connected with state: ~p~n",[State]),
    %%submitted_job={From, Timestamp, job, Handle}
    JobsInProgress=State#state.jobs_inprogress,
    error_logger:info_msg("Gearman connected, cancelling current jobs in progress: ~p",[JobsInProgress]),
    lists:foreach(fun({From,_,_,_}) -> send_reply(From,error) end, JobsInProgress),
    {noreply, State#state{jobs_inprogress=[]}};

%% {self(), disconnected} -> pending requests foutmelding returnen en afsluiten?
handle_info({_,disconnected}, State) ->
    %%io:format("gearman_client, info received disconnected with state: ~p~n",[State]),
    error_logger:error_msg("stress, gearman disconnected when state: ~p",[State]),
    handle_info({undefined,connected}, State);

handle_info(Info, State) ->
    error_logger:info_msg("gearman_client, info: ~p~n",[Info]),
    {noreply, State}.



%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
    Reason :: normal
            | shutdown
            | {shutdown, term()}
            | term().
%% ====================================================================
terminate(_Reason, _State) ->
    ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term()},
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

send_reply(is_cast,_Result) ->
    ok;
send_reply(From,Result) ->
    gen_server:reply(From,Result).

timestamp_diff(T,T) ->
    0;
timestamp_diff(T1,T2) when T1<T2 ->
    timestamp_diff(T2,T1);
timestamp_diff({MS1,S1,US1},{MS2,S2,US2}) when US2>US1 ->
    timestamp_diff({MS1,S1-1,US1+1000000},{MS2,S2,US2});
timestamp_diff({MS1,S1,US1},{MS2,S2,US2}) when S2>S1 ->
    timestamp_diff({MS1-1,S1+1000000,US1},{MS2,S2,US2});
timestamp_diff({MS1,S1,US1},{MS2,S2,US2}) ->
    {MS1-MS2,S1-S2,US1-US2}.

timestamp_to_millisecs({MS1,S1,US1}) ->
    (MS1*1000000*1000) + (S1*1000) + (US1 div 1000).

get_gearman_connection(GearmanHost) ->
    {ok,Pid}=gearman_connection:start_link(),
    gearman_connection:connect(Pid, GearmanHost),
    Pid.

restart_gearman(GearmanHost,CurrentGearman) ->
    gearman_connection:stop(CurrentGearman),
    get_gearman_connection(GearmanHost).

handle_gearman_timeout(TimeDiff, State)  when TimeDiff>=3000 ->
    error_logger:error_msg("Gearman timeout, did not get a jobhandle in time (3sec)"),
    log_queues(State),
    CurrentGearman=State#state.gearman_connection,
    GearmanHost=State#state.gearman_host,

    %% move all jobs back to job_requests list. Leave original timestamps
    %% Job_requests=[{From,os:timestamp(),Job}|State#state.job_requests],
    %% current_job={From,os:timestamp(),Job}
    %% JIP=[{From,os:timestamp(),Job,Handle}|State#state.jobs_inprogress],
    NewRequests=lists:append([State#state.job_requests,[State#state.current_job],[ {From,Time,Job} || {From,Time,Job,_Handle} <- State#state.jobs_inprogress]]),
    NewCurrent=undefined,
    NewInProgress=[],
    GC=restart_gearman(GearmanHost,CurrentGearman), %% restart gearman connection
    gen_server:cast(self(),jobsubmit),
    State#state{gearman_connection=GC,job_requests=NewRequests,current_job=NewCurrent,jobs_inprogress=NewInProgress};

handle_gearman_timeout(_TimeDiff, State) ->     %% no timeout yet.
    State.

log_queues(State) ->
    NumberOfRequests=length(State#state.job_requests),
    NumberOfProgress=length(State#state.jobs_inprogress),
    WaitingHandle=case State#state.current_job of undefined->0; _->1 end,
    error_logger:info_msg("Number of job requests without handle: ~p, number of jobs waiting for result: ~p, job waiting for handle: ~p",[NumberOfRequests,NumberOfProgress,WaitingHandle]).
