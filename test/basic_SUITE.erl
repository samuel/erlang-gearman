-module(basic_SUITE).
-author('Samuel Stauffer <samuel@lefora.com>').

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([test_client/1, test_worker/1]).
-export([functions/0, echo/3, fail/3]).

-define(SERVER_TIMEOUT, 5000).
-define(GEARMAN_HOST, "127.0.0.1").

all() -> [test_worker, test_client].
%~ all() -> [test_worker].

init_per_testcase(_Case, Config) ->
	Config.


end_per_testcase(_Case, Config) ->
	Config.

test_worker(_Config) ->
    Result = gearman_worker:start(?GEARMAN_HOST, [?MODULE]),
    ?assertMatch({ok, _}, Result),
    {comment, "OK"}.

test_client(_Config) ->
    {ok, _Pid}       = gearman_worker:start(?GEARMAN_HOST, [?MODULE]),
    {ok, P} = gearman_connection:start_link(),
    ok      = gearman_connection:connect(P, ?GEARMAN_HOST),

    receive
        {P, connected} -> ok
	after ?SERVER_TIMEOUT ->
		ct:fail("Connection to gearman server timedout")
    end,

    gearman_connection:send_request(P, submit_job, {"echo", "", "Test"}),
    receive
        {P, command, {job_created, _Handle}} -> ok
    after ?SERVER_TIMEOUT ->
		ct:fail("creating task 'echo' failed")
    end,
    receive
        {P, command, {work_complete, _Handle2, _Result}} -> ok;
        Any -> ct:fail(Any)
    after ?SERVER_TIMEOUT ->
		ct:fail("Didn't receive 'work_complete' for 'echo' task")
    end,

    gearman_connection:send_request(P, submit_job, {"fail", "", "Test"}),
    receive
        {P, command, {job_created, _Handle3}} -> ok
	after ?SERVER_TIMEOUT ->
		ct:fail("creating task 'fail' failed")
    end,
    receive
        {P, command, {work_fail, _Handle4}} -> ok
    after ?SERVER_TIMEOUT ->
		ct:fail("Didn't receive 'work_complete' for 'echo' task")
    end,

    {comment, "OK"}.

    % P = connect({"127.0.0.1", 4730}),
    % send_request(P, echo_req, {"Test"}),
    % receive
    %     {P, command, echo_res, {"Test"}} ->
    %         io:format("SUCCESS: Received valid echo response~n");
    %     Any ->
    %         io:format("FAIL: Received unexpected message ~p~n", [Any])
    % end,
    % send_request(P, grab_job, {}),
    % receive
    %     {P, command, no_job, {}} ->
    %         io:format("SUCCESS: Received no_job response~n");
    %     Any2 ->
    %         io:format("FAIL: Received unexpected message ~p~n", [Any2])
    % end.

%% ====================================================================
%% gearman worker functions
%% ====================================================================


functions() ->
	[{"echo", fun echo/3}, {"fail", fun fail/3}].

echo(_Handle, _Func, Arg) ->
	Arg.

fail(_Handle, _Func, Arg) ->
	throw(Arg).
