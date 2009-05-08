-module(gearman_tests).
-author('Samuel Stauffer <samuel@lefora.com>').

-export([test/0, test_worker/0]).

-include_lib("gearman.hrl").

test_worker() ->
    gearman_worker:start({"127.0.0.1"}, [{"echo", fun(Task) -> Task#task.arg end}, {"fail", fun(Task) -> throw(Task#task.arg) end}]).

test() ->
    gearman_worker:start({"127.0.0.1"}, [{"echo", fun(Task) -> Task#task.arg end}, {"fail", fun(Task) -> throw(Task#task.arg) end}]),
    {ok, P} = gearman_connection:connect({"127.0.0.1"}),
    receive
        {P, connected} -> void;
        Any -> io:format("FAIL: Didn't receive 'connected' from ~p instead got ~p~n", [P, Any])
    end,
    gearman_connection:send_request(P, submit_job, {"echo", "", "Test"}),
    receive
        {P, command, job_created, {Handle}} -> io:format("SUCCESS: Received job_created: ~p~n", [Handle]);
        Any2 -> io:format("FAIL: Didn't receive 'job_created' instead got ~p~n", [Any2])
    end,
    receive
        {P, command, work_complete, {Handle2, Result}} -> io:format("SUCCESS: Received work_complete for ~p: ~p~n", [Handle2, Result]);
        Any3 -> io:format("FAIL: Didn't receive 'work_complete' instead got ~p~n", [Any3])
    end,
    gearman_connection:send_request(P, submit_job, {"fail", "", "Test"}),
    receive
        {P, command, job_created, {Handle3}} -> io:format("SUCCESS: Received job_created: ~p~n", [Handle3]);
        Any4 -> io:format("FAIL: Didn't receive 'job_created' instead got ~p~n", [Any4])
    end,
    receive
        {P, command, work_fail, {Handle4}} -> io:format("SUCCESS: Received work_fail for ~p~n", [Handle4]);
        Any5 -> io:format("FAIL: Didn't receive 'work_complete' instead got ~p~n", [Any5])
    end,
    io:format("TESTS DONE~n").

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
