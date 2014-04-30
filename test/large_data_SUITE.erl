-module(large_data_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([large_data_test/1]).
-export([functions/0, large_data/3]).

-define(SERVER_TIMEOUT, 5000).
-define(GEARMAN_HOST, "127.0.0.1").
-define(DATA_SIZE, 65000).

all() -> [large_data_test].

init_per_testcase(_Case, Config) ->
	Config.

end_per_testcase(_Case, Config) ->
	Config.

large_data_test(_Config) ->
    {ok, _Pid}       = gearman_worker:start(?GEARMAN_HOST, [?MODULE]),
    {ok, P} = gearman_connection:start_link(),
    ok      = gearman_connection:connect(P, ?GEARMAN_HOST),
    receive
        {P, connected} -> ok
	after ?SERVER_TIMEOUT ->
		ct:fail("Connection to gearman server timedout")
    end,


	Data = lists:merge(lists:duplicate(?DATA_SIZE, "1")),
    gearman_connection:send_request(P, submit_job, {"large_data", "", Data}),
    receive
        {P, command, {job_created, _Handle}} -> ok
    after ?SERVER_TIMEOUT ->
		ct:fail("creating task 'large_data' failed")
    end,
    receive
        {P, command, {work_complete, _Handle2, Result}} ->
			?assertEqual(io_lib:print(?DATA_SIZE), Result);
        Any ->
			ct:fail({work_complete, Any})
    after ?SERVER_TIMEOUT ->
		ct:fail("Didn't receive 'work_complete' for 'large_data' task")
    end
.

functions() ->
	[{"large_data", fun large_data/3}].

large_data(_, _, Arg) ->
	io_lib:print(size(list_to_binary(Arg))).
