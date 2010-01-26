-module (drmaa).
-author ('sergey.miryanov@gmail.com').
-export ([test/1]).

-behaviour (gen_server).

-export ([start_link/0]).

%% gen_server
-export ([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

%% API
-export ([allocate_job_template/0, delete_job_template/0]).
-export ([run_job/0]).
-export ([remote_command/1, v_argv/1]).

%% Internal
-export ([control/2, control/3]).

-define ('CMD_ALLOCATE_JOB_TEMPLATE',   1).
-define ('CMD_DELETE_JOB_TEMPLATE',     2).
-define ('CMD_RUN_JOB',                 3).
-define ('CMD_RUN_BULK_JOBS',           4).
-define ('CMD_CONTROL',                 5).
-define ('CMD_JOB_PS',                  6).
-define ('CMD_SYNCHRONIZE',             7).
-define ('CMD_WAIT',                    8).
-define ('CMD_BLOCK_EMAIL',             9).
-define ('CMD_DEADLINE_TIME',           10).
-define ('CMD_DURATION_HLIMIT',         11).
-define ('CMD_DURATION_SLIMIT',         12).
-define ('CMD_ERROR_PATH',              13).
-define ('CMD_INPUT_PATH',              14).
-define ('CMD_JOB_CATEGORY',            15).
-define ('CMD_JOB_NAME',                16).
-define ('CMD_JOIN_FILES',              17).
-define ('CMD_JS_STATE',                18).
-define ('CMD_NATIVE_SPECIFICATION',    19).
-define ('CMD_OUTPUT_PATH',             20).
-define ('CMD_REMOTE_COMMAND',          21).
-define ('CMD_START_TIME',              22).
-define ('CMD_TRANSFER_FILES',          23).
-define ('CMD_V_ARGV',                  24).
-define ('CMD_V_EMAIL',                 25).
-define ('CMD_V_ENV',                   26).
-define ('CMD_WCT_HLIMIT',              27).
-define ('CMD_WCT_SLIMIT',              28).
-define ('CMD_WD',                      29).

-record (state, {port, ops = []}).

start_link () ->
  gen_server:start_link ({local, drmaa}, ?MODULE, [], []).

%% API %%
allocate_job_template () ->
  gen_server:call (drmaa, {allocate_job_template}).

delete_job_template () ->
  gen_server:call (drmaa, {delete_job_template}).

run_job () ->
  gen_server:call (drmaa, {run_job}).

remote_command (Command) when is_list (Command) ->
  gen_server:call (drmaa, {remote_command, Command}).

v_argv (Argv) when is_list (Argv) ->
  gen_server:call (drmaa, {v_argv, Argv}).


%% gen_server callbacks %%

init ([]) ->
  process_flag (trap_exit, true),
  SearchDir = filename:join ([filename:dirname (code:which (?MODULE)), "..", "ebin"]),
  case erl_ddll:load (SearchDir, "drmaa_drv")
  of
    ok -> 
      Port = open_port ({spawn, "drmaa_drv"}, [binary]),
      {ok, #state {port = Port, ops = []}};
    {error, Error} ->
      io:format ("Error loading drmaa driver: ~p~n", [erl_ddll:format_error (Error)]),
      {stop, failed}
  end.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

handle_cast (_Msg, State) ->
  {noreply, State}.

handle_info (_Info, State) ->
  {noreply, State}.

terminate (normal, #state {port = Port}) ->
  port_command (Port, term_to_binary ({close, nop})),
  port_close (Port),
  ok;
terminate (_Reason, _State) ->
  ok.

handle_call ({allocate_job_template}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_ALLOCATE_JOB_TEMPLATE),
  {reply, Reply, State};
handle_call ({delete_job_template}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_DELETE_JOB_TEMPLATE),
  {reply, Reply, State};
handle_call ({run_job}, _From, #state {port = Port} = State) ->
  %{ok, JobID} = drmaa:control (Port, ?CMD_RUN_JOB),
  case drmaa:control (Port, ?CMD_RUN_JOB)
  of
    {ok, JobID} -> {reply, {ok, JobID}, State};
    Other       -> {reply, {unknown, Other}, State}
  end;
  %{reply, {ok, JobID}, State};
handle_call ({remote_command, Command}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_REMOTE_COMMAND, erlang:list_to_binary (Command)),
  {reply, Reply, State};
handle_call ({v_argv, Argv}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_V_ARGV, erlang:list_to_binary (Argv)),
  {reply, Reply, State};
handle_call (Request, _From, State) ->
  {reply, {unknown, Request}, State}.


control (Port, Command) when is_port (Port) and is_integer (Command) ->
  port_control (Port, Command, <<>>),
  wait_result (Port).

control (Port, Command, Data) 
  when is_port (Port) and is_integer (Command) and is_binary (Data) ->
    port_control (Port, Command, Data),
    wait_result (Port).

wait_result (Port) ->
  receive
    {ok, Reply} -> 
      {ok, Reply};
    {error, Reason} ->
      io:format("Error: ~p~n", [Reason]),
      {error, Reason};
	  Unknown ->
      io:format("Unknown: ~p~n", [Unknown]),
	    Unknown
  end.

test (Port) ->
  port_control (Port, 1, <<"xxx">>),
  port_control (Port, 2, <<"xxx">>),
  port_control (Port, 3, <<"xxx">>),
  port_control (Port, 4, <<"xxx">>).
