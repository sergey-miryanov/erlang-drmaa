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
-export ([run_job/0, run_jobs/3, run_jobs/1]).
-export ([wait/1, synchronize/1]).
-export ([join_files/1]).
-export ([remote_command/1, args/1, env/1, emails/1]).
-export ([job_state/1, working_dir/1]).
-export ([job_name/1]).
-export ([input_path/1, output_path/1, error_path/1]).
-export ([job_category/1, native_spec/1]).
-export ([block_email/1]).
-export ([start_time/1, deadline_time/1]).
-export ([hlimit/1, slimit/1, hlimit_duration/1, slimit_duration/1]).
-export ([transfer_files/1]).
-export ([placeholder/1]).
-export ([job_ids/1]).

%% Internal
-export ([control/2, control/3]).
-export ([pair_array_to_vector/1]).

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
-define ('CMD_PLACEHOLDER_HD',          30).
-define ('CMD_PLACEHOLDER_WD',          31).
-define ('CMD_PLACEHOLDER_INCR',        32).
-define ('CMD_JOB_IDS_SESSION_ALL',     33).
-define ('CMD_JOB_IDS_SESSION_ANY',     34).

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

run_jobs (Start, End, Incr) when is_integer (Start) 
                             and is_integer (End)
                             and is_integer (Incr) 
                             and (Start > 0) 
                             and (Start =< End) 
                             and (End =< 2147483647) ->
  gen_server:call (drmaa, {run_jobs, Start, End, Incr}).
run_jobs (Count) when is_integer (Count) and (Count > 0) ->
  gen_server:call (drmaa, {run_jobs, 1, Count, 1}).

wait (any) -> 
  gen_server:call (drmaa, {wait, job_ids (any)});
wait (JobID) when is_list (JobID) ->
  gen_server:call (drmaa, {wait, JobID}).

synchronize (Jobs) when is_list (Jobs) ->
  gen_server:call (drmaa, {sync, Jobs}, 30000).

join_files (true) ->
  gen_server:call (drmaa, {join_files, "y"});
join_files (false) ->
  gen_server:call (drmaa, {join_files, "n"}).

remote_command (Command) when is_list (Command) ->
  gen_server:call (drmaa, {remote_command, Command}).

args (Argv) when is_list (Argv) ->
  gen_server:call (drmaa, {args, Argv}).

env (Env) when is_list (Env) ->
  gen_server:call (drmaa, {env, Env}).

emails (Emails) when is_list (Emails) ->
  gen_server:call (drmaa, {emails, Emails}).

job_state (active) ->
  gen_server:call (drmaa, {job_state, "drmaa_active"});
job_state (hold) ->
  gen_server:call (drmaa, {job_state, "drmaa_hold"}).

working_dir (Dir) when is_list (Dir) ->
  gen_server:call (drmaa, {wd, Dir}).

job_name (JobName) when is_list (JobName) ->
  gen_server:call (drmaa, {job_name, JobName}).

input_path (InputPath) when is_list (InputPath) ->
  {error, not_supported}.
  %gen_server:call (drmaa, {input_path, InputPath}).

output_path (OutputPath) when is_list (OutputPath) ->
  gen_server:call (drmaa, {output_path, OutputPath}).

error_path (ErrorPath) when is_list (ErrorPath) ->
  gen_server:call (drmaa, {error_path, ErrorPath}).

job_category (_JobCategory) ->
  {error, not_supported}.

native_spec (_NativeSpec) ->
  {error, not_supported}.

block_email (true) ->
  gen_server:call (drmaa, {block_email, "1"});
block_email (false) ->
  gen_server:call (drmaa, {block_email, "0"}).

start_time (_StartTime) ->
  {error, not_supported}.

deadline_time (_DeadlineTime) ->
  {error, not_supported}.

hlimit (_HLimit) ->
  {error, not_supported}.

slimit (_SLimit) ->
  {error, not_supported}.

hlimit_duration (_Duration) ->
  {error, not_supported}.

slimit_duration (_Duration) ->
  {error, not_supported}.

transfer_files (List) when is_list (List) ->
  gen_server:call (drmaa, {transfer_files, List}).

placeholder (incr) ->
  gen_server:call (drmaa, {placeholder_incr});
placeholder (hd) ->
  gen_server:call (drmaa, {placeholder_hd});
placeholder (home) ->
  gen_server:call (drmaa, {placeholder_hd});
placeholder (home_dir) ->
  gen_server:call (drmaa, {placeholder_hd});
placeholder (wd) ->
  gen_server:call (drmaa, {placeholder_wd});
placeholder (working_dir) ->
  gen_server:call (drmaa, {placeholder_wd}).

job_ids (all) ->
  gen_server:call (drmaa, {job_ids_all});
job_ids (any) ->
  gen_server:call (drmaa, {job_ids_any}).

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
  {ok, JobID} = drmaa:control (Port, ?CMD_RUN_JOB),
  {reply, {ok, JobID}, State};
handle_call ({wait, JobID}, _From, #state {port = Port} = State) ->
  {ok, {exit, Exit}, {exit_status, ExitStatus}, {usage, Usage}} 
    = drmaa:control (Port, ?CMD_WAIT, erlang:list_to_binary (JobID)),
  {reply, {ok, {exit, Exit}, {exit_status, ExitStatus}, {usage, Usage}}, State};
handle_call ({sync, Jobs}, _From, #state { port = Port} = State) ->
  Len = length (Jobs),
  List = [erlang:integer_to_list (Len) | sync_jobs_to_list (Port, Jobs)],
  Args = string:join (List, ","),
  Reply = drmaa:control (Port, ?CMD_SYNCHRONIZE, erlang:list_to_binary (Args)),
  {reply, Reply, State};
handle_call ({join_files, Join}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_JOIN_FILES, erlang:list_to_binary (Join)),
  {reply, Reply, State};
handle_call ({remote_command, Command}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_REMOTE_COMMAND, erlang:list_to_binary (Command)),
  {reply, Reply, State};
handle_call ({args, Argv}, _From, #state {port = Port} = State) ->
  Len = length (Argv),
  List = [erlang:integer_to_list (Len) | Argv],
  Args = string:join (List, ","),
  Reply = drmaa:control (Port, ?CMD_V_ARGV, erlang:list_to_binary (lists:flatten (Args))),
  {reply, Reply, State};
handle_call ({env, Env}, _From, #state {port = Port} = State) ->
  Buffer = pair_array_to_vector (Env),
  Len = length (Env),
  List = [erlang:integer_to_list (Len) | Buffer],
  Args = string:join (List, ","),
  Reply = drmaa:control (Port, ?CMD_V_ENV, erlang:list_to_binary (Args)),
  {reply, Reply, State};
handle_call ({emails, Emails}, _From, #state {port = Port} = State) ->
  Len = length (Emails),
  List = [erlang:integer_to_list (Len) | Emails],
  Args = string:join (List, ","),
  Reply = drmaa:control (Port, ?CMD_V_EMAIL, erlang:list_to_binary (Args)),
  {reply, Reply, State};
handle_call ({job_state, JobState}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_JS_STATE, erlang:list_to_binary (JobState)),
  {reply, Reply, State};
handle_call ({wd, WorkingDir}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_WD, erlang:list_to_binary (WorkingDir)),
  {reply, Reply, State};
handle_call ({input_path, InputPath}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_INPUT_PATH, erlang:list_to_binary (InputPath)),
  {reply, Reply, State};
handle_call ({output_path, OutputPath}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_OUTPUT_PATH, erlang:list_to_binary (OutputPath)),
  {reply, Reply, State};
handle_call ({error_path, ErrorPath}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_ERROR_PATH, erlang:list_to_binary (ErrorPath)),
  {reply, Reply, State};
handle_call ({block_email, BlockEmail}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_BLOCK_EMAIL, erlang:list_to_binary (BlockEmail)),
  {reply, Reply, State};
handle_call ({transfer_files, List}, _From, #state {port = Port} = State) ->
  TF = list_to_transfer_files (List),
  Reply = drmaa:control (Port, ?CMD_TRANSFER_FILES, erlang:list_to_binary (TF)),
  {reply, Reply, State};
handle_call ({job_name, JobName}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_JOB_NAME, erlang:list_to_binary (JobName)),
  {reply, Reply, State};
handle_call ({run_jobs, Start, End, Incr}, _From, #state {port = Port} = State) ->
  List = [erlang:integer_to_list (Start), erlang:integer_to_list (End), erlang:integer_to_list (Incr)],
  Buffer = erlang:list_to_binary (string:join (List, ",")),
  Reply = drmaa:control (Port, ?CMD_RUN_BULK_JOBS, Buffer),
  {reply, Reply, State};
handle_call ({placeholder_hd}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_PLACEHOLDER_HD),
  {reply, Reply, State};
handle_call ({placeholder_wd}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_PLACEHOLDER_WD),
  {reply, Reply, State};
handle_call ({placeholder_incr}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_PLACEHOLDER_INCR),
  {reply, Reply, State};
handle_call ({job_ids_all}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_JOB_IDS_SESSION_ALL),
  {reply, Reply, State};
handle_call ({job_ids_any}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control (Port, ?CMD_JOB_IDS_SESSION_ANY),
  {reply, Reply, State};
handle_call (Request, _From, State) ->
  {reply, {unknown, Request}, State}.


control (Port, Command) when is_port (Port) and is_integer (Command) ->
  port_control (Port, Command, <<"_">>),
  wait_result (Port).

control (Port, Command, Data) 
  when is_port (Port) and is_integer (Command) and is_binary (Data) ->
    port_control (Port, Command, Data),
    wait_result (Port);
control (Port, Command, <<>>)
  when is_port (Port) and is_integer (Command) ->
    port_control (Port, Command, <<"_">>),
    wait_result (Port).


wait_result (_Port) ->
  receive
	  Smth -> Smth
  end.

pair_array_to_vector (Array) ->
  List = lists:map (
    fun ({Key, Value}) when is_atom (Key) ->
        string:join ([erlang:atom_to_list (Key), Value], "=") 
    end, Array),
  string:join (List, ",").

list_to_transfer_files (List) ->
  list_to_transfer_files (List, []).
%list_to_transfer_files ([input  | Tail], TF) -> list_to_transfer_files (Tail, ["i" | TF]);
list_to_transfer_files ([input  | Tail], TF) -> list_to_transfer_files (Tail, TF);
list_to_transfer_files ([error  | Tail], TF) -> list_to_transfer_files (Tail, ["e" | TF]);
list_to_transfer_files ([output | Tail], TF) -> list_to_transfer_files (Tail, ["o" | TF]);
list_to_transfer_files ([], TF) ->
  TF.

sync_jobs_to_list (Port, Jobs) ->
  sync_jobs_to_list (Port, Jobs, []).
sync_jobs_to_list (Port, [all | Tail], List) -> 
  sync_jobs_to_list (Port, Tail, [drmaa:control (Port, ?CMD_JOB_IDS_SESSION_ALL) | List]);
sync_jobs_to_list (Port, [H   | Tail], List) -> 
  sync_jobs_to_list (Port, Tail, [H | List]);
sync_jobs_to_list (_Port, [], List) ->
  List.

test (Port) ->
  port_control (Port, 1, <<"xxx">>),
  port_control (Port, 2, <<"xxx">>),
  port_control (Port, 3, <<"xxx">>),
  port_control (Port, 4, <<"xxx">>).
