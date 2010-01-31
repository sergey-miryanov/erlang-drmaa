%%% -------------------------------------------------------------------
%%% \file drmaa.erl
%%% @author Sergey Miryanov (sergey.miryanov@gmail.com)
%%% @copyright 31 Jan 2010 by Sergey Miryanov
%%% @version 0.4
%%% @doc Erlang binding for DRMAA C interface
%%% @end
%%% -------------------------------------------------------------------
-module (drmaa).
-author ('sergey.miryanov@gmail.com').

-behaviour (gen_server).

%% API
-export ([allocate_job_template/0, delete_job_template/0]).
-export ([run_job/0, run_jobs/3, run_jobs/1]).
-export ([wait/2, synchronize/2]).
-export ([control/2]).
-export ([job_status/1]).
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
-export ([timeout/1]).
-export ([control_tag/1]).

%% gen_server callbacks
-export ([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

%% Internal
-export ([start_link/0]).
-export ([control_drv/2, control_drv/3]).
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
-define ('CMD_TIMEOUT_FOREVER',         35).
-define ('CMD_TIMEOUT_NO_WAIT',         36).
-define ('CMD_CONTROL_SUSPEND',         37).
-define ('CMD_CONTROL_RESUME',          38).
-define ('CMD_CONTROL_HOLD',            39).
-define ('CMD_CONTROL_RELEASE',         40).
-define ('CMD_CONTROL_TERMINATE',       41).

-record (state, {port, ops = []}).

%% --------------------------------------------------------------------
%% @spec start_link () -> {ok, Pid} | ignore | {error, Error}
%% @doc Starts DRMAA session
%% @end
%% --------------------------------------------------------------------
-type (result () :: {'ok', pid ()} | 'ignore' | {'error', any ()}).
-spec (start_link/0::() -> result ()).
start_link () ->
  gen_server:start_link ({local, drmaa}, ?MODULE, [], []).

%% API %%
%% --------------------------------------------------------------------
%% @spec allocate_job_template () -> {ok}
%% @doc
%%   It is a wrapper for drmaa_allocate_job_template.
%% 
%%   Allocates a new job template and stores it in current session. This 
%%   template is used to describe the job to be submitted. This 
%%   description is accomplished by setting the desired scalar and vector 
%%   attributes to their appropriate values. This template is then used 
%%   in the job submission process.
%% @end
%% --------------------------------------------------------------------
-spec (allocate_job_template/0::() -> {'ok'}).
allocate_job_template () ->
  gen_server:call (drmaa, {allocate_job_template}).

%% --------------------------------------------------------------------
%% @spec delete_job_template () -> {ok}
%% @doc
%%   It is a wrapper for drmaa_delete_job_template.
%% 
%%   Frees the job template which stored in current session.
%% @end
%% --------------------------------------------------------------------
-spec (delete_job_template/0::() -> {'ok'}).
delete_job_template () ->
  gen_server:call (drmaa, {delete_job_template}).

%% --------------------------------------------------------------------
%% @spec run_job () -> {ok, JobID} | {error, Error}
%%        JobID = string ()
%%        Error = string ()
%% @doc
%%   It is a wrapper for drmaa_run_job.
%% 
%%   Submits a single job with the attributes defined in the job 
%%   template stored in current session. On success returns job 
%%   identifier.
%% @end
%% --------------------------------------------------------------------
-type (job_id () :: string ()).
-type (error () :: string ()).
-spec (run_job/0::() -> {'ok', job_id ()} | {'error', error ()}).
run_job () ->
  gen_server:call (drmaa, {run_job}).

%% --------------------------------------------------------------------
%% @spec run_jobs (Start, End, Incr) -> {ok, JobIDs} | {error, Error}
%%        Start = integer ()
%%        End = integer ()
%%        Incr = integer ()
%%        JobIDs = [string ()]
%%        Error = string ()
%% @doc
%%   It is a wrapper for drmaa_run_bulk_jobs.
%%
%%   Submits a set of parametric jobs which can be run concurrently. The 
%%   attributes defined in the job template are used for every parametric 
%%   job in the set. Each job in the set is identical except for it's 
%%   index. 
%%   The first parametric job has an index equal to Start. The next job 
%%   has an index equal to Start + Incr, and so on. The last job has an 
%%   index equal to Start + n * Incr, where n is equal to 
%%   (End – Start) / Incr. Note that the value of the last job's index 
%%   may not be equal to End if the difference between Start and End is 
%%   not evenly divisble by Incr. 
%%   The smallest valid value for Start is 1. The largest valid value 
%%   for End is 2147483647 (2^31-1). The Start value must be less than 
%%   or equal to the End value, and only positive index numbers are 
%%   allowed. On success, returns a list containing job identifiers for 
%%   all submitted jobs.
%% @end
%% --------------------------------------------------------------------
-type (job_ids () :: [string ()]).
-spec (run_jobs/3:: (integer (), integer (), integer ()) -> 
    {'ok', job_ids ()} | {'error', error ()}).
run_jobs (Start, End, Incr) when is_integer (Start) 
                             and is_integer (End)
                             and is_integer (Incr) 
                             and (Start > 0) 
                             and (Start =< End) 
                             and (End =< 2147483647) ->
  gen_server:call (drmaa, {run_jobs, Start, End, Incr}).

%% --------------------------------------------------------------------
%% @spec run_jobs (Count :: integer ()) -> {ok, JobIDs} | {error, Error}
%%        JobIDs = [string ()]
%%        Error = string ()
%% @doc
%%  It is a shortcut for run_jobs (1, Count, 1).
%% 
%% @see run_jobs/3. <b>run_jobs</b>
%% @end
%% --------------------------------------------------------------------
-spec (run_jobs/1:: (integer ()) -> {'ok', job_ids ()} | {'error', error ()}).
run_jobs (Count) when is_integer (Count) and (Count > 0) ->
  gen_server:call (drmaa, {run_jobs, 1, Count, 1}).

%% --------------------------------------------------------------------
%% @spec wait (JobID, Timeout) -> {ok, Result} | {error, Error}
%%        JobID = string () | any
%%        Timeout = integer () | infinity | no_wait
%%        Result = {ok, {job_id, string ()}, {exit, string ()}, 
%%                  {exit_status, string ()}, {usage, [string ()]}}
%%        Error = string ()
%% @doc
%%   It is a wrapper for drmaa_wait.
%% 
%%   Waits for a job identified by JobID to finish execution or fail. 
%%   If atom 'any' is provided as the JobID, this function will wait 
%%   for any job from the session to finish execution or fail. In this 
%%   case, any job for which exit status information is available will 
%%   satisfy the requirement, including jobs which preivously finished 
%%   but have never been the subject of a drmaa_wait() call. 
%%
%%   The Timeout parameter value indicates how many seconds to remain 
%%   blocked in this call waiting for a result, before returning with 
%%   an error. The atom 'infinity' may be specified to wait indefinitely 
%%   for a result. The 'no_wait' may be specified to return immediately 
%%   with an error if no result is available. 
%%   
%%   On success, returns job identifier, exit type, exit type and list
%%   of strings that describe the amount of resources consumed by the 
%%   job and are DRMAA C implementation defined. 
%%   
%%   Function reaps job data records on a successful call, so any 
%%   subsequent calls to function will fail, returning an error, meaning 
%%   that the job's data record has already been reaped. 
%%
%%   If function exists due to a timeout no rusage information is reaped. 
%%   (The only case where function can be successfully called on a single 
%%   job more than once).
%%   
%%   Exit and ExitStatus results filled by calling to drmaa_wifexited(), 
%%   drmaa_wexitstatus(), drmaa_wifsignaled(), drmaa_wtermsig() and 
%%   drmaa_wifaborted() functions.
%% @end
%% --------------------------------------------------------------------
-type (drmaa_timeout () :: 'infinity' | 'no_wait' | integer ()).
-type (wait_result () :: {'ok', {'job_id', string ()}, 
    {'exit', string ()}, {'exit_status', string ()},
    {'usage', [string ()]}}).
-spec (wait/2:: (string () | 'any', drmaa_timeout ()) -> 
    {ok, wait_result ()} | {error, string ()}).
wait (JobID, infinity) -> wait (JobID, timeout (forever), infinity);
wait (JobID, no_wait)  -> wait (JobID, timeout (no_wait), 1000);
wait (JobID, Timeout)  -> wait (JobID, Timeout, Timeout * 2).

wait (any, Timeout, CallTimeout) when is_integer (Timeout) -> 
  gen_server:call (drmaa, {wait, job_ids (any), Timeout}, CallTimeout);
wait (JobID, Timeout, CallTimeout) when is_list (JobID) and is_integer (Timeout) ->
  gen_server:call (drmaa, {wait, JobID, Timeout}, CallTimeout).

%% --------------------------------------------------------------------
%% @spec synchronize (JobIDs, Timeout) -> {ok, integer ()} | {error, Error}
%%        JobIDs = [string () | all]
%%        Timeout = infinity | no_wait | integer ()
%%        Error = string ()
%% @doc
%%   It is a wrapper for drmaa_synchronize.
%% 
%%   Function waits until all jobs specified by JobIDs have finished 
%%   execution. If JobIDs contains an atom 'all', then this function 
%%   shall wait for all jobs submitted during current session. 
%%
%%   To avoid thread race conditions in multithreaded applications, 
%%   user should explicitly synchronize this call with any other job 
%%   submission calls or control calls that may change the number of 
%%   remote jobs.
%%
%%   The Timeout parameter value indicates how many seconds to remain 
%%   blocked in this call waiting for results to become available, 
%%   before returning with an timeout error. 
%%   The atom 'infinity' may be specified to wait indefinitely for
%%   a result. The atom 'no_wait' may be specified to return 
%%   immediately with an timeout error if no result is available. 
%%
%%   Function lefts job's data records for future access via the 
%%   wait call (@see wait/2. <b>wait</b>).
%% @end
%% --------------------------------------------------------------------
-spec (synchronize/2::([string () | 'all'], drmaa_timeout ()) -> 
    {'ok', integer ()} | {'error', string ()}).
synchronize (Jobs, infinity) when is_list (Jobs) ->
  gen_server:call (drmaa, {sync, Jobs, timeout (forever)}, infinity);
synchronize (Jobs, no_wait) when is_list (Jobs) ->
  gen_server:call (drmaa, {sync, Jobs, timeout (no_wait)}, 1000);
synchronize (Jobs, Timeout) when is_list (Jobs) and is_integer (Timeout) ->
  gen_server:call (drmaa, {sync, Jobs, Timeout}, Timeout * 2).

%% --------------------------------------------------------------------
%% @spec control (JobID, Action) -> {ok} | {error, string ()}
%%        JobID = string () | all
%%        Action = suspend | resume | hold | release | terminate
%% @doc
%%   It is a wrapper for drmaa_control.
%%   
%%   Enacts the action indicated by Action on the job specified by the
%%   job identifier, JobID. The Action parameter's value may be on the 
%%   following: suspend, resume, hold, release, terminate (all values
%%   mapped to DRMAA C implementation one-by-one).
%%   
%%   If JobID is an 'all' atom this function performs the specified 
%%   action on all jobs submitted during current session.
%% @end
%% --------------------------------------------------------------------
-type (control_action () :: 'suspend' | 'resume' | 'hold' | 
  'release' | 'terminate').
-spec (control/2::(string () | 'all', control_action ()) ->
    {'ok'} | {'error', string ()}).
control (all, Action) -> 
  control_inner (job_ids (all), Action);
control (JobID, Action) -> 
  control_inner (JobID, Action).

control_inner (JobID, suspend) -> 
  gen_server:call (drmaa, {control, control_tag (suspend),   JobID});
control_inner (JobID, resume) -> 
  gen_server:call (drmaa, {control, control_tag (resume),    JobID});
control_inner (JobID, hold) -> 
  gen_server:call (drmaa, {control, control_tag (hold),      JobID});
control_inner (JobID, release) -> 
  gen_server:call (drmaa, {control, control_tag (release),   JobID});
control_inner (JobID, terminate) -> 
  gen_server:call (drmaa, {control, control_tag (terminate), JobID}). 

%% --------------------------------------------------------------------
%% @spec job_status (string ()) -> {ok, string ()} | {error, string ()}
%% @doc
%%   It is a wrapper for drmaa_job_ps.
%% 
%%   Functions returns the program status of the job identified by JobID.
%%   The possible values of a program's status are:
%%    - Job status cannot be determined
%%    - Job is queued and active
%%    - Job has been placed in a hold state by the system or administrator
%%    - Job has been placed in a hold state by the user
%%    - Job has been placed in a hold state by the system or administrator and the user
%%    - Job is running
%%    - Job has been placed in a suspend state by the system or administrator
%%    - Job has been placed in a suspend state by the user
%%    - Job has been placed in a suspend state by the system or adminisrator and the user
%%    - Job has successfully completed
%%    - Job has terminated execution abnormally
%%    - Unknown job status
%% @end
%% --------------------------------------------------------------------
-spec (job_status/1::(string()) -> {'ok', string ()} | {'error', string ()}).
job_status (JobID) ->
  gen_server:call (drmaa, {job_status, JobID}).

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

timeout (forever) ->
  gen_server:call (drmaa, {timeout, forever});
timeout (no_wait) ->
  gen_server:call (drmaa, {timeout, no_wait}).

control_tag (suspend) -> 
  gen_server:call (drmaa, {control_tag, suspend});
control_tag (resume) -> 
  gen_server:call (drmaa, {control_tag, resume});
control_tag (hold) -> 
  gen_server:call (drmaa, {control_tag, hold});
control_tag (release) -> 
  gen_server:call (drmaa, {control_tag, release});
control_tag (terminate) -> 
  gen_server:call (drmaa, {control_tag, terminate}).

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
  Reply = drmaa:control_drv (Port, ?CMD_ALLOCATE_JOB_TEMPLATE),
  {reply, Reply, State};
handle_call ({delete_job_template}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_DELETE_JOB_TEMPLATE),
  {reply, Reply, State};
handle_call ({run_job}, _From, #state {port = Port} = State) ->
  {ok, JobID} = drmaa:control_drv (Port, ?CMD_RUN_JOB),
  {reply, {ok, JobID}, State};
handle_call ({wait, JobID, Timeout}, _From, #state {port = Port} = State) ->
  Args = string:join ([erlang:integer_to_list (Timeout) | [JobID]], ","),
  Reply = drmaa:control_drv (Port, ?CMD_WAIT, erlang:list_to_binary (Args)),
  {reply, Reply, State};
handle_call ({sync, Jobs, Timeout}, _From, #state { port = Port} = State) ->
  Len = length (Jobs),
  List = [erlang:integer_to_list (Len) | sync_jobs_to_list (Port, Jobs)],
  Args = string:join ([erlang:integer_to_list (Timeout) | List], ","),
  Reply = drmaa:control_drv (Port, ?CMD_SYNCHRONIZE, erlang:list_to_binary (Args)),
  {reply, Reply, State};
handle_call ({control, Action, JobID}, _From, #state {port = Port} = State) ->
  Reply = control_impl (Port, Action, JobID),
  {reply, Reply, State};
handle_call ({job_status, JobID}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_JOB_PS, erlang:list_to_binary (JobID)),
  {reply, Reply, State};
handle_call ({join_files, Join}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_JOIN_FILES, erlang:list_to_binary (Join)),
  {reply, Reply, State};
handle_call ({remote_command, Command}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_REMOTE_COMMAND, erlang:list_to_binary (Command)),
  {reply, Reply, State};
handle_call ({args, Argv}, _From, #state {port = Port} = State) ->
  Len = length (Argv),
  List = [erlang:integer_to_list (Len) | Argv],
  Args = string:join (List, ","),
  Reply = drmaa:control_drv (Port, ?CMD_V_ARGV, erlang:list_to_binary (lists:flatten (Args))),
  {reply, Reply, State};
handle_call ({env, Env}, _From, #state {port = Port} = State) ->
  Buffer = pair_array_to_vector (Env),
  Len = length (Env),
  List = [erlang:integer_to_list (Len) | Buffer],
  Args = string:join (List, ","),
  Reply = drmaa:control_drv (Port, ?CMD_V_ENV, erlang:list_to_binary (Args)),
  {reply, Reply, State};
handle_call ({emails, Emails}, _From, #state {port = Port} = State) ->
  Len = length (Emails),
  List = [erlang:integer_to_list (Len) | Emails],
  Args = string:join (List, ","),
  Reply = drmaa:control_drv (Port, ?CMD_V_EMAIL, erlang:list_to_binary (Args)),
  {reply, Reply, State};
handle_call ({job_state, JobState}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_JS_STATE, erlang:list_to_binary (JobState)),
  {reply, Reply, State};
handle_call ({wd, WorkingDir}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_WD, erlang:list_to_binary (WorkingDir)),
  {reply, Reply, State};
handle_call ({input_path, InputPath}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_INPUT_PATH, erlang:list_to_binary (InputPath)),
  {reply, Reply, State};
handle_call ({output_path, OutputPath}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_OUTPUT_PATH, erlang:list_to_binary (OutputPath)),
  {reply, Reply, State};
handle_call ({error_path, ErrorPath}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_ERROR_PATH, erlang:list_to_binary (ErrorPath)),
  {reply, Reply, State};
handle_call ({block_email, BlockEmail}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_BLOCK_EMAIL, erlang:list_to_binary (BlockEmail)),
  {reply, Reply, State};
handle_call ({transfer_files, List}, _From, #state {port = Port} = State) ->
  TF = list_to_transfer_files (List),
  Reply = drmaa:control_drv (Port, ?CMD_TRANSFER_FILES, erlang:list_to_binary (TF)),
  {reply, Reply, State};
handle_call ({job_name, JobName}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_JOB_NAME, erlang:list_to_binary (JobName)),
  {reply, Reply, State};
handle_call ({run_jobs, Start, End, Incr}, _From, #state {port = Port} = State) ->
  List = [erlang:integer_to_list (Start), erlang:integer_to_list (End), erlang:integer_to_list (Incr)],
  Buffer = erlang:list_to_binary (string:join (List, ",")),
  Reply = drmaa:control_drv (Port, ?CMD_RUN_BULK_JOBS, Buffer),
  {reply, Reply, State};
handle_call ({placeholder_hd}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_PLACEHOLDER_HD),
  {reply, Reply, State};
handle_call ({placeholder_wd}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_PLACEHOLDER_WD),
  {reply, Reply, State};
handle_call ({placeholder_incr}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_PLACEHOLDER_INCR),
  {reply, Reply, State};
handle_call ({job_ids_all}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_JOB_IDS_SESSION_ALL),
  {reply, Reply, State};
handle_call ({job_ids_any}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_JOB_IDS_SESSION_ANY),
  {reply, Reply, State};
handle_call ({timeout, forever}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_TIMEOUT_FOREVER),
  {reply, Reply, State};
handle_call ({timeout, no_wait}, _From, #state {port = Port} = State) ->
  Reply = drmaa:control_drv (Port, ?CMD_TIMEOUT_NO_WAIT),
  {reply, Reply, State};
handle_call ({control_tag, Tag}, _From, #state {port = Port} = State) ->
  Reply = control_tag_impl (Port, Tag),
  {reply, Reply, State};
handle_call (Request, _From, State) ->
  {reply, {unknown, Request}, State}.

control_tag_impl (Port, suspend)   -> drmaa:control_drv (Port, ?CMD_CONTROL_SUSPEND);
control_tag_impl (Port, resume)    -> drmaa:control_drv (Port, ?CMD_CONTROL_RESUME);
control_tag_impl (Port, hold)      -> drmaa:control_drv (Port, ?CMD_CONTROL_HOLD);
control_tag_impl (Port, release)   -> drmaa:control_drv (Port, ?CMD_CONTROL_RELEASE);
control_tag_impl (Port, terminate) -> drmaa:control_drv (Port, ?CMD_CONTROL_TERMINATE).

control_impl (Port, Action, JobID) ->
  Args = string:join ([erlang:integer_to_list (Action) | [JobID]], ","),
  drmaa:control_drv (Port, ?CMD_CONTROL, erlang:list_to_binary (Args)).

control_drv (Port, Command) when is_port (Port) and is_integer (Command) ->
  port_control (Port, Command, <<"_">>),
  wait_result (Port).

control_drv (Port, Command, Data) 
  when is_port (Port) and is_integer (Command) and is_binary (Data) ->
    port_control (Port, Command, Data),
    wait_result (Port);
control_drv (Port, Command, <<>>)
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
  sync_jobs_to_list (Port, Tail, [drmaa:control_drv (Port, ?CMD_JOB_IDS_SESSION_ALL) | List]);
sync_jobs_to_list (Port, [H   | Tail], List) -> 
  sync_jobs_to_list (Port, Tail, [H | List]);
sync_jobs_to_list (_Port, [], List) ->
  List.

