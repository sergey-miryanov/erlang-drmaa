%%% -------------------------------------------------------------------
%%% @author Sergey Miryanov (sergey.miryanov@gmail.com)
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
%%   Wrapper for drmaa_allocate_job_template.
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
%%   Wrapper for drmaa_delete_job_template.
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
%%   Wrapper for drmaa_run_job.
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
%%   Wrapper for drmaa_run_bulk_jobs.
%%
%%   Submits a set of parametric jobs which can be run concurrently. The 
%%   attributes defined in the job template are used for every parametric 
%%   job in the set. Each job in the set is identical except for it's 
%%   index. 
%%
%%   The first parametric job has an index equal to Start. The next job 
%%   has an index equal to Start + Incr, and so on. The last job has an 
%%   index equal to Start + n * Incr, where n is equal to 
%%   (End – Start) / Incr. Note that the value of the last job's index 
%%   may not be equal to End if the difference between Start and End is 
%%   not evenly divisble by Incr. 
%%
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
%%  Shortcut for run_jobs (1, Count, 1).
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
%%   Wrapper for drmaa_wait.
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
%%   Wrapper for drmaa_synchronize.
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
%%   drmaa:wait call.
%% @see wait/2. <b>drmaa:wait</b>
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
%%   Wrapper for drmaa_control.
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
%%   Wrapper for drmaa_job_ps.
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

%% --------------------------------------------------------------------
%% @spec join_files (true | false) -> {ok} | {error, string ()}
%% @doc
%%   Sets drmaa_join_files attribute.
%%
%%   The drmaa_join_files attribute specifies whether the job's error 
%%   stream should be intermixed with the job's output stream. If not 
%%   explicitly set in the job template, the attribute's value defaults 
%%   to 'false'.
%%
%%   Either 'true' or 'false' can be specified as valid attribute values. 
%%   If 'true' is specified as the attribute value, the DRMAA 
%%   implementation will ignore the value of the drmaa:error_path
%%   and intermix the standard error stream with the standard output 
%%   stream at the location specified by the drmaa::output_path.
%%
%% @see error_path/1. <b>drmaa:error_path</b>
%% @see output_path/1. <b>drmaa:output_path</b>
%% @end
%% --------------------------------------------------------------------
-spec (join_files/1::('true' | 'false') -> {'ok'} | {'error', string ()}).
join_files (true) ->
  gen_server:call (drmaa, {join_files, "y"});
join_files (false) ->
  gen_server:call (drmaa, {join_files, "n"}).

%% --------------------------------------------------------------------
%% @spec remote_command (string ()) -> {ok} | {error, string ()}
%% @doc
%%   Sets drmaa_remote_command attribute.
%%
%%   The drmaa_remote_command attribute specifies the remote command to 
%%   execute. The drmaa_remote_command must be the path of an executable 
%%   that is available at the job's execution host. If the path is 
%%   relative, it is assumed to be relative to the working directory, 
%%   usually set through the drmaa:working_dir. If the working directory 
%%   is not set, the path is interpreted in an DRMAA C interface 
%%   implementation-specific manner. In any case, no binary file 
%%   management is done.
%% @see working_dir/1. <b>drmaa:working_dir</b>
%% @end
%% --------------------------------------------------------------------
-spec (remote_command/1::(string ()) -> {'ok'} | {'error', string ()}).
remote_command (Command) when is_list (Command) ->
  gen_server:call (drmaa, {remote_command, Command}).

%% --------------------------------------------------------------------
%% @spec args ([string ()]) -> {ok} | {error, string ()}
%% @doc
%%   Sets drmaa_v_argv attribute vector.
%%
%%   The drmaa_v_argv attribute specifies the array of string values 
%%   which will be passed as arguments to the job.
%% @end
%% --------------------------------------------------------------------
-spec (args/1::([string ()]) -> {'ok'} | {'error', string ()}).
args (Argv) when is_list (Argv) ->
  gen_server:call (drmaa, {args, Argv}).

%% --------------------------------------------------------------------
%% @spec env (Env) -> {ok} | {error, string ()}
%%        Env = [{atom (), string ()}]
%% @doc
%%   Sets drmaa_v_env attribute vector.
%%
%%   The drmaa_v_env attribute specifies the environment variable 
%%   settings for the job to be submitted. 
%% @end
%% --------------------------------------------------------------------
-type (env_list () :: [{atom (), string ()}]).
-spec (env/1::(env_list ()) -> {'ok'} | {'error', string ()}).
env (Env) when is_list (Env) ->
  gen_server:call (drmaa, {env, Env}).

%% --------------------------------------------------------------------
%% @spec emails ([string ()]) -> {ok} | {error, string ()}
%% @doc
%%   Sets drmaa_v_email attribute vector.
%%
%%   The drmaa_v_email attribute specifies the list of e-mail addresses 
%%   to which job completion and status reports are to be sent. Which 
%%   reports are sent and when they are sent are determined by the DRMS 
%%   configuration and the drmaa:block_email.
%% @see block_email/1. <b>drmaa:block_email</b>
%% @end
%% --------------------------------------------------------------------
-spec (emails/1::([string ()]) -> {'ok'} | {'error', string ()}).
emails (Emails) when is_list (Emails) ->
  gen_server:call (drmaa, {emails, Emails}).

%% --------------------------------------------------------------------
%% @spec job_state (active | hold) -> {ok} | {error, string ()}
%% @doc
%%   Sets drmaa_js_state attribute.
%%
%%   The drmaa_js_state attribute specifies the job's state at submission. 
%% 
%%   When 'active' is used, the job will be submitted in a runnable state. <br/>
%%   When 'hold' is used, the job will be submitted in a user hold state 
%%   (either DRMAA_PS_USER_ON_HOLD or DRMAA_PS_USER_SYSTEM_ON_HOLD).
%% @end
%% --------------------------------------------------------------------
-spec (job_state/1::('active' | 'hold') -> {'ok'} | {'error', string ()}).
job_state (active) ->
  gen_server:call (drmaa, {job_state, "drmaa_active"});
job_state (hold) ->
  gen_server:call (drmaa, {job_state, "drmaa_hold"}).

%% --------------------------------------------------------------------
%% @spec working_dir (string ()) -> {ok} | {error, string ()}
%% @doc
%%   Sets drmaa_wd attribute.
%%
%%   The drmaa_wd attribute specifies the directory name where the job 
%%   will be executed. A drmaa:placeholder (hd) at the beginning of the 
%%   drmaa:wd value denotes the remaining string portion as a relative 
%%   directory name which is resolved relative to the job user's home
%%   directory at the execution host. When the DRMAA job template is 
%%   used for bulk job submission drmaa:placeholder (incr) can be used 
%%   at any position within the drmaa:wd value to cause a substitution 
%%   with the parametric job's index.
%%
%%   The drmaa:wd value must be specified in a syntax that is common at 
%%   the host where the job is executed. If set to a relative path and 
%%   no placeholder is used, the path is interpreted in a DRMAA C 
%%   interface implementation-specific manner. If not set, the working 
%%   directory will be set in a DRMAA C interface implementation-
%%   specific manner. If set and the given directory does not exist, 
%%   the job will enter the DRMAA_PS_FAILED state when run.
%% @see placeholder/1. <b>drmaa:placeholder</b>
%% @end
%% --------------------------------------------------------------------
-spec (working_dir/1::(string ()) -> {'ok'} | {'error', string ()}).
working_dir (Dir) when is_list (Dir) ->
  gen_server:call (drmaa, {wd, Dir}).

%% --------------------------------------------------------------------
%% @spec job_name (string ()) -> {ok} | {error, string ()}
%% @doc
%%   Sets drmaa_job_name attribute.
%%
%%   The drmaa_job_name attribute specifies the job's name. A job name 
%%   shall contain only alpha-numeric and '_' characters.
%% @end
%% --------------------------------------------------------------------
-spec (job_name/1::(string ()) -> {'ok'} | {'error', string ()}).
job_name (JobName) when is_list (JobName) ->
  gen_server:call (drmaa, {job_name, JobName}).

%% --------------------------------------------------------------------
%% @spec input_path (string ()) -> {error, not_supported}
%% @doc
%%   Sets drmaa_input_path attribute. 
%%
%%   Not supported now.
%%
%%   The drmaa_input_path attribute specifies the standard input path of 
%%   the job. If set, this attribute's value specifies the network path 
%%   of the job's input stream file. The value of the drmaa_input_path
%%   attribute must be of the form:<br/>
%%      [hostname]:file_path
%%
%%   When the drmaa:transfer_files attribute is supported and contains 
%%   the atom 'input', the input file will be fetched by the DRMS from 
%%   the specified host or from the submit host if no hostname is 
%%   specified in the drmaa_input_path attribute value. When the 
%%   drmaa:transfer_files attribute is not supported or does not contain 
%%   the atom 'input', the input file is always expected at the host 
%%   where the job is executed, regardless of any hostname specified in 
%%   the drmaa_input_path attribute value.
%%
%%   If the DRMAA job template will be used for bulk job submission the
%%   drmaa:placeholder (incr) can be used at any position within the 
%%   drmaa_input_path attribute value to cause a substitution with the 
%%   parametric job's index. A drmaa:placeholder (hd) at the beginning 
%%   of the drmaa_input_path attribute value denotes the remaining 
%%   portion of the drmaa_input_path attribute value as a relative file 
%%   specification resolved relative to the job submitter's home 
%%   directory at the host where the file is located. A drmaa:placeholder (wd)
%%   at the beginning of the drmaa_input_path attribute value denotes 
%%   the remaining portion of the drmaa_input_path attribute value as 
%%   a relative file specification resolved relative to the job's
%%   working directory at the host where the file is located. 
%%
%%   The drmaa_input_path attribute value must be specified in a syntax 
%%   that is common at the host where the file is located. If set and 
%%   the file can't be read the job enters the state DRMAA_PS_FAILED 
%%   upon submission.
%%
%% @see transfer_files/1. <b>drmaa:transfer_files</b>
%% @see placeholder/1. <b>drmaa:placeholder</b>
%% @end
%% --------------------------------------------------------------------
-spec (input_path/1::(string ()) -> {'error', 'not_supported'}).
input_path (InputPath) when is_list (InputPath) ->
  {error, not_supported}.
  %gen_server:call (drmaa, {input_path, InputPath}).

%% --------------------------------------------------------------------
%% @spec output_path (string ()) -> {ok} | {error, string ()}
%% @doc
%%   Sets drmaa_output_path attribute.
%%
%%   The drmaa_output_path attribute specifies the standard output path 
%%   of the job. If set, this attribute's value specifies the network 
%%   path of the job's output stream file. The value of the attribute 
%%   must be of the form: <br/>
%%      [hostname]:file_path
%%
%%   When the drmaa:transfer_files attribute is supported and contains 
%%   the atom, 'output', the output file shall be transferred by the DRMS 
%%   to the specified host or to the submit host if no hostname is 
%%   specified in the drmaa_output_path attribute value. When the 
%%   drmaa:transfer_files attribute is not supported or does not contain 
%%   the atom, 'output', the output file is always kept at the host
%%   where the job is executed, regardless of any hostname specified in 
%%   the drmaa_output_path attribute value.
%%
%%   If the DRMAA job template will be used for bulk job submission the
%%   drmaa:placeholder (incr) can be used at any position within the 
%%   drmaa_output_path attribute value to cause a substitution with the 
%%   parametric job's index. A drmaa:placeholder (hd) at the beginning 
%%   of the drmaa_output_path attribute value denotes the remaining
%%   portion of the drmaa_output_path attribute value as a relative file 
%%   specification resolved relative to the job submitter's home 
%%   directory at the host where the file is located. 
%%   A drmaa:placeholder (wd) at the beginning of the drmaa_output_path 
%%   attribute value denotes the remaining portion of the the 
%%   drmaa_output_path attribute value as a relative file specification 
%%   resolved relative to the job's working directory at the host where 
%%   the file is located. 
%%
%%   The drmaa_output_path attribute value must be specified in a syntax 
%%   that is common at the host where the file is located.
%%
%%   If set and the file can't be written before execution the job 
%%   enters the state DRMAA_PS_FAILED upon submission.
%%
%% @see transfer_files/1. <b>drmaa:transfer_files</b>
%% @see placeholder/1. <b>drmaa:placeholder</b>
%% @end
%% --------------------------------------------------------------------
-spec (output_path/1::(string ()) -> {'ok'} | {'error', string ()}).
output_path (OutputPath) when is_list (OutputPath) ->
  gen_server:call (drmaa, {output_path, OutputPath}).

%% --------------------------------------------------------------------
%% @spec error_path (string ()) -> {ok} | {error, string ()}
%% @doc
%%   Sets drmaa_error_path attribute.
%%
%%   The drmaa_error_path attribute specifies the standard error path 
%%   of the job. If set, this attribute's value specifies the network 
%%   path of the job's error stream file. The value of the 
%%   drmaa_error_path attribute must be of the form: <br/>
%%      [hostname]:file_path
%%
%%   When the drmaa:transfer_files attribute is supported and contains 
%%   the atom, 'error', the output file shall be transferred by the DRMS 
%%   to the specified host or to the submit host if no hostname is 
%%   specified in the drmaa_error_path attribute value. When the 
%%   drmaa:transfer_files attribute is not supported or does not contain 
%%   the atom, 'error', the output file is always kept at the host where 
%%   the job is executed, regardless of any hostname specified in the 
%%   drmaa_error_path attribute value.
%%
%%   If the DRMAA job template will be used for bulk job submission the
%%   drmaa:placeholder (incr) can be used at any position within the 
%%   drmaa_error_path attribute value to cause a substitution with the 
%%   parametric job's index. A drmaa:placeholder (hd) at the beginning 
%%   of the drmaa_error_path attribute value denotes the remaining portion 
%%   of the drmaa_error_path attribute value as a relative file 
%%   specification resolved relative to the job submitter's home directory 
%%   at the host where the file is located. A drmaa:placeholder (wd) at 
%%   the beginning of the drmaa_error_path attribute value denotes the 
%%   remaining portion of the the drmaa_error_path attribute value as a 
%%   relative file specification resolved relative to the job's working 
%%   directory at the host where the file is located. 
%%
%%   The drmaa_error_path attribute value must be specified in a syntax 
%%   that is common at the host where the file is located. If set and the 
%%   file can't be written before execution the job enters the state 
%%   DRMAA_PS_FAILED upon submission.
%%
%% @see transfer_files/1. <b>drmaa:transfer_files</b>
%% @see placeholder/1. <b>drmaa:placeholder</b>
%% @end
%% --------------------------------------------------------------------
-spec (error_path/1::(string ()) -> {'ok'} | {'error', string ()}).
error_path (ErrorPath) when is_list (ErrorPath) ->
  gen_server:call (drmaa, {error_path, ErrorPath}).

%% --------------------------------------------------------------------
%% @spec job_category (string ()) -> {error, not_supported}
%% @doc
%%   Sets drmaa_job_category attribute.
%%
%%   Not supported now.
%%
%%   The drmaa_job_catrgory attribute specifies the DRMAA job category. 
%%   
%%   See section 2.4.1 of the Distributed Resource Management Application 
%%   API Specification 1.0 for more information about DRMAA job categories.
%% @end
%% --------------------------------------------------------------------
-spec (job_category/1::(string ()) -> {'error', 'not_supported'}).
job_category (_JobCategory) ->
  {error, not_supported}.

%% --------------------------------------------------------------------
%% @spec native_spec (string ()) -> {error, not_supported}
%% @doc
%%   Sets drmaa_native_specification attribute.
%%
%%   Not supported now.
%%
%%   The drmaa_native_specification attribute specifies the native 
%%   submission options which shall be passed to the DRMS at job 
%%   submission time. 
%%
%%   See section 2.4.2 of the Distributed Resource Management 
%%   Application API Specification 1.0 for more information about 
%%   DRMAA job categories.
%% @end
%% --------------------------------------------------------------------
-spec (native_spec/1::(string ()) -> {'error', 'not_supported'}).
native_spec (_NativeSpec) ->
  {error, not_supported}.

%% --------------------------------------------------------------------
%% @spec block_email (true | false) -> {ok} | {error, string ()}
%% @doc
%%   Sets drmaa_block_email attribute.
%%
%%   The drmaa_block_email attribute specifies whether the sending of 
%%   email shall blocked or not. If the DRMS configuration or the 
%%   drmaa:native_specification or drmaa:job_category attribute would 
%%   normally cause email to be sent in association with job events, 
%%   the drmaa_block_email attribute value can will override that 
%%   setting, causing no email to be sent.
%%
%%   If the attribute's value is 'false', no email will be sent, 
%%   regardless of DRMS configuration or other attribute values. 
%%   If the attribute's value 'true', the sending of email is unaffected.
%% @see native_spec/1. <b>drmaa:native_spec</b>
%% @see job_category/1. <b>drmaa:job_category</b>
%% @end
%% --------------------------------------------------------------------
-spec (block_email/1::('true' | 'false') -> {'ok'} | {'error', string ()}).
block_email (true) ->
  gen_server:call (drmaa, {block_email, "1"});
block_email (false) ->
  gen_server:call (drmaa, {block_email, "0"}).

%% --------------------------------------------------------------------
%% @spec start_time (string ()) -> {error, not_supported}
%% @doc
%%   Sets drmaa_start_time attribute.
%%
%%   Not supported now.
%%
%%   The drmaa_start_time attribute specifies the earliest point in time 
%%   when the job may be eligible to be run. drmaa_start_time attribute 
%%   value is of the format: <br/>
%%     [[[[CC]YY/]MM/]DD] hh:mm[:ss] [{-|+}UU:uu]<br/>
%%   where
%%    <ul>
%%      <li>CC is the first two digits of the year [19,)</li>
%%      <li>YY is the last two digits of the year [00,99]</li>
%%      <li>MM is the two digits of the month [01,12]</li>
%%      <li>DD is the two digit day of the month [01,31]</li>
%%      <li>hh is the two digit hour of the day [00,23]</li>
%%      <li>mm is the two digit minute of the day [00,59]</li>
%%      <li>ss is the two digit second of the minute [00,61]</li>
%%      <li>UU is the two digit hours since (before) UTC [-11,12]</li>
%%      <li>uu is the two digit minutes since (before) UTC [00,59]</li>
%%    </ul>
%%
%%   If the optional UTC-offset is not specified, the offset associated 
%%   with the local timezone will be used. If any of the other optional 
%%   fields are not specified, the time shall be resolved to the 
%%   soonest time which can be constructed using the values of the 
%%   specified fields, which is in the future at the time of resolution. 
%%   That is to say that if the attribute's value is "10:00", and it is
%%   resolved to a concrete time at 11:01am on November 24th, the time 
%%   will be resolved to 10:00am on November 25th, because that is the 
%%   soonest time which matches the specified fields and is in the future. 
%%   If at 9:34am on December 1st the same time string is resolved 
%%   again (such as by reusing the containing job template for another 
%%   job submission), it will resolve to 10:00am on December 1st.
%% @end
%% --------------------------------------------------------------------
-spec (start_time/1::(string ()) -> {'error', 'not_supported'}).
start_time (_StartTime) ->
  {error, not_supported}.

%% --------------------------------------------------------------------
%% @spec deadline_time (string ()) -> {error, not_supported}
%% @doc
%%   Sets drmaa_deadline_time attribute.
%% 
%%   Not supported now.
%%
%%   The drmaa_start_time attribute specifies a deadline after which 
%%   the DRMS will terminate a job. drmaa_start_time attribute value 
%%   is of the format: <br/>
%%      [[[[CC]YY/]MM/]DD] hh:mm[:ss] [{-|+}UU:uu]<br/>
%%   where
%%    <ul>
%%     <li>CC is the first two digits of the year [19,)</li>
%%     <li>YY is the last two digits of the year [00,99]</li>
%%     <li>MM is the two digits of the month [01,12]</li>
%%     <li>DD is the two digit day of the month [01,31]</li>
%%     <li>hh is the two digit hour of the day [00,23]</li>
%%     <li>mm is the two digit minute of the day [00,59]</li>
%%     <li>ss is the two digit second of the minute [00,61]</li>
%%     <li>UU is the two digit hours since (before) UTC [-11,12]</li>
%%     <li>uu is the two digit minutes since (before) UTC [00,59]</li>
%%    </ul>
%%
%%   If the optional UTC-offset is not specified, the offset 
%%   associated with the local timezone will be used. If any of the 
%%   other optional fields are not specified, the time shall be 
%%   resolved to the soonest time which can be constructed using the 
%%   values of the specified fields, which is in the future at the 
%%   time of resolution. That is to say that if the attribute's value 
%%   is "10:00", and it is resolved to a concrete time at 11:01am on 
%%   November 24th, the time will be resolved to 10:00am on November 
%%   25th, because that is the soonest time which matches the specified 
%%   fields and is in the future. If at 9:34am on December 1st the same 
%%   time string is resolved again (such as by reusing the containing 
%%   job template for another job submission), it will resolve to 
%%   10:00am on December 1st.
%% @end
%% --------------------------------------------------------------------
-spec (deadline_time/1::(string ()) -> {'error', 'not_supported'}).
deadline_time (_DeadlineTime) ->
  {error, not_supported}.

%% --------------------------------------------------------------------
%% @spec hlimit (string ()) -> {error, not_supproted}
%% @doc
%%   Sets drmaa_wct_hlimit attribute.
%%  
%%   Not supported now.
%%
%%   The drmaa_wct_hlimit attribute specifies how much wall clock time 
%%   a job is allowed to consume before its limit has been exceeded. 
%%   The DRMS shall terminate a job that has exceeded its wallclock time 
%%   limit. Suspended time shall also be accumulated here.
%%
%%   This attribute's value must be of the form:<br/>
%%     [[h:]m:]s<br/>
%%   where
%%    <ul>
%%      <li>h is one or more digits representing hours</li>
%%      <li>m is one or more digits representing minutes</li>
%%      <li>s is one or more digits representing seconds</li>
%%    </ul>
%% @end
%% --------------------------------------------------------------------
-spec (hlimit/1::(string ()) -> {'error', 'not_supported'}).
hlimit (_HLimit) ->
  {error, not_supported}.

%% --------------------------------------------------------------------
%% @spec slimit (string ()) -> {error, not_supported}
%% @doc
%%   Sets drmaa_wct_slimit attribute.
%%
%%   Not supported now.
%%
%%   The drmaa_wct_slimit attribute specifies an estimate as to how much 
%%   wall clock time the job will need to complete. Suspended time shall 
%%   also be accumulated here. This attribute is intended to assist the 
%%   scheduler. If the time specified by this attribute's value in 
%%   insufficient, the DRMAA implementation may impose a scheduling 
%%   penalty.
%%
%%   This attribute's value MUST be of the form:<br/>
%%      [[h:]m:]s<br/>
%%   where
%%    <ul>
%%       <li>h is one or more digits representing hours</li>
%%       <li>m is one or more digits representing minutes</li>
%%       <li>s is one or more digits representing seconds</li>
%%    </ul>
%% @end
%% --------------------------------------------------------------------
-spec (slimit/1::(string ()) -> {'error', 'not_supported'}).
slimit (_SLimit) ->
  {error, not_supported}.

%% --------------------------------------------------------------------
%% @spec hlimit_duration (string ()) -> {error, not_supported}
%% @doc
%%   Sets drmaa_duration_hlimit attribute.
%% 
%%   Not supported now.
%%
%%   The drmaa_duration_hlimit attribute specifies how long the job may 
%%   be in a running state before its time limit has been exceeded, 
%%   and therefore is terminated by the DRMS.
%%
%%   This attribute's value MUST be of the form:<br/>
%%      [[h:]m:]s<br/>
%%   where
%%    <ul>
%%       <li>h is one or more digits representing hours</li>
%%       <li>m is one or more digits representing minutes</li>
%%       <li>s is one or more digits representing seconds</li>
%%    </ul>
%% @end
%% --------------------------------------------------------------------
-spec (hlimit_duration/1::(string ()) -> {'error', 'not_supported'}).
hlimit_duration (_Duration) ->
  {error, not_supported}.

%% --------------------------------------------------------------------
%% @spec slimit_duration (string ()) -> {error, not_supported}
%% @doc
%%   Sets drmaa_duration_slimit attribute.
%%
%%   Not supported now.
%%
%%   The drmaa_duration_slimit attribute specifies an estimate as to how 
%%   long the job will need to remain in a running state in order to 
%%   complete. This attribute is intended to assist the scheduler. If the 
%%   time specified by this attribute's value in insufficient, the DRMAA 
%%   implementation may impose a scheduling penalty.
%%
%%   This attribute's value MUST be of the form:<br/>
%%       [[h:]m:]s<br/>
%%   where
%%    <ul>
%%        <li>h is one or more digits representing hours</li>
%%        <li>m is one or more digits representing minutes</li>
%%        <li>s is one or more digits representing seconds</li>
%%    </ul>
%% @end
%% --------------------------------------------------------------------
-spec (slimit_duration/1::(string ()) -> {'error', 'not_supported'}).
slimit_duration (_Duration) ->
  {error, not_supported}.

%% --------------------------------------------------------------------
%% @spec transfer_files ([input | output | error]) -> {ok} | {error, string ()}
%% @doc
%%   Sets drmaa_transfer_files attribute.
%%
%%   The drmaa_transfer_files attribute specifies, which of the standard 
%%   I/O files (stdin, stdout and stderr) are to be transferred to/from 
%%   the execution host. 
%%
%%   The attribute's value may contain any of the atoms, 'error', 'input'
%%   and 'output'. If the atom, 'error', is present, the error stream will 
%%   be transferred. If the atom, 'input', is present, the input stream 
%%   will be transferred. If the atom, 'output', is present, the output 
%%   stream will be transferred. See the drmaa:input_path, drmaa:output_path 
%%   and drmaa:error_path for information about how to specify the standard 
%%   input file, standard output file and standard error file and the 
%%   effects of this attribute's value.
%% @see input_path/1. <b>drmaa:input_path</b>
%% @see output_path/1. <b>drmaa:output_path</b>
%% @see error_path/1. <b>drmaa:error_path</b>
%% @end
%% --------------------------------------------------------------------
-spec (transfer_files/1::(['input' | 'output' | 'error']) -> 
    {'ok'} | {'error', string ()}).
transfer_files (List) when is_list (List) ->
  gen_server:call (drmaa, {transfer_files, List}).

%% --------------------------------------------------------------------
%% @spec placeholder (Type) -> string ()
%%        Type = incr | hd | home | home_dir | wd | working_dir
%% @doc
%%   Returns DRMAA_PLACEHOLDER_[HD | WD | INCR].
%% 
%%   hd, home, home_dir - the DRMAA_PLACEHOLDER_HD directive is used with 
%%   the drmaa:working_dir, drmaa:input_path, drmaa:output_path, and 
%%   drmaa:error_path attributes to represent the user's home directory.
%%
%%   wd, working_dir - the DRMAA_PLACEHOLDER_WD directive is used with 
%%   the drmaa:input_path, drmaa:output_path, and drmaa:error_path 
%%   attributes to represent the job working directory.
%%   
%%   incr - the DRMAA_PLACEHOLDER_INCR directive is used with the 
%%   drmaa:working_dir, drmaa:input_path, drmaa:output_path, and 
%%   drmaa:error_path attributes to represent the individual id of each 
%%   subjob in the parametric job. See drmaa:run_jobs.
%%
%% @see working_dir/1. <b>drmaa:working_dir</b>
%% @see input_path/1. <b>drmaa:input_path</b>
%% @see error_path/1. <b>drmaa:error_path</b>
%% @see output_path/1. <b>drmaa:output_path</b>
%% @see run_jobs/3. <b>drmaa:run_jobs</b>
%% @end
%% --------------------------------------------------------------------
-type (placeholder_type () :: 'incr' | 'hd' | 'home' | 'home_dir' | 'wd'
  | 'working_dir').
-spec (placeholder/1::(placeholder_type ()) -> string ()).
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

%% --------------------------------------------------------------------
%% @spec job_ids (all | any) -> string ()
%% @doc
%%   Returns DRMAA_JOB_IDS_SESSION_[ALL | ANY].
%%
%%   all - The DRMAA_JOB_IDS_SESSION_ALL directive is used to indicate 
%%   to drmaa:control() and drmaa:synchronize() that all jobs currently 
%%   active in the session should be the operation's target.
%%
%%   any - The DRMAA_JOB_IDS_SESSION_ANY directive is used to indicate 
%%   to drmaa_wait() that any job currently active in the session should 
%%   be the operation's target.
%%
%% @see control/2. <b>drmaa:control</b>
%% @see synchronize/2. <b>drmaa:synchronize</b>
%% @see wait/2. <b>drmaa:wait</b>
%% @end
%% --------------------------------------------------------------------
-spec (job_ids/1::('all' | 'any') -> string ()).
job_ids (all) ->
  gen_server:call (drmaa, {job_ids_all});
job_ids (any) ->
  gen_server:call (drmaa, {job_ids_any}).

%% --------------------------------------------------------------------
%% @spec timeout (forever | no_wait) -> int ()
%% @doc
%%   Returns a DRMAA_TIMEOUT_[WAIT_FOREVER | NO_WAIT].
%%
%%   forever - The DRMAA_TIMEOUT_WAIT_FOREVER directive is used as to 
%%   indicate to drmaa:wait() and drmaa:synchronize() that the 
%%   implementation should block indefinitely until the requested job 
%%   exit status information is available.
%%
%%   no_wait - The DRMAA_TIMEOUT_NO_WAIT directive is used as to 
%%   indicate to drmaa:wait() and drmaa:synchronize() that the 
%%   implementation should not block if the requested job exit status
%%   information is not available.
%% @see wait/2. <b>drmaa:wait</b>
%% @see synchronize/2. <b>drmaa:synchronize</b>
%% @end
%% --------------------------------------------------------------------
-spec (timeout/1::('forever' | 'no_wait') -> integer ()).
timeout (forever) ->
  gen_server:call (drmaa, {timeout, forever});
timeout (no_wait) ->
  gen_server:call (drmaa, {timeout, no_wait}).

%% --------------------------------------------------------------------
%% @spec control_tag (Tag) -> integer ()
%%        Tag = suspend | resume | hold | release | terminate
%%
%% @doc
%%   Returns DRMAA_CONTROL_[SUSPEND | RESUME | HOLD | RELEASE | TERMINATE]
%%
%%   suspend - The DRMAA_CONTROL_SUSPEND directive is used to indicate 
%%   to drmaa:control() that the requested job should be placed in a user 
%%   suspend state.
%%
%%   resume - The DRMAA_CONTROL_RESUME directive is used to indicate 
%%   to drmaa:control() that the requested job should be resumed from 
%%   a user suspend state.
%%
%%   hold - The DRMAA_CONTROL_HOLD directive is used to indicate 
%%   to drmaa:control() that the requested job should be placed into a 
%%   user hold state.
%%
%%   release - The DRMAA_CONTROL_RELEASE directive is used to indicate 
%%   to drmaa:control() that the requested job should the released from 
%%   a user hold state.
%%  
%%   terminate - The DRMAA_CONTROL_TERMINATE directive is used to 
%%   indicate to drmaa:control() that the requested job should be 
%%   terminated.
%%
%% @see control/2. <b>drmaa:control</b>
%% @end
%% --------------------------------------------------------------------
-spec (control_tag/1::('suspend' | 'resume' | 'hold' | 'release' 
    | 'terminate') -> integer ()).
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
%% --------------------------------------------------------------------
%% @spec init ([]) -> {ok, State} | {ok, State, Timeout} |
%%                    ignore | {stop, Reason}
%% @doc Initiates the server.
%% @end
%% @hidden
%% --------------------------------------------------------------------
-type(init_return() :: {'ok', tuple()} | {'ok', tuple(), integer()} | 'ignore' | {'stop', any()}).
-spec(init/1::([]) -> init_return()).
init ([]) ->
  process_flag (trap_exit, true),
  SearchDir = filename:join ([filename:dirname (code:which (?MODULE)), "..", "ebin"]),
  case erl_ddll:load (SearchDir, "drmaa_drv")
  of
    ok -> 
      Port = open_port ({spawn, "drmaa_drv"}, [binary]),
      {ok, #state {port = Port, ops = []}};
    {error, Error} ->
      {stop, string:join (["Error loading drmaa driver: ", erl_ddll:format_error (Error)], "")}
  end.

%% --------------------------------------------------------------------
%% @spec code_change (OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
%% @end
%% @hidden
%% --------------------------------------------------------------------
code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

%% --------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages.
%% @end
%% @hidden
%% --------------------------------------------------------------------
handle_cast (_Msg, State) ->
  {noreply, State}.

%% --------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages.
%% @end
%% @hidden
%% --------------------------------------------------------------------
handle_info (_Info, State) ->
  {noreply, State}.

%% --------------------------------------------------------------------
%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any 
%% necessary cleaning up. When it returns, the gen_server terminates 
%% with Reason.
%%
%% The return value is ignored.
%% @end
%% @hidden
%% --------------------------------------------------------------------
terminate (normal, #state {port = Port}) ->
  port_command (Port, term_to_binary ({close, nop})),
  port_close (Port),
  ok;
terminate (_Reason, _State) ->
  ok.

%% --------------------------------------------------------------------
%% @spec handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% @doc Handling call messages.
%% @end
%% @hidden
%% --------------------------------------------------------------------
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


%% --------------------------------------------------------------------
%% @spec control_drv (Port::port (), Command::integer ()) -> {ok} 
%%                                          | {ok, job_id ()}
%%                                          | {ok, job_ids ()}
%%                                          | {ok, string ()}
%%                                          | string ()
%%                                          | integer ()
%%                                          | {error, string ()}
%% @doc Sends Command to the Port.
%% @end
%% @hidden
%% --------------------------------------------------------------------
-spec (control_drv/2::(port (), integer ()) -> {'ok'} | wait_result () 
    | {'error', string ()} | {'ok', job_id ()} | {'ok', job_ids()}
    | {'ok', string ()} | string () | integer ()).
control_drv (Port, Command) when is_port (Port) and is_integer (Command) ->
  port_control (Port, Command, <<"_">>),
  wait_result (Port).

%% --------------------------------------------------------------------
%% @spec control_drv (Port::port (), Command::integer (), Data::binary ()) -> 
%%                                            {ok} 
%%                                          | {ok, job_id ()}
%%                                          | {ok, job_ids ()}
%%                                          | {ok, string ()}
%%                                          | string ()
%%                                          | integer ()
%%                                          | {error, string ()}
%% @doc Sends Command and Data to the Port.
%% @end
%% @hidden
%% --------------------------------------------------------------------
-spec (control_drv/3::(port (), integer (), binary ()) -> {'ok'} | wait_result () 
    | {'error', string ()} | {'ok', job_id ()} | {'ok', job_ids()}
    | {'ok', string ()} | string () | integer ()).
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

