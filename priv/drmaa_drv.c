#include "drmaa_drv.h"
#include <stdio.h>
#include <string.h>

static ErlDrvEntry driver_entry__ = {
  NULL,                             /* init */
  start,                            /* startup (defined below) */
  stop,                             /* shutdown (defined below) */
  NULL,                             /* output */
  NULL,                             /* ready_input */
  NULL,                             /* ready_output */
  "drmaa_drv",                      /* the name of the driver */
  NULL,                             /* finish */
  NULL,                             /* handle */
  control,                          /* control */
  NULL,                             /* timeout */
  NULL,                             /* outputv (defined below) */
  ready_async,                      /* ready_async */
  NULL,                             /* flush */
  NULL,                             /* call */
  NULL,                             /* event */
  ERL_DRV_EXTENDED_MARKER,          /* ERL_DRV_EXTENDED_MARKER */
  ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MAJOR_VERSION */
  ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MINOR_VERSION */
  ERL_DRV_FLAG_USE_PORT_LOCKING     /* ERL_DRV_FLAGs */
};

DRIVER_INIT (drmaa_driver)
{
  return &driver_entry__;
}

int
is_error (int err_no)
{
  return err_no != DRMAA_ERRNO_SUCCESS;
}

static ErlDrvData
start (ErlDrvPort port, char *cmd)
{
  FILE *log = fopen ("/tmp/erlang-drmaa-drv.log", "a+");
  if (!log)
    {
      fprintf (stderr, "Can't create log file\n");
      return 0;
    }

  drmaa_drv_t *drv = (drmaa_drv_t *)driver_alloc (sizeof (drmaa_drv_t));
  if (!drv)
    {
      fprintf (log, "Can't allocate memory for driver\n");
      fflush (log);
      fclose (log);
      return 0;
    }
  memset (drv, 0, sizeof (drmaa_drv_t));

  drv->key    = 44;
  drv->port   = port;
  drv->log    = log;
  drv->err_no = drmaa_init (NULL, drv->err_msg, DRMAA_ERROR_STRING_BUFFER);
  if (is_error (drv->err_no))
    {
      fprintf (drv->log, "Couldn't initialize the DRMAA library: %s\n", drv->err_msg);
    }
  else
    {
      fprintf (drv->log, "The DRMAA library initialized\n"); 
    }

  fflush (drv->log);
  return (ErlDrvData) drv;
}

static void
stop (ErlDrvData p)
{
  drmaa_drv_t *drv = (drmaa_drv_t *) (p);

  drv->err_no = drmaa_exit (drv->err_msg, DRMAA_ERROR_STRING_BUFFER);
  if (is_error (drv->err_no))
    {
      fprintf (drv->log, "Couldn't shut down the DRMAA library: %s\n", drv->err_msg);
    }
  else
    {
      fprintf (drv->log, "The DRMAA library shut down\n");
    }

  fclose (drv->log);
  drv->log = 0;

  driver_free (drv);
}

#define SET_ATTR(NAME)                              \
  case CMD_##NAME:                                  \
    set_attr (drv, DRMAA_##NAME, buf, len);         \
    break;
#define SET_V_ATTR(NAME)                            \
  case CMD_##NAME:                                  \
    set_vector_attr (drv, DRMAA_##NAME, buf, len);  \
    break;

static int
control (ErlDrvData p,
         unsigned int command,
         char *buf,
         int len,
         char **rbuf,
         int rlen)
{
  drmaa_drv_t *drv = (drmaa_drv_t *) (p);
  if (len)
    buf[len] = 0;

  fprintf (drv->log, "cmd: %d\n", command);
  fprintf (drv->log, "buf/%d: %s\n", len, buf);
  fflush (drv->log);

  switch (command)
    {
    case CMD_ALLOCATE_JOB_TEMPLATE:
      allocate_job_template (drv, buf, len);
      break;
    case CMD_DELETE_JOB_TEMPLATE:
      delete_job_template (drv, buf, len);
      break;
    case CMD_RUN_JOB:
      run_job (drv, buf, len);
      break;
    case CMD_RUN_BULK_JOBS:
      run_bulk_jobs (drv, buf, len);
      break;
    case CMD_CONTROL:
      control_drmaa (drv, buf, len);
      break;
    case CMD_JOB_PS:
      job_ps (drv, buf, len);
      break;
    case CMD_SYNCHRONIZE:
      synchronize (drv, buf, len);
      break;
    case CMD_WAIT:
      wait (drv, buf, len);
      break;
    SET_ATTR (BLOCK_EMAIL);
    SET_ATTR (DEADLINE_TIME);
    SET_ATTR (DURATION_HLIMIT);
    SET_ATTR (DURATION_SLIMIT);
    SET_ATTR (ERROR_PATH);
    SET_ATTR (INPUT_PATH);
    SET_ATTR (JOB_CATEGORY);
    SET_ATTR (JOB_NAME);
    SET_ATTR (JOIN_FILES);
    SET_ATTR (JS_STATE);
    SET_ATTR (NATIVE_SPECIFICATION);
    SET_ATTR (OUTPUT_PATH);
    SET_ATTR (REMOTE_COMMAND);
    SET_ATTR (START_TIME);
    SET_ATTR (TRANSFER_FILES);
    SET_V_ATTR (V_ARGV);
    SET_V_ATTR (V_EMAIL);
    SET_V_ATTR (V_ENV);
    SET_ATTR (WCT_HLIMIT);
    SET_ATTR (WCT_SLIMIT);
    SET_ATTR (WD);
    case CMD_PLACEHOLDER_HD:
      return send_placeholder (drv, DRMAA_PLACEHOLDER_HD);
    case CMD_PLACEHOLDER_WD:
      return send_placeholder (drv, DRMAA_PLACEHOLDER_WD);
    case CMD_PLACEHOLDER_INCR:
      return send_placeholder (drv, DRMAA_PLACEHOLDER_INCR);
    case CMD_JOB_IDS_SESSION_ALL:
      return send_job_ids_session (drv, DRMAA_JOB_IDS_SESSION_ALL);
    case CMD_JOB_IDS_SESSION_ANY:
      return send_job_ids_session (drv, DRMAA_JOB_IDS_SESSION_ANY);
    case CMD_TIMEOUT_FOREVER:
      return send_timeout (drv, DRMAA_TIMEOUT_WAIT_FOREVER);
    case CMD_TIMEOUT_NO_WAIT:
      return send_timeout (drv, DRMAA_TIMEOUT_NO_WAIT);
    default:
      unknown (drv, buf, len);
      break;
    }

  return 0;
}

static void
ready_async (ErlDrvData drv_data, ErlDrvThreadData thread_data)
{
}

static int
unknown (drmaa_drv_t *drv, char *command, int len)
{
  ErlDrvTermData spec[] = {
      ERL_DRV_ATOM, driver_mk_atom ("error"),
      ERL_DRV_ATOM, driver_mk_atom ("unknown_command"),
      ERL_DRV_TUPLE, 2
  };

  return driver_output_term (drv->port,
                             spec,
                             sizeof (spec) / sizeof (spec[0]));
}

static int
send_atom (drmaa_drv_t *drv, char *atom)
{
  ErlDrvTermData spec[] = {
      ERL_DRV_ATOM, driver_mk_atom (atom),
      ERL_DRV_TUPLE, 1
  };

  return driver_output_term (drv->port,
                             spec,
                             sizeof (spec) / sizeof (spec[0]));
}

static
send_error (drmaa_drv_t *drv,
            char *tag,
            char *msg)
{
  ErlDrvTermData spec[] = {
      ERL_DRV_ATOM, driver_mk_atom (tag),
      ERL_DRV_STRING, (ErlDrvTermData)msg, strlen (msg),
      ERL_DRV_TUPLE, 2
  };

  return driver_output_term (drv->port,
                             spec,
                             sizeof (spec) / sizeof (spec[0]));
}

static int
allocate_job_template (drmaa_drv_t *drv, 
                       char *command,
                       int len)
{
  drv->err_no = drmaa_allocate_job_template (&drv->job_template, drv->err_msg, DRMAA_ERROR_STRING_BUFFER);
  if (is_error (drv->err_no))
    {
      fprintf (drv->log, "Couldn't allocate job template: %s\n", drv->err_msg);
      fflush (drv->log);

      return send_atom (drv, "error");
    }

  return send_atom (drv, "ok");
}

static int
delete_job_template (drmaa_drv_t *drv,
                     char *command,
                     int len)
{
  if (!drv->job_template)
    {
      fprintf (drv->log, "Job template is NULL\n");
      fflush (drv->log);

      return send_error (drv, "error", "Job template is null, allocate first");
    }

  drv->err_no = drmaa_delete_job_template (drv->job_template,
                                           drv->err_msg,
                                           DRMAA_ERROR_STRING_BUFFER);
  if (is_error (drv->err_no))
    {
      fprintf (drv->log, "Couldn't delete job template: %s\n", drv->err_msg);
      fflush (drv->log);

      return send_error (drv, "error", drv->err_msg);
    }

  return send_atom (drv, "ok");
}

static int
run_job (drmaa_drv_t *drv,
         char *command,
         int len)
{
  if (!drv->job_template)
    {
      fprintf (drv->log, "Job template is NULL\n");
      fflush (drv->log);

      return send_error (drv, "error", "Job template is null, allocate first");
    }

  char job_id[DRMAA_JOBNAME_BUFFER] = {0};
  drv->err_no = drmaa_run_job (job_id,
                               DRMAA_JOBNAME_BUFFER,
                               drv->job_template,
                               drv->err_msg,
                               DRMAA_ERROR_STRING_BUFFER);
  if (is_error (drv->err_no))
    {
      fprintf (drv->log, "Coldn't submit job: %s\n", drv->err_msg);
      return send_atom (drv, "error");
    }
  else
    {
      fprintf (drv->log, "JobID: %s\n", job_id);
      fflush (drv->log);
    }

  ErlDrvTermData spec[] = {
    ERL_DRV_ATOM, driver_mk_atom ("ok"),
    ERL_DRV_STRING, (ErlDrvTermData)job_id, strlen (job_id),
    ERL_DRV_TUPLE, 2
  };

  return driver_output_term (drv->port,
                             spec,
                             sizeof (spec) / sizeof (spec[0]));
}

static int
run_bulk_jobs (drmaa_drv_t *drv,
               char *command,
               int len)
{
  if (!drv->job_template)
    {
      fprintf (drv->log, "Job template is NULL\n");
      fflush (drv->log);

      return send_error (drv, "error", "Job template is null, allocate first");
    }

  char *start = command;
  char *end = strstr (start, ",");
  if (!end)
    {
      fprintf (drv->log, "Invalid command: %s\n", command);
      fflush (drv->log);

      return send_error (drv, "error", "Invalid command buffer");
    }

  char *incr = strstr (end + 1, ",");
  if (!incr)
    {
      fprintf (drv->log, "Invalid command: %s\n", command);
      fflush (drv->log);

      return send_error (drv, "error", "Invalid command buffer");
    }

  drmaa_job_ids_t *jobs = NULL;
  drv->err_no = drmaa_run_bulk_jobs (&jobs,
                                     drv->job_template,
                                     atoi (start),
                                     atoi (end + 1),
                                     atoi (incr + 1),
                                     drv->err_msg,
                                     DRMAA_ERROR_STRING_BUFFER);
  if (is_error (drv->err_no))
    {
      fprintf (drv->log, "Couldn't run bulk of jobs: %s\n", drv->err_msg);
      fflush (drv->log);

      return send_error (drv, "error", "Couldn't run bulk of jobs");
    }

  ErlDrvTermData *result = (ErlDrvTermData *)driver_alloc (sizeof (ErlDrvTermData) * 2);
  if (!result)
    {
      fprintf (drv->log, "Couldn't allocate memory for result\n");
      fflush (drv->log);

      drmaa_release_job_ids (jobs);
      return send_error (drv, "error", "Couldn't allocate memory for result");
    }

  size_t result_idx = 0;
  result[result_idx++] = ERL_DRV_ATOM;
  result[result_idx++] = driver_mk_atom ("ok");

  char job [DRMAA_JOBNAME_BUFFER] = {0};
  size_t job_count = 0;
  for (;; ++job_count)
    {
      drv->err_no = drmaa_get_next_job_id (jobs, job, DRMAA_JOBNAME_BUFFER);
      if (is_error (drv->err_no))
        {
          if (drv->err_no == DRMAA_ERRNO_NO_MORE_ELEMENTS)
            break;
          else
            {
              fprintf (drv->log, "Couldn't get next job id: %d\n", drv->err_no);
              fflush (drv->log);

              drmaa_release_job_ids (jobs);
              driver_free (result);

              return send_error (drv, "error", "Couldn't get next job id");
            }
        }

      result = (ErlDrvTermData *) driver_realloc (result, sizeof (*result) * (result_idx + 3));
      if (!result)
        {
          fprintf (drv->log, "Couldn't realloc memory for result\n");
          fflush (drv->log);

          drmaa_release_job_ids (jobs);

          return send_error (drv, "error", "Couldn't reallocate memory for result");
        }

      size_t job_id_len = strlen (job);
      char *job_id = (char *) driver_alloc (sizeof (char) * (job_id_len + 1));
      if (!job_id)
        {
          fprintf (drv->log, "Couldn't allocate memory for job id: %d\n", job_count);
          fflush (drv->log);

          driver_free (result);
          drmaa_release_job_ids (jobs);

          return send_error (drv, "error", "Couldn't allocate memory for job id");
        }

      memcpy (job_id, job, job_id_len + 1);

      result[result_idx++] = ERL_DRV_STRING;
      result[result_idx++] = (ErlDrvTermData)job_id;
      result[result_idx++] = job_id_len;
    }

  result = (ErlDrvTermData *) driver_realloc (result, sizeof (*result) * (result_idx + 5));
  if (!result)
    {
      fprintf (drv->log, "Couldn't reallocate memory for result\n");
      fflush (drv->log);

      drmaa_release_job_ids (jobs);

      return send_error (drv, "error", "Couldn't reallocate memory for result");
    }

  result[result_idx++] = ERL_DRV_NIL;
  result[result_idx++] = ERL_DRV_LIST;
  result[result_idx++] = job_count + 1;
  result[result_idx++] = ERL_DRV_TUPLE;
  result[result_idx++] = 2;

  int r = driver_output_term (drv->port,
                              result, result_idx);

  size_t idx = 0;
  for (; idx < result_idx; ++idx)
    {
      if (result[idx] == ERL_DRV_STRING)
        driver_free ((char *)result[idx + 1]);
    }

  driver_free (result);
  drmaa_release_job_ids (jobs);

  return r;
}

static int
control_drmaa (drmaa_drv_t *drv,
               char *command,
               int len)
{
  return 0;
}

static int
job_ps (drmaa_drv_t *drv,
        char *command,
        int len)
{
  return 0;
}

static int 
synchronize (drmaa_drv_t *drv,
             char *command,
             int len)
{
  size_t timeout = atoi (command);
  command = strstr (command, ",");
  if (!command)
    {
      fprintf (drv->log, "Command should contain not only timeout value: %s\n", command);
      fflush (drv->log);

      return send_error (drv, "error", "Command should contain not only timeout value");
    }
  ++command;

  size_t job_count = atoi (command);
  if (!job_count)
    {
      fprintf (drv->log, "Command buffer should contain at least one job_id: %s\n", command);
      fflush (drv->log);

      return send_error (drv, "error", "Command buffer should contain at least on job_id");
    }

  command = strstr (command, ",");
  if (!command)
    {
      fprintf (drv->log, "Command buffer should contain any job ids: %s\n", command);
      fflush (drv->log);

      return send_error (drv, "error", "Command buffer should contain any job ids");
    }

  char const **job_ids = (char const **)driver_alloc (sizeof (char *) * (job_count + 1));
  if (!job_ids)
    {
      fprintf (drv->log, "Couldn't allocate memory for job ids\n");
      fflush (drv->log);

      return send_error (drv, "error", "Couldn't allocate memory for job ids");
    }

  ++command;

  size_t idx = 0;
  char *job_id = 0;
  while ((job_id = strstr (command, ",")))
    {
      job_ids[idx++]  = command;
      job_id[0]       = 0;
      command         = job_id + 1;
    }

  if (strlen (command))
    {
      job_ids[idx++] = command;
    }

  job_ids[idx] = 0;

  drv->err_no = drmaa_synchronize (job_ids, timeout, 0, 
                                   drv->err_msg, DRMAA_ERROR_STRING_BUFFER);
  if (is_error (drv->err_no))
    {
      fprintf (drv->log, "Couldn't synchronize with jobs: %s\n", drv->err_msg);
      fflush (drv->log);

      driver_free (job_ids);
      return send_error (drv, "error", drv->err_msg);
    }

  ErlDrvTermData result[] = {
      ERL_DRV_ATOM, driver_mk_atom ("ok"),
      ERL_DRV_UINT, idx,
      ERL_DRV_TUPLE, 2
  };

  int r = driver_output_term (drv->port,
                              result,
                              sizeof (result) / sizeof (result[0]));

  driver_free (job_ids);
  return r;
}

static int 
wait (drmaa_drv_t *drv,
      char *command,
      int len)
{
  size_t timeout = atoi (command);
  command = strstr (command, ",");
  if (!command)
    {
      fprintf (drv->log, "Command buffer should contain not only timeout value: %s\n", command);
      fflush (drv->log);

      return send_error (drv, "error", "Command buffer should contain not only timeout value");
    }
  ++command;

  char job_out[DRMAA_JOBNAME_BUFFER] = {0};
  int status = 0;
  drmaa_attr_values_t *rusage = NULL;

  drv->err_no = drmaa_wait (command, job_out, DRMAA_JOBNAME_BUFFER,
                            &status, DRMAA_TIMEOUT_WAIT_FOREVER,
                            &rusage, 
                            drv->err_msg, DRMAA_ERROR_STRING_BUFFER);
  if (is_error (drv->err_no))
    {
      fprintf (drv->log, "Couldn't wait for job: %s\n", drv->err_msg);
      fflush (drv->log);

      return send_error (drv, "error", drv->err_msg);
    }

  char *exit_type = 0;
  int exit_status = 0;
  char term_signal [DRMAA_SIGNAL_BUFFER + 1] = {0};
  int has_term_signal = 0;

  if (!exit_type)
    {
      int aborted = 0;
      drmaa_wifaborted (&aborted, status, NULL, 0);
      if (aborted)
        {
          exit_type = "aborted";

          fprintf (drv->log, "Job %s never ran\n", command);
          fflush (drv->log);
        }
    }

  if (!exit_type)
    {
      int exited = 0;
      drmaa_wifexited (&exited, status, NULL, 0);
      if (exited)
        {
          exit_type = "exited";
          drmaa_wexitstatus (&exit_status, status, NULL, 0);

          fprintf (drv->log, "Job %s finished regularly with exit status %d\n", command, exit_status);
          fflush (drv->log);
        }
    }

  if (!exit_type)
    {
      int signaled = 0;
      drmaa_wifsignaled (&signaled, status, NULL, 0);
      if (signaled)
        {
          exit_type = "signaled";
          has_term_signal = 1;
          drmaa_wtermsig (term_signal, DRMAA_SIGNAL_BUFFER,
                         status, NULL, 0);

          fprintf (drv->log, "Job %s finished due to signal %s\n", command, term_signal);
          fflush (drv->log);
        }
    }

  if (!exit_type)
    {
      exit_type = "unclear";
      fprintf (drv->log, "Job %s finished with unclear conditions\n", command);
      fflush (drv->log);
    }

  int attr_num = 0;
  drv->err_no = drmaa_get_num_attr_values (rusage, &attr_num);
  if (is_error (drv->err_no))
    {
      fprintf (drv->log, "Couldn't get number of attribute values\n");
      fflush (drv->log);

      drmaa_release_attr_values (rusage);
      return send_error (drv, "error", "Couldn't get number of attribute values");
    }

  ErlDrvTermData spec[] = {
      ERL_DRV_ATOM, driver_mk_atom ("ok"),
      ERL_DRV_ATOM, driver_mk_atom ("job_id"),
      ERL_DRV_STRING, (ErlDrvTermData)job_out, strlen (job_out),
      ERL_DRV_TUPLE, 2,
      ERL_DRV_ATOM, driver_mk_atom ("exit"),
      ERL_DRV_STRING, (ErlDrvTermData)exit_type, strlen (exit_type),
      ERL_DRV_TUPLE, 2,
      ERL_DRV_ATOM, driver_mk_atom ("exit_status"),
      ERL_DRV_INT, exit_status,
      ERL_DRV_TUPLE, 2,
      ERL_DRV_ATOM, driver_mk_atom ("usage"),
  };

  size_t spec_num = sizeof (spec) / sizeof (spec[0]);
  size_t result_num = spec_num + 3 * attr_num + 1 + 2 + 2 + 2;
  ErlDrvTermData *result = (ErlDrvTermData *)driver_alloc (sizeof (ErlDrvTermData) * result_num);
  if (!result)
    {
      fprintf (drv->log, "Couldn't allocate memory for reply\n");
      fflush (drv->log);

      return send_error (drv, "error", "Couldn't allocate memory");
    }

  size_t idx = 0;
  for (; idx < spec_num; ++idx)
    {
      result[idx] = spec[idx];
    }

  fprintf (drv->log, "Usage: \n");
  fflush (drv->log);

  typedef char usage_t[DRMAA_ERROR_STRING_BUFFER];
  usage_t *usage = (usage_t *)driver_alloc (sizeof (usage_t) * attr_num);
  if (!usage)
    {
      fprintf (drv->log, "Couldn't allocate memory for usage\n");
      fflush (drv->log);

      return send_error (drv, "error", "Couldn't allocate usage");
    }

  size_t usage_idx = 0;
  for (; usage_idx < attr_num; ++usage_idx)
    {
      int err_no = drmaa_get_next_attr_value (rusage,
                                              usage[usage_idx],
                                              sizeof (usage_t));
      if (err_no == DRMAA_ERRNO_SUCCESS)
        {
          result[idx++] = ERL_DRV_STRING;
          result[idx++] = (ErlDrvTermData)usage[usage_idx];
          result[idx++] = strlen (usage[usage_idx]);

          fprintf (drv->log, "  %s\n", usage[usage_idx]);
          fflush (drv->log);
        }
    }

  result[idx++] = ERL_DRV_NIL;
  result[idx++] = ERL_DRV_LIST;
  result[idx++] = attr_num + 1;
  result[idx++] = ERL_DRV_TUPLE;
  result[idx++] = 2;
  result[idx++] = ERL_DRV_TUPLE;
  result[idx++] = 5;

  fprintf (drv->log, "result: %d, idx: %d\n", result_num, idx);
  fflush (drv->log);

  drmaa_release_attr_values (rusage);

  int r = driver_output_term (drv->port, result, result_num);

  driver_free (usage);
  driver_free (result);

  return r;
}

static int
set_attr (drmaa_drv_t *drv,
          const char *name,
          char *value,
          int len)
{
  if (!drv->job_template)
    {
      fprintf (drv->log, "Job template is NULL\n");
      fflush (drv->log);

      return send_error (drv, "error", "Job template is null, allocate first");
    }

  drv->err_no = drmaa_set_attribute (drv->job_template,
                                     name,
                                     value,
                                     drv->err_msg,
                                     DRMAA_ERROR_STRING_BUFFER);
  if (is_error (drv->err_no))
    {
      fprintf (drv->log, "Couldn't set attribute \"%s\": %s\n",
               name,
               drv->err_msg);
      return send_atom (drv, "error");
    }

  return send_atom (drv, "ok");
}

static int
set_vector_attr (drmaa_drv_t *drv,
                 const char *name,
                 char *value,
                 int len)
{
  size_t count = atoi (value);
  if (!count)
    {
      const char *values[] = {0};
      return set_vector_attr_ (drv, name, values, 0);
    }

  value = strstr (value, ",");
  if (!value)
    {
      fprintf (drv->log, "Command should contain number of items: %s\n", count);
      fflush (drv->log);
      return send_error (drv, "error", "Command should contain number of items");
    }
  else
    {
      ++value;
    }

  const char **values = (const char **)driver_alloc (sizeof (const char *) * (count + 1));
  if (!values)
    {
      fprintf (drv->log, "Couldn't allocate memory for values: %d\n", count);
      fflush (drv->log);
      return send_error (drv, "error", "Couldn't allocate memory for values");
    }

  int idx = 0;
  char *new_value = 0;
  while ((new_value = strstr (value, ",")))
    {
      values[idx++] = value;
      new_value[0]  = 0;
      value         = new_value + 1;
    }

  if (strlen (value))
    {
      values[idx++] = value;
    }

  values[idx] = 0;
  int result = set_vector_attr_ (drv, name, values, idx);

  driver_free (values);
  return result;
}

static int
set_vector_attr_ (drmaa_drv_t *drv,
                  const char *name,
                  const char **values,
                  int len)
{
  if (!drv->job_template)
    {
      fprintf (drv->log, "Job template is NULL\n");
      fflush (drv->log);

      return send_error (drv, "error", "Job template is null, allocate first");
    }

  drv->err_no = drmaa_set_vector_attribute (drv->job_template,
                                            name,
                                            values,
                                            drv->err_msg,
                                            DRMAA_ERROR_STRING_BUFFER);
  if (is_error (drv->err_no))
    {
      fprintf (drv->log, "Couldn't set attribute \"%s\": %s\n",
               name,
               drv->err_msg);
      return send_error (drv, "error", "Couldn't set attribute");
    }

  return send_atom (drv, "ok");
}

static int
send_placeholder (drmaa_drv_t *drv, const char *placeholder)
{
  ErlDrvTermData result[] = {
      ERL_DRV_STRING, (ErlDrvTermData)placeholder, strlen (placeholder)
  };

  return driver_output_term (drv->port,
                             result,
                             sizeof (result) / sizeof (result[0]));
}

static int
send_job_ids_session (drmaa_drv_t *drv, const char *job_id_session)
{
  ErlDrvTermData result[] = {
      ERL_DRV_STRING, (ErlDrvTermData) job_id_session, strlen (job_id_session)
  };

  return driver_output_term (drv->port,
                             result,
                             sizeof (result) / sizeof (result[0]));
}

static int
send_timeout (drmaa_drv_t *drv, long timeout)
{
  ErlDrvTermData result[] = {
      ERL_DRV_INT, timeout
  };

  return driver_output_term (drv->port,
                             result,
                             sizeof (result) / sizeof (result[0]));
}
