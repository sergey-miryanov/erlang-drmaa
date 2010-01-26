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

  fprintf (drv->log, "buf/%d: %s\n", len, buf);
  fprintf (drv->log, "rbuf/%d: %s\n", rlen, *rbuf);
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
      return send_atom (drv, "error");
    }

  return send_atom (drv, "ok");
}

static int
delete_job_template (drmaa_drv_t *drv,
                     char *command,
                     int len)
{
  return 0;
}

static int
run_job (drmaa_drv_t *drv,
         char *command,
         int len)
{
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
    }

  fflush (drv->log);
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
  return 0;
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
  return 0;
}

static int 
wait (drmaa_drv_t *drv,
      char *command,
      int len)
{
  return 0;
}

static int
set_attr (drmaa_drv_t *drv,
          const char *name,
          char *value,
          int len)
{
  value[len] = 0;
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
  value[len] = 0;

  char *count = value;
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

  const char **values = (const char **)driver_alloc (sizeof (const char *) * atoi (count) + 1);
  if (!values)
    {
      fprintf (drv->log, "Couldn't allocate memory for values: %d\n", atoi (count));
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

