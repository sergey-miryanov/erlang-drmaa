#include "drmaa_drv.h"

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

static int
control (ErlDrvData p,
         unsigned int command,
         char *buf,
         int len,
         char **rbuf,
         int rlen)
{
  drmaa_drv_t *drv = (drmaa_drv_t *) (p);

  switch (command)
    {
    case CMD_ALLOCATE_JOB_TEMPLATE:
      allocate_job_template (drv, buf, len);
      break;
    case CMD_SET_REMOTE_COMMAND:
      set_remote_command (drv, buf, len);
      break;
    case CMD_SET_V_ARGV:
      set_v_argv (drv, buf, len);
      break;
    case CMD_RUN_JOB:
      run_job (drv, buf, len);
      break;
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
allocate_job_template (drmaa_drv_t *drv, 
                       char *command,
                       int len)
{
  drv->err_no = drmaa_allocate_job_template (&drv->job_template, drv->err_msg, DRMAA_ERROR_STRING_BUFFER);
  if (is_error (drv->err_no))
    {
      fprintf (drv->log, "Couldn't allocate job template: %s\n", drv->err_msg);
      return 1;
    }

  return 0;
}

static int
set_attr (drmaa_drv_t *drv,
          const char *name,
          const char *value)
{
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
      return 1;
    }

  return 0;
}

static int
set_vector_attr (drmaa_drv_t *drv,
                 const char *name,
                 const char *values[])
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
      return 1;
    }

  return 0;
}

static int
set_remote_command (drmaa_drv_t *drv,
                    char *command,
                    int len)
{
  return set_attr (drv, DRMAA_REMOTE_COMMAND, "python");
}

static int
set_v_argv (drmaa_drv_t *drv,
            char *command,
            int len)
{
  const char *args[] = {"run_alpha_script.py", NULL};
  return set_vector_attr (drv, DRMAA_V_ARGV, args);
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
      return 1;
    }
  else
    {
      fprintf (drv->log, "JobID: %s\n", job_id);
    }

  fflush (drv->log);
  ErlDrvTermData spec[] = {
    ERL_DRV_ATOM, driver_mk_atom ("ok"),
    ERL_DRV_STRING, (ErlDrvTermData)job_id,
    ERL_DRV_TUPLE, 2
  };

  return driver_output_term (drv->port,
                             spec,
                             sizeof (spec) / sizeof (spec[0]));
}

