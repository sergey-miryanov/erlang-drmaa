#include <drmaa.h>
#include <erl_driver.h>
#include <ei.h>
#include <erl_interface.h>

#define CMD_ALLOCATE_JOB_TEMPLATE   1
#define CMD_SET_REMOTE_COMMAND      2
#define CMD_SET_V_ARGV              3
#define CMD_RUN_JOB                 4

typedef struct drmaa_drv_t {
  char                  err_msg[DRMAA_ERROR_STRING_BUFFER];
  int                   err_no;
  unsigned int          key;
  ErlDrvPort            port;
  FILE                  *log;
  drmaa_job_template_t  *job_template;
} drmaa_drv_t;

static ErlDrvData
start (ErlDrvPort port, 
       char *cmd);

static void
stop (ErlDrvData drv);

static int
control (ErlDrvData drv, 
         unsigned int command,
         char *buf,
         int len,
         char **rbuf,
         int rlen);

static int 
unknown (drmaa_drv_t *drv, 
         char *command,
         int len);

static void
ready_async (ErlDrvData drv_data,
             ErlDrvThreadData thread_data);

static int
allocate_job_template (drmaa_drv_t *drv, 
                       char *command,
                       int len);

static int 
set_remote_command (drmaa_drv_t *drv,
                    char *command,
                    int len);

static int
set_v_argv (drmaa_drv_t *drv,
            char *command,
            int len);

static int
run_job (drmaa_drv_t *drv,
         char *command,
         int len);

