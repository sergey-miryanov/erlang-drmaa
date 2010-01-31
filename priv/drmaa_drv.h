#include <drmaa.h>
#include <erl_driver.h>
#include <ei.h>
#include <erl_interface.h>

#define CMD_ALLOCATE_JOB_TEMPLATE   1
#define CMD_DELETE_JOB_TEMPLATE     2
#define CMD_RUN_JOB                 3
#define CMD_RUN_BULK_JOBS           4
#define CMD_CONTROL                 5
#define CMD_JOB_PS                  6
#define CMD_SYNCHRONIZE             7
#define CMD_WAIT                    8
#define CMD_BLOCK_EMAIL             9
#define CMD_DEADLINE_TIME           10
#define CMD_DURATION_HLIMIT         11
#define CMD_DURATION_SLIMIT         12
#define CMD_ERROR_PATH              13
#define CMD_INPUT_PATH              14
#define CMD_JOB_CATEGORY            15
#define CMD_JOB_NAME                16
#define CMD_JOIN_FILES              17
#define CMD_JS_STATE                18
#define CMD_NATIVE_SPECIFICATION    19
#define CMD_OUTPUT_PATH             20
#define CMD_REMOTE_COMMAND          21
#define CMD_START_TIME              22
#define CMD_TRANSFER_FILES          23
#define CMD_V_ARGV                  24
#define CMD_V_EMAIL                 25
#define CMD_V_ENV                   26
#define CMD_WCT_HLIMIT              27
#define CMD_WCT_SLIMIT              28
#define CMD_WD                      29
#define CMD_PLACEHOLDER_HD          30
#define CMD_PLACEHOLDER_WD          31
#define CMD_PLACEHOLDER_INCR        32
#define CMD_JOB_IDS_SESSION_ALL     33
#define CMD_JOB_IDS_SESSION_ANY     34
#define CMD_TIMEOUT_FOREVER         35
#define CMD_TIMEOUT_NO_WAIT         36
#define CMD_CONTROL_SUSPEND         37
#define CMD_CONTROL_RESUME          38
#define CMD_CONTROL_HOLD            39
#define CMD_CONTROL_RELEASE         40
#define CMD_CONTROL_TERMINATE       41

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

static int 
send_atom (drmaa_drv_t *drv,
           char *atom);

static int
send_error (drmaa_drv_t *drv,
            char *tag,
            char *msg);

static void
ready_async (ErlDrvData drv_data,
             ErlDrvThreadData thread_data);

/* API */
static int
allocate_job_template (drmaa_drv_t *drv, 
                       char *command,
                       int len);

static int
delete_job_template (drmaa_drv_t *drv,
                     char *command,
                     int len);

static int
run_job (drmaa_drv_t *drv,
         char *command,
         int len);

static int
run_bulk_jobs (drmaa_drv_t *drv,
               char *command,
               int len);

static int
control_drmaa (drmaa_drv_t *drv,
               char *command,
               int len);

static int 
job_ps (drmaa_drv_t *drv,
        char *command,
        int len);

static int
synchronize (drmaa_drv_t *drv,
             char *command,
             int len);

static int
wait (drmaa_drv_t *drv,
      char *buffer,
      int len);

static int
set_attr (drmaa_drv_t *drv,
          const char *name,
          char *value,
          int len);

static int 
set_vector_attr (drmaa_drv_t *drv,
                 const char *name,
                 char *value,
                 int len);
static int
set_vector_attr_ (drmaa_drv_t *drv,
                  const char *name,
                  const char **value,
                  int len);

static int
send_placeholder (drmaa_drv_t *drv,
                  const char *placeholder);

static int
send_job_ids_session (drmaa_drv_t *drv,
                      const char *job_id_session);

static int
send_int (drmaa_drv_t *drv, long timeout);
