#ifndef MQTT_CLIENT_H
#define MQTT_CLIENT_H

#include <time.h>

#ifndef true
#define true                             1
#endif

#ifndef false
#define false                            0
#endif

/* default values */
#define DEFAULT_MQTT_BROKER_IP           "127.0.0.1"
#define DEFAULT_MQTT_BROKER_PORT         1883
#define DEFAULT_PUB_QOS                  0
#define DEFAULT_PUB_TIMEOUT              10000L

/* topic types */
#define MQTT_UPDATE_RESULT_TOPIC           1
#define MQTT_READ_VALUE_CMD_RESULT_TOPIC   2
#define MQTT_WRITE_VALUE_CMD_RESULT_TOPIC  3

/* topic value types */
#define MQTT_TOPIC_VALUE_STRING          1
#define MQTT_TOPIC_VALUE_BACNET_STRING   2
#define MQTT_TOPIC_VALUE_INTEGER         3
#define MQTT_TOPIC_VALUE_FLOAT           4

#define MAX_TOPIC_TOKENS                 10
#define MAX_TOPIC_TOKEN_LENGTH           31
#define MAX_TOPIC_VALUE_LENGTH           255

#define MQTT_RETRY_LIMIT                 5
#define MQTT_RETRY_INTERVAL              10

#define MAX_JSON_KEY_LENGTH              100
#define MAX_JSON_VALUE_LENGTH            100

#define MAX_CMD_STR_OPT_VALUE_LENGTH     100
#define MAX_CMD_OPT_TAG_VALUE_PAIR       5

#define CMD_TAG_FLAG_SLOW_TEST           0x01

#define BACNET_CLIENT_REQUEST_TTL        12

typedef struct _llist_cb {
  struct _llist_cb *next;
  time_t timestamp;
  union {
    uint8_t invoke_id;
  } data;
} llist_cb;

/* bacnet-client command options */
typedef struct _cmd_opt_tag_value_pair {
} cmd_opt_tag_value_pair;

typedef struct _bacnet_client_cmd_opts {
  BACNET_OBJECT_TYPE object_type;
  uint32_t object_instance;
  BACNET_PROPERTY_ID property;
  int32_t index;
  int priority;
  uint32_t device_instance;
  int dnet;
  char mac[MAX_CMD_STR_OPT_VALUE_LENGTH];
  char daddr[MAX_CMD_STR_OPT_VALUE_LENGTH];
  char value[MAX_CMD_STR_OPT_VALUE_LENGTH];
  cmd_opt_tag_value_pair tag_value_pairs[MAX_CMD_OPT_TAG_VALUE_PAIR];
  uint32_t tag_flags;
  char uuid[MAX_CMD_STR_OPT_VALUE_LENGTH];
} bacnet_client_cmd_opts;

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

void mqtt_check_reconnect(void);
int mqtt_client_init(void);
void mqtt_client_shutdown(void);
char *mqtt_form_publish_topic(char *device_id, char *object_name);
char *mqtt_create_topic(int object_type, int object_instance, int property_id, char *buf, int buf_len, int topic_type);
int mqtt_publish_topic(int object_type, int object_instance, int property_id, int vtype, void *vptr, char *uuid_value);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif
