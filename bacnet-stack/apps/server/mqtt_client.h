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
#define MQTT_TOPIC_VALUE_FLOAT_MAX_PRIO  5
#define MQTT_TOPIC_VALUE_BINARY_MAX_PRIO 6

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
#define BACNET_CLIENT_WHOIS_TIMEOUT      3

#define MAX_JSON_KEY_VALUE_PAIR          10

typedef struct _llist_obj_data {
  uint32_t device_instance;
  BACNET_OBJECT_TYPE object_type;
  uint32_t object_instance;
  BACNET_PROPERTY_ID object_property;
  int priority;
  int index;
  char value[MAX_CMD_STR_OPT_VALUE_LENGTH];
} llist_obj_data;

typedef struct _llist_cb {
  struct _llist_cb *next;
  time_t timestamp;
  struct _llist_cb_data {
    uint8_t invoke_id;
    llist_obj_data obj_data;
  } data;
} llist_cb;

typedef struct _address_entry_cb {
    struct _address_entry_cb *next;
    uint8_t Flags;
    uint32_t device_id;
    unsigned max_apdu;
    BACNET_ADDRESS address;
} address_entry_cb;

typedef struct _address_table_cb {
  address_entry_cb *first;
  address_entry_cb *last;
} address_table_cb;

typedef struct _llist_whois_cb {
  struct _llist_whois_cb *next;
  uint32_t request_id;
  time_t timestamp;
  struct _llist_whois_cb_data {
    BACNET_ADDRESS dest;
    int dnet;
    int32_t device_instance_min;
    int32_t device_instance_max;
    address_table_cb address_table;
    uint32_t timeout;
  } data;
} llist_whois_cb;

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
  uint32_t tag_flags;
  char uuid[MAX_CMD_STR_OPT_VALUE_LENGTH];
  int32_t device_instance_min;
  int32_t device_instance_max;
  uint32_t timeout;
} bacnet_client_cmd_opts;

typedef struct _json_key_value_pair {
  char key[MAX_JSON_KEY_LENGTH];
  char value[MAX_CMD_STR_OPT_VALUE_LENGTH];
} json_key_value_pair;

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

void mqtt_check_reconnect(void);
int mqtt_client_init(void);
void mqtt_client_shutdown(void);
char *mqtt_form_publish_topic(char *device_id, char *object_name);
char *mqtt_create_topic(int object_type, int object_instance, int property_id, char *buf, int buf_len, int topic_type);
int mqtt_publish_topic(int object_type, int object_instance, int property_id, int vtype, void *vptr, char *uuid_value);
void sweep_bacnet_client_aged_requests(void);
void sweep_bacnet_client_whois_requests(void);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif
