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
#define DEFAULT_PUB_QOS                  1
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
#define MQTT_TOPIC_VALUE_OBJECT_LIST     7
#define MQTT_TOPIC_VALUE_FLOAT_PRIO_ARRAY  8
#define MQTT_TOPIC_VALUE_BINARY_PRIO_ARRAY 9

#define MAX_TOPIC_TOKENS                 10
#define MAX_TOPIC_TOKEN_LENGTH           31
#define MAX_TOPIC_VALUE_LENGTH           1501

#define MQTT_RETRY_LIMIT                 5
#define MQTT_RETRY_INTERVAL              10

#define MAX_JSON_KEY_LENGTH              100
#define MAX_JSON_VALUE_LENGTH            100
#define MAX_JSON_MULTI_VALUE_LENGTH      1024

#define MAX_CMD_STR_OPT_VALUE_LENGTH     256
#define MAX_CMD_OPT_TAG_VALUE_PAIR       5

#define CMD_TAG_FLAG_SLOW_TEST           0x01

#define BACNET_CLIENT_REQUEST_TTL        12
#define BACNET_CLIENT_WHOIS_TIMEOUT      3
#define BACNET_CLIENT_POINT_DISC_TIMEOUT 5

#define MAX_JSON_KEY_VALUE_PAIR          25
#define MAX_PRIORITY_ARRAY_LENGTH        16

#define PICS_RETRY_MAX                   3

#define POINT_DISC_STATE_GET_SIZE        0
#define POINT_DISC_STATE_GET_SIZE_SENT   1
#define POINT_DISC_STATE_GET_POINTS      2
#define POINT_DISC_STATE_GET_POINTS_SENT 3
#define POINT_DISC_STATE_PUBLISH         4

typedef struct _json_key_value_pair {
  char key[MAX_JSON_KEY_LENGTH];
  char value[MAX_CMD_STR_OPT_VALUE_LENGTH];
  int index;
} json_key_value_pair;

typedef struct _single_token_cb {
  char token[MAX_JSON_KEY_LENGTH];
} single_token_cb;

typedef struct _request_token_cb {
  char key[MAX_JSON_KEY_LENGTH];
  char value[MAX_JSON_VALUE_LENGTH];
} request_token_cb;

typedef struct _rpm_object_cb {
  BACNET_OBJECT_TYPE object_type;
  uint32_t object_instance;
  BACNET_PROPERTY_ID property;
  int32_t index;
  int32_t priority;
  char value[MAX_CMD_STR_OPT_VALUE_LENGTH];
  char *blob_value;
  char error[MAX_CMD_STR_OPT_VALUE_LENGTH];
} rpm_object_cb;

typedef struct _llist_obj_data {
  BACNET_OBJECT_TYPE object_type;
  uint32_t object_instance;
  BACNET_PROPERTY_ID object_property;
  int32_t index;
  int32_t priority;
  uint32_t device_instance;
  int dnet;
  char mac[MAX_CMD_STR_OPT_VALUE_LENGTH];
  char dadr[MAX_CMD_STR_OPT_VALUE_LENGTH];
  char value[MAX_CMD_STR_OPT_VALUE_LENGTH];
  int32_t req_tokens_len;
  int32_t prio_array_len;
  request_token_cb req_tokens[MAX_JSON_KEY_VALUE_PAIR];
  json_key_value_pair prio_array[MAX_PRIORITY_ARRAY_LENGTH];
  int topic_id;
  int rpm_objects_len;
  rpm_object_cb *rpm_objects;
  bool dont_publish_on_success;
  char err_msg[MAX_CMD_STR_OPT_VALUE_LENGTH];
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
    char mac[MAX_CMD_STR_OPT_VALUE_LENGTH];
    char dadr[MAX_CMD_STR_OPT_VALUE_LENGTH];
    int32_t device_instance_min;
    int32_t device_instance_max;
    address_table_cb address_table;
    uint32_t timeout;
    int32_t req_tokens_len;
    request_token_cb req_tokens[MAX_JSON_KEY_VALUE_PAIR];
    int publish_per_device;
  } data;
} llist_whois_cb;

typedef struct _llist_pics_cb {
  struct _llist_pics_cb *next;
  time_t timestamp;
  struct _llist_pics_data {
    uint8_t invoke_id;
  } data;
} llist_pics_cb;

typedef struct _point_entry_cb {
  int object_type;
  uint32_t instance;
  struct _point_entry_cb *next;
} point_entry_cb;

typedef struct _point_list_cb {
  point_entry_cb *head;
  point_entry_cb *tail;
} point_list_cb;

typedef struct _llist_point_disc_cb {
  struct _llist_point_disc_cb *next;
  uint32_t request_id;
  uint32_t request_object_id;
  time_t timestamp;
  int state;
  bool data_received;
  struct _llist_point_disc_cb_data {
    BACNET_ADDRESS dest;
    int dnet;
    char mac[MAX_CMD_STR_OPT_VALUE_LENGTH];
    char dadr[MAX_CMD_STR_OPT_VALUE_LENGTH];
    uint32_t device_instance;
    uint32_t points_count;
    point_list_cb point_list;
    uint32_t timeout;
    int32_t req_tokens_len;
    request_token_cb req_tokens[MAX_JSON_KEY_VALUE_PAIR];
  } data;
} llist_point_disc_cb;

typedef struct _bacnet_client_cmd_opts {
  BACNET_OBJECT_TYPE object_type;
  uint32_t object_instance;
  BACNET_PROPERTY_ID property;
  int32_t index;
  int32_t priority;
  uint32_t device_instance;
  int dnet;
  char mac[MAX_CMD_STR_OPT_VALUE_LENGTH];
  char dadr[MAX_CMD_STR_OPT_VALUE_LENGTH];
  char value[MAX_CMD_STR_OPT_VALUE_LENGTH];
  uint32_t tag_flags;
  char uuid[MAX_CMD_STR_OPT_VALUE_LENGTH];
  int32_t device_instance_min;
  int32_t device_instance_max;
  uint32_t timeout;
  int32_t req_tokens_len;
  int32_t prio_array_len;
  int publish_per_device;
  int rpm_objects_len;
  request_token_cb req_tokens[MAX_JSON_KEY_VALUE_PAIR];
  json_key_value_pair prio_array[MAX_PRIORITY_ARRAY_LENGTH];
  rpm_object_cb *rpm_objects;
  int page_number;
  int page_size;
} bacnet_client_cmd_opts;

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

void mqtt_check_reconnect(void);
int mqtt_client_init(void);
void mqtt_client_shutdown(void);
char *mqtt_form_publish_topic(char *device_id, char *object_name);
char *mqtt_create_topic(int object_type, int object_instance, int property_id, char *buf, int buf_len, int topic_type);
char *mqtt_create_error_topic(int object_type, int object_instance, int property_id, char *buf, int buf_len, int topic_type);
int mqtt_publish_topic(int object_type, int object_instance, int property_id, int vtype, void *vptr, char *uuid_value);
void sweep_bacnet_client_aged_requests(void);
void sweep_bacnet_client_whois_requests(void);
void sweep_bacnet_client_point_disc_requests(void);

int mqtt_lock(void);
int mqtt_unlock(void);

extern int pics_request_locked;

void mqtt_msg_queue_init(void);
void mqtt_msg_queue_shutdown(void);
int mqtt_msg_length(void);
unsigned int mqtt_msg_queue_size(void);
int mqtt_msg_pop_and_process(void);

int unsubscribe_persistent_topics(void);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif
