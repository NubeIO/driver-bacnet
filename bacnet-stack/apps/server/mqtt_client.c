/*
 * MQTT client module
 */
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

#include <pthread.h>

#include "bacnet/indtext.h"
#include "bacnet/bacenum.h"
#include "bacnet/basic/object/device.h"
#include "bacnet/basic/binding/address.h"
#include "bacnet/npdu.h"
#include "bacnet/apdu.h"
#include "bacnet/datalink/datalink.h"
#include "bacnet/bactext.h"
#include "bacnet/basic/tsm/tsm.h"
#include "bacnet/property.h"

#include "bacnet/basic/services.h"
#include "bacnet/basic/sys/keylist.h"

#include "MQTTAsync.h"
#include "json-c/json.h"
#include "mqtt_client.h"
#if defined(YAML_CONFIG)
#include "yaml_config.h"
#endif /* defined(YAML_CONFIG) */

#ifndef AO_LEVEL_NULL
#define AO_LEVEL_NULL 255
#endif

#ifndef AV_LEVEL_NULL
#define AV_LEVEL_NULL 255
#endif

#ifndef BAC_ADDRESS_MULT
#define BAC_ADDRESS_MULT 1
#endif

#ifndef WRITEM_PROP_PUB_LENGTH
#define WRITEM_PROP_PUB_LENGTH    1024
#endif

#ifndef EPICS_PAGE_SIZE_DEFAULT
#define EPICS_PAGE_SIZE_DEFAULT   100
#endif

#ifndef PROP_VALUE_MAX_LEN
#define PROP_VALUE_MAX_LEN 256
#endif

typedef enum {
        /** Initial state to establish a binding with the target device. */
    INITIAL_BINDING,
        /** Get selected device information and put out the heading information. */
    GET_HEADING_INFO, GET_HEADING_RESPONSE, PRINT_HEADING,
        /** Getting ALL properties and values at once with RPM. */
    GET_ALL_REQUEST, GET_ALL_RESPONSE,
        /** Getting ALL properties with array index = 0, just to get the list. */
    GET_LIST_OF_ALL_REQUEST, GET_LIST_OF_ALL_RESPONSE,
        /** Processing the properties individually with ReadProperty. */
    GET_PROPERTY_REQUEST, GET_PROPERTY_RESPONSE,
        /** Done with this Object; move onto the next. */
    NEXT_OBJECT
} EPICS_STATES;

/* forward decls */
extern bool Analog_Input_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid, int bacnet_client);
extern void Analog_Input_Present_Value_Set(
  uint32_t object_instance,
  float value,
  char *uuid,
  int bacnet_client);
extern float Analog_Input_Present_Value(uint32_t object_instance);
extern unsigned Analog_Input_Instance_To_Index(
  uint32_t instance);
extern bool Analog_Input_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name);

extern bool Analog_Output_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid, int bacnet_client);
extern bool Analog_Output_Present_Value_Set(
  uint32_t object_instance, float value, unsigned priority, char *uuid, int bacnet_client);
extern bool Analog_Output_Priority_Array_Set(
  uint32_t object_instance, float value, unsigned priority, char *uuid, int bacnet_client);
extern bool Analog_Output_Priority_Array_Set2(
  uint32_t object_instance, float value, unsigned priority);
extern float Analog_Output_Present_Value(uint32_t object_instance);
extern unsigned Analog_Output_Instance_To_Index(uint32_t object_instance);
extern bool Analog_Output_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name);
extern void get_ao_priority_array(uint32_t object_instance, float *pa, int pa_length);

extern bool Analog_Value_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid, int bacnet_client);
extern bool Analog_Value_Present_Value_Set(
  uint32_t object_instance, float value, uint8_t priority, char *uuid, int bacnet_client);
extern bool Analog_Value_Priority_Array_Set(
  uint32_t object_instance, float value, uint8_t priority, char *uuid);
extern bool Analog_Value_Priority_Array_Set2(
  uint32_t object_instance, float value, uint8_t priority);
extern float Analog_Value_Present_Value(uint32_t object_instance);
extern unsigned Analog_Value_Instance_To_Index(uint32_t object_instance);
extern bool Analog_Value_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name);
extern void get_av_priority_array(uint32_t object_instance, float *pa, int pa_length);

extern bool Binary_Input_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid, int bacnet_client);
extern bool Binary_Input_Present_Value_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, char *uuid, int bacnet_client);
extern BACNET_BINARY_PV Binary_Input_Present_Value(uint32_t object_instance);
extern unsigned Binary_Input_Instance_To_Index(uint32_t object_instance);
extern bool Binary_Input_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name);

extern bool Binary_Output_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid, int bacnet_client);
extern bool Binary_Output_Present_Value_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid, int bacnet_client);
extern bool Binary_Output_Priority_Array_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid);
extern bool Binary_Output_Priority_Array_Set2(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority);
extern BACNET_BINARY_PV Binary_Output_Present_Value(uint32_t object_instance);
extern unsigned Binary_Output_Instance_To_Index(uint32_t object_instance);
extern bool Binary_Output_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name);
extern void get_bo_priority_array(uint32_t object_instance, BACNET_BINARY_PV *pa, int pa_length);

extern bool Binary_Value_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid, int bacnet_client);
extern bool Binary_Value_Present_Value_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid, int bacnet_client);
extern bool Binary_Value_Priority_Array_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid);
extern bool Binary_Value_Priority_Array_Set2(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority);
extern BACNET_BINARY_PV Binary_Value_Present_Value(uint32_t object_instance);
extern unsigned Binary_Value_Instance_To_Index(uint32_t object_instance);
extern bool Binary_Value_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name);
extern void get_bv_priority_array(uint32_t object_instance, BACNET_BINARY_PV *pa, int pa_length);

int tokenize_topic(char *topic, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH]);
void dump_topic_tokens(int n_topic_tokens, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH]);
int process_ai_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int set_ai_property_persistent_value(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value);
int process_ao_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int set_ao_property_persistent_value(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, json_object *json_field);
int process_av_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int set_av_property_persistent_value(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, json_object *json_field);
int process_bi_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int set_bi_property_persistent_value(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value);
int process_bo_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int set_bo_property_persistent_value(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, json_object *json_field);
int process_bv_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int set_bv_property_persistent_value(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, json_object *json_field);
void mqtt_connection_lost(void *context, char *cause);
int mqtt_msg_arrived(void *context, char *topic, int topic_len, MQTTAsync_message *message);
int mqtt_connect_to_broker(void);
int subscribe_write_prop_name(void *context);
int subscribe_write_prop_present_value(void *context);
int subscribe_write_prop_priority_array(void *context);
int mqtt_subscribe_to_topics(void *context);
int subscribe_bacnet_client_whois_command(void *context);
int subscribe_bacnet_client_read_value_command(void *context);
int subscribe_bacnet_client_write_value_command(void *context);
int subscribe_bacnet_client_pics_command(void *context);
int subscribe_bacnet_client_point_discovery_command(void *context);
int subscribe_bacnet_client_point_list_command(void *context);
int mqtt_subscribe_to_bacnet_client_topics(void *context);
int extract_json_fields_to_cmd_opts(json_object *json_root, bacnet_client_cmd_opts *cmd_opts);
int process_bacnet_client_whois_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_pics_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_point_discovery_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_points_info_command(bacnet_client_cmd_opts *opts);
int mqtt_publish_command_result(int object_type, int object_instance, int property_id, int priority,
  int vtype, void *vptr, int topic_id, json_key_value_pair *kv_pairs, int kvps_length);
bool bbmd_address_match_self(BACNET_IP_ADDRESS *addr);
int is_address_us(bacnet_client_cmd_opts *opts);
int is_command_for_us(bacnet_client_cmd_opts *opts);
char *get_object_type_str(int object_type);
bool get_object_type_id(char *name, uint32_t *id);
char *get_object_property_str(int object_property);
int encode_read_value_result(BACNET_READ_PROPERTY_DATA *data, llist_obj_data *obj_data, char *buf, int buf_len);
int encode_read_multiple_value_result(BACNET_READ_ACCESS_DATA *rpm_data, llist_obj_data *obj_data);
int encode_write_value_result(llist_obj_data *data, char *buf, int buf_len);
int publish_bacnet_client_read_value_result(BACNET_READ_PROPERTY_DATA *data, llist_obj_data *obj_data);
int publish_bacnet_client_write_value_result(llist_obj_data *data);
int publish_bacnet_client_read_multiple_value_result(llist_obj_data *data);
int publish_bacnet_client_write_multiple_value_result(llist_obj_data *data);
int publish_bacnet_client_whois_result(llist_whois_cb *cb);
int publish_topic_value(char *topic, char *topic_value);
int publish_bacnet_client_pics_result(bacnet_client_cmd_opts *opts, char *epics_json);
int publish_whois_device(char *topic, char *topic_value);
int publish_bacnet_client_whois_result_per_device(llist_whois_cb *cb);
void set_kvps_for_reply(json_key_value_pair *kvps, int kvps_len, int *kvps_outlen, bacnet_client_cmd_opts *opts);
int process_local_read_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_read_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_read_multiple_value_command(bacnet_client_cmd_opts *opts);
void init_bacnet_client_service_handlers(void);
int process_local_write_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_write_multiple_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_write_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_command(char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], int n_topic_tokens, bacnet_client_cmd_opts *opts);

void encode_array_value_result(BACNET_READ_PROPERTY_DATA *data, llist_obj_data *obj_data, char *buf, int buf_len);
void encode_read_multiple_array_value_result(BACNET_READ_ACCESS_DATA *rpm_data, char *buf, int buf_len);
void encode_read_multiple_list_value_result(BACNET_READ_ACCESS_DATA *rpm_data, char **buf);
void encode_object_list_value_result(BACNET_READ_PROPERTY_DATA *data, llist_obj_data *obj_data, char *buf, int buf_len);

void init_bacnet_client_request_list(void);
void shutdown_bacnet_client_request_list(void);
int is_bacnet_client_request_present(uint8_t invoke_id);
llist_obj_data *get_bacnet_client_request_obj_data(uint8_t invoke_id, llist_obj_data *buf);
int add_bacnet_client_request(uint8_t invoke_id, llist_obj_data *obj_data);
int del_bacnet_client_request(uint8_t invoke_id);
int is_request_list_locked(void);
void lock_request_list(void);
void unlock_request_list(void);
void init_cmd_opts_from_list_cb(bacnet_client_cmd_opts *opts, llist_obj_data *obj);

void init_bacnet_client_whois_list(void);
void shutdown_bacnet_client_whois_list(void);
int is_bacnet_client_whois_present(void);
address_table_cb *get_bacnet_client_first_whois_data(void);
int add_bacnet_client_whois(BACNET_ADDRESS *dest, bacnet_client_cmd_opts *opts);
int del_bacnet_client_first_whois_request(void);
void whois_address_table_add(address_table_cb *table, uint32_t device_id,
  unsigned max_apdu, BACNET_ADDRESS *src);
void whois_address_table_add(address_table_cb *table, uint32_t device_id,
  unsigned max_apdu, BACNET_ADDRESS *src);
int encode_whois_result(llist_whois_cb *cb, char *buf, int buf_len);

void init_bacnet_client_pics_list(void);
void shutdown_bacnet_client_pics_list(void);
int is_bacnet_client_pics_present(uint8_t invoke_id);
int add_bacnet_client_pics(uint8_t invoke_id);
int del_bacnet_client_pics(uint8_t invoke_id);

void init_bacnet_client_point_disc_list(void);
void shutdown_bacnet_client_point_disc_list(void);
int add_bacnet_client_point_disc(uint32_t request_id, BACNET_ADDRESS *dest, bacnet_client_cmd_opts *opts);
llist_point_disc_cb *get_bacnet_client_point_disc_req(uint32_t request_id, BACNET_ADDRESS *src);
void process_point_disc_resp(uint8_t *service_request,
  uint16_t service_len, BACNET_ADDRESS *src, llist_point_disc_cb *pd_data);
int get_bacnet_client_point_disc_points(llist_point_disc_cb *pd_ptr);
int publish_bacnet_client_point_disc_result(llist_point_disc_cb *pd_cb);

void init_bacnet_client_points_info_list(void);
void shutdown_bacnet_client_points_info_list(void);
llist_points_info_cb *get_bacnet_client_points_info_req(uint32_t request_id, BACNET_ADDRESS *src);
llist_points_info_cb *add_bacnet_client_points_info(BACNET_ADDRESS *dest, bacnet_client_cmd_opts *opts);
int publish_bacnet_client_points_info_result(llist_points_info_cb *pi_cb);

void mqtt_on_send_success(void* context, MQTTAsync_successData* response);
void mqtt_on_send_failure(void* context, MQTTAsync_failureData* response);
void mqtt_on_connect(void* context, MQTTAsync_successData* response);
void mqtt_on_connect_failure(void* context, MQTTAsync_failureData* response);
void mqtt_on_disconnect(void* context, MQTTAsync_successData* response);
void mqtt_on_disconnect_failure(void* context, MQTTAsync_failureData* response);
void mqtt_on_subscribe(void* context, MQTTAsync_successData* response);
void mqtt_on_subscribe_failure(void* context, MQTTAsync_failureData* response);
int mqtt_publish_command_error(char *err_msg, bacnet_client_cmd_opts *opts, int topic_id);

int get_pics_target_address(bacnet_client_cmd_opts *opts);

int iam_decode_service_request(uint8_t *apdu,
  uint32_t *pDevice_id,
  unsigned *pMax_apdu,
  int *pSegmentation,
  uint16_t *pVendor_id);
bool bacnet_address_same(BACNET_ADDRESS *dest, BACNET_ADDRESS *src);

int is_reqquest_token_present(char *token);
void init_request_tokens(void);

int extract_char_string(char *buf, int buf_len, BACNET_APPLICATION_DATA_VALUE *value);

int restore_persistent_values(void* context);
int is_restore_persistent_done(void);


/* globals */
static int mqtt_debug = false;
static char mqtt_broker_ip[51] = {0};
static int mqtt_broker_port = DEFAULT_MQTT_BROKER_PORT;
static char mqtt_client_id[124] = {0};
static MQTTAsync mqtt_client = NULL;
static int mqtt_client_connected = false;
static bacnet_client_cmd_opts init_bacnet_client_cmd_opts = {-1, BACNET_MAX_INSTANCE, -1, BACNET_ARRAY_ALL, 0, BACNET_MAX_INSTANCE, -1, {0}, {0}, {0}, 0, {0}, -1, -1, 0, 0, 0, 0};

static llist_cb *bc_request_list_head = NULL;
static llist_cb *bc_request_list_tail = NULL;

static uint32_t bc_whois_request_ctr = 0;
static llist_whois_cb *bc_whois_head = NULL;
static llist_whois_cb *bc_whois_tail = NULL;

static llist_pics_cb *bc_pics_head = NULL;
static llist_pics_cb *bc_pics_tail = NULL;

static uint32_t bc_point_disc_request_ctr = 0;
static llist_point_disc_cb *bc_point_disc_head = NULL;
static llist_point_disc_cb *bc_point_disc_tail = NULL;

static uint32_t bc_points_info_request_ctr = 0;
static llist_points_info_cb *bc_points_info_head = NULL;
static llist_points_info_cb *bc_points_info_tail = NULL;

static int req_tokens_len = 0;
static char req_tokens[MAX_JSON_KEY_VALUE_PAIR][MAX_JSON_KEY_LENGTH] = {0};

static BACNET_ADDRESS pics_Target_Address = {0};
static uint32_t pics_Target_Device_Object_Instance = BACNET_MAX_INSTANCE;
static bool pics_Provided_Targ_MAC = false;
static bool Error_Detected = false;
static uint16_t Last_Error_Class = 0;
static uint16_t Last_Error_Code = 0;
static uint16_t Error_Count = 0;
static bool Has_RPM = true;
static EPICS_STATES myState = INITIAL_BINDING;
static uint8_t pics_Request_Invoke_ID = 0;

typedef struct BACnet_RPM_Service_Data_t {
    bool new_data;
    BACNET_CONFIRMED_SERVICE_ACK_DATA service_data;
    BACNET_READ_ACCESS_DATA *rpm_data;
} BACNET_RPM_SERVICE_DATA;
static BACNET_RPM_SERVICE_DATA pics_Read_Property_Multiple_Data;

#define MAX_PROPS 128
static uint32_t Property_List_Length = 0;
static uint32_t Property_List_Index = 0;
static int32_t Property_List[MAX_PROPS + 2];

struct property_value_list_t {
  int32_t property_id;
  BACNET_APPLICATION_DATA_VALUE *value;
};

static struct property_value_list_t Property_Value_List[] = {
  { PROP_VENDOR_NAME, NULL }, { PROP_MODEL_NAME, NULL },
  { PROP_MAX_APDU_LENGTH_ACCEPTED, NULL },
  { PROP_PROTOCOL_SERVICES_SUPPORTED, NULL },
  { PROP_PROTOCOL_OBJECT_TYPES_SUPPORTED, NULL }, { PROP_DESCRIPTION, NULL },
  { PROP_PRODUCT_NAME, NULL },
  { -1, NULL }
};

/* We get the length of the object list,
   and then get the objects one at a time */
static uint32_t Object_List_Length = 0;
static int32_t Object_List_Index = 0;
/* object that we are currently printing */
static OS_Keylist Object_List;

static uint32_t Walked_List_Length = 0;
static uint32_t Walked_List_Index = 0;
static bool Using_Walked_List = false;
static bool IsLongArray = false;
/* Show value instead of '?' */
static bool ShowValues = false;
/* show only device object properties */
static bool ShowDeviceObjectOnly = false;
static bool Optional_Properties = false;
static bool WriteResultToJsonFile = true;
static char JsonFilePath[251] = {0};
static int JsonFileFd = -1;
static bool FirstJsonItem = true;
static bool epics_pagination = true;
static bool epics_page_number = 0;
static int epics_page_size = EPICS_PAGE_SIZE_DEFAULT;
static int current_pics_object_type = -1;
static int pics_object_json_start = false;
static uint32_t total_device_objects;
int WriteKeyValueToJsonFile(char *key, char *value, int comma_end);
int WriteKeyValueNoNLToJsonFile(char *key, char *value, int comma_end);
int WriteCommaNLToJsonFile(void);
void StripDoubleQuotes(char *str);
void StripLastDoubleQuote(char *str);
bool is_object_type_supported(int object_type);
bool is_object_property_supported(int object_type, int property_id);

/* MQTT sub-system lock */
static pthread_mutex_t mqtt_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mqtt_msg_queue_mutex = PTHREAD_MUTEX_INITIALIZER;

/* crude request list locking mechanism */
static int request_list_locked = false;
int pics_request_locked = false;

/* persistent restore */
static int restore_persistent_done = false;


#define MQTT_MSG_CMD            1
#define MQTT_MSG_UNKNOWN        10

typedef struct _mqtt_msg_cb {
  int type;
  char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH];
  int topic_tokens_length;
  char topic_value[MAX_TOPIC_VALUE_LENGTH];
  bacnet_client_cmd_opts cmd_opts;
} mqtt_msg_cb;

typedef struct _mqtt_msg_queue_cb {
  mqtt_msg_cb *mqtt_msg;
  struct _mqtt_msg_queue_cb *next;
} mqtt_msg_queue_cb;

int mqtt_msg_queue_lock(void);
int mqtt_msg_queue_unlock(void);
int mqtt_msg_push(mqtt_msg_cb *msg);
mqtt_msg_cb *mqtt_msg_pop(void);

static mqtt_msg_queue_cb *mqtt_msg_head = NULL;
static mqtt_msg_queue_cb *mqtt_msg_tail = NULL;

static int enable_pics_filter_objects = true;

static int pics_retry_max = PICS_RETRY_MAX;
static int pics_retry_ctr = 0;


/*
 * Initialize mqtt message queue.
 */
void mqtt_msg_queue_init(void)
{
  mqtt_msg_head = mqtt_msg_tail = NULL;
}


/*
 * Shutdown mqtt message queue.
 */
void mqtt_msg_queue_shutdown(void)
{
  mqtt_msg_queue_cb *tmp, *ptr = mqtt_msg_head;

  while (ptr != NULL) {
    tmp = ptr;
    ptr = ptr->next;
    free(tmp);
  }
}


/*
 * Push mqtt message.
 */
int mqtt_msg_push(mqtt_msg_cb *msg)
{
  mqtt_msg_queue_cb *entry;

  entry = malloc(sizeof(mqtt_msg_queue_cb));
  if (!entry) {
    fprintf(stderr, "Failed to allocate memory for mqtt_msg_queue_cb.");
    return(1);
  }

  mqtt_msg_queue_lock();

  entry->mqtt_msg = msg;
  entry->next = NULL;

  if (!mqtt_msg_head) {
    mqtt_msg_head = mqtt_msg_tail = entry;
  } else {
    mqtt_msg_tail->next = entry;
    mqtt_msg_tail = entry;
  }

  mqtt_msg_queue_unlock();

  return(0);
}


/*
 * Pop mqtt message.
 */
mqtt_msg_cb *mqtt_msg_pop(void)
{
  mqtt_msg_queue_cb *entry;
  mqtt_msg_cb *msg = NULL;

  mqtt_msg_queue_lock();

  entry = mqtt_msg_head;

  if (entry) {
    msg = entry->mqtt_msg;
    mqtt_msg_head = entry->next;
    free(entry);
    if (!mqtt_msg_head) {
      mqtt_msg_tail = mqtt_msg_head;
    }
  }

  mqtt_msg_queue_unlock();

  return(msg);
}


/*
 * Count length of mqtt message queue.
 */
int mqtt_msg_length(void)
{
  mqtt_msg_queue_cb *ptr;
  int i = 0;

  mqtt_msg_queue_lock();
  for (i = 0, ptr = mqtt_msg_head; ptr != NULL; i++, ptr = ptr->next);
  mqtt_msg_queue_unlock();

  return(i);
}


/*
 * Device status code to string.
 */
static void device_status_to_str(BACNET_DEVICE_STATUS device_status, char *buf, int buf_len)
{
  switch (device_status) {
    case STATUS_OPERATIONAL:
      snprintf(buf, buf_len, "operational");
      break;

    case STATUS_OPERATIONAL_READ_ONLY:
      snprintf(buf, buf_len, "operational_read_only");
      break;

    case STATUS_DOWNLOAD_REQUIRED:
      snprintf(buf, buf_len, "download_required");
      break;

    case STATUS_DOWNLOAD_IN_PROGRESS:
      snprintf(buf, buf_len, "download_in_progress");
      break;

    case STATUS_NON_OPERATIONAL:
      snprintf(buf, buf_len, "non_operational");
      break;

    case STATUS_BACKUP_IN_PROGRESS:
      snprintf(buf, buf_len, "backup_in_progress");
      break;

    default:
      break;
  }
}


/*
 * MQTT subscribe connection lost callback.
 */
void mqtt_connection_lost(void *context, char *cause)
{ 
  if (mqtt_debug) {
     printf("WARNING: MQTT connection lost: %s\n", cause);
  }

  mqtt_client_connected = false;
}


/*
 * Tokenize topic tokens.
 */
int tokenize_topic(char *topic, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH])
{
  char *token, *ptr, *saveptr;
  int n_tokens;

  for (n_tokens = 0, ptr = topic; ; ptr = NULL) {
    token = strtok_r(ptr, "/", &saveptr);
    if (token == NULL) {
      break;
    }

    if (n_tokens >= MAX_TOPIC_TOKENS) {
      return(-1);
    }

    strncpy(topic_tokens[n_tokens], token, MAX_TOPIC_TOKEN_LENGTH - 1);
    topic_tokens[n_tokens][MAX_TOPIC_TOKEN_LENGTH - 1] = '\0';
    n_tokens++;
  }

  return(n_tokens);
}
    

/*
 * Dump topic tokens.
 */
void dump_topic_tokens(int n_topic_tokens, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH])
{
  printf("MQTT Topic Tokens:\n");
  for (int i = 0; i < n_topic_tokens; i++) {
    printf("- token[%d] => [%s]\n", i, topic_tokens[i]);
  }
}


/*
 * Process Analog Input (AI) write.
 */
int process_ai_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid)
{
  BACNET_CHARACTER_STRING bacnet_string;
  char *prop_name = topic_tokens[4];

  if (!strcasecmp(prop_name, "name")) {
    characterstring_init_ansi(&bacnet_string, value);
    Analog_Input_Set_Object_Name(index, &bacnet_string, uuid, false);
  } else if (!strcasecmp(prop_name, "pv")) {
    float f;
    char *endptr;

    f = strtof(value, &endptr);
    Analog_Input_Present_Value_Set(index, f, uuid, false);
  } else {
    if (mqtt_debug) {
      printf("MQTT Invalid Property for Analog Input (AI): [%s]\n", prop_name);
    }

    return(1);
  }

  return(0);
}


/*
 * Set Analog Input (AI) property persistent value.
 */
int set_ai_property_persistent_value(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value)
{
  BACNET_CHARACTER_STRING bacnet_string;
  char *prop_name = topic_tokens[3];

  if (mqtt_debug) {
    printf("Setting AI property persistent value: %s/%d\n", prop_name, index);
  }

  if (!strcasecmp(prop_name, "name")) {
    characterstring_init_ansi(&bacnet_string, value);
    Analog_Input_Set_Object_Name(index, &bacnet_string, NULL, true);
  } else if (!strcasecmp(prop_name, "pv")) {
    float f;
    char *endptr;

    f = strtof(value, &endptr);
    Analog_Input_Present_Value_Set(index, f, NULL, true);
  } else {
    if (mqtt_debug) {
      printf("MQTT Invalid persistent Property for Analog Input (AI): [%s]\n", prop_name);
    }

    return(1);
  }

  return(0);
}


/*
 * Process Analog Output (AO) write.
 */
int process_ao_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid)
{ 
  BACNET_CHARACTER_STRING bacnet_string;
  float f;
  char *prop_name = topic_tokens[4];
  char *endptr;
  
  if (!strcasecmp(prop_name, "name")) {
    characterstring_init_ansi(&bacnet_string, value);
    Analog_Output_Set_Object_Name(index, &bacnet_string, uuid, false);
  } else if (!strcasecmp(prop_name, "pv")) {
    f = (!strcasecmp(value, "null")) ? 255 :  strtof(value, &endptr);
    Analog_Output_Present_Value_Set(index, f, BACNET_MAX_PRIORITY, uuid, false);
  } else if (!strcasecmp(prop_name, "pri")) {
    int pri_idx = atoi(topic_tokens[5]);
    int all_items = false;

    if (mqtt_debug) {
      printf("- pri_idx: [%d]\n", pri_idx);
    }

    if ((pri_idx < 1) || (pri_idx > BACNET_MAX_PRIORITY)) {
      if (mqtt_debug) {
        printf("MQTT Invalid Priority Array index for Analog Output (AO): [%d]\n", pri_idx);
      }
      return(1);
    }

    if (strlen(topic_tokens[6]) > 0) {
      if (strcasecmp(topic_tokens[6], "all")) {
        if (mqtt_debug) {
          printf("MQTT Invalid Priority Array value attribute for Analog Output (AO): [%s]\n", topic_tokens[6]);
        }

        return(1);
      }

      all_items = true;
    }

    f = (!strcasecmp(value, "null")) ? 255 :  strtof(value, &endptr);
    if (all_items) {
      for (int i = 1; i <= pri_idx; i++) {
        if (i != pri_idx) {
          Analog_Output_Priority_Array_Set2(index, f, i);
        } else {
          Analog_Output_Priority_Array_Set(index, f, i, uuid, false);
        }
      }
    } else {
      Analog_Output_Priority_Array_Set(index, f, pri_idx, uuid, false);
    }
  } else {
    if (mqtt_debug) {
      printf("MQTT Invalid Property for Analog Output (AO): [%s]\n", prop_name);
    }
  
    return(1);
  }

  return(0);
}


/*
 * Process Analog Output (AO) property persistent value.
 */
int set_ao_property_persistent_value(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, json_object *json_field)
{
  BACNET_CHARACTER_STRING bacnet_string;
  float f;
  char array_value_buf[MAX_JSON_VALUE_LENGTH];
  char *prop_name = topic_tokens[3];
  char *endptr;
  int rc = 1;

  if (mqtt_debug) {
    printf("Setting AO property persistent value: %s/%d\n", prop_name, index);
  }

  if (!strcasecmp(prop_name, "name")) {
    characterstring_init_ansi(&bacnet_string, value);
    Analog_Output_Set_Object_Name(index, &bacnet_string, NULL, true);
  } else if (!strcasecmp(prop_name, "pv")) {
    f = (!strcasecmp(value, "null")) ? 255 :  strtof(value, &endptr);
    Analog_Output_Present_Value_Set(index, f, BACNET_MAX_PRIORITY, NULL, true);
  } else if (!strcasecmp(prop_name, "pri")) {
    json_object *obj;
    int arr_len = json_object_array_length(json_field);
    for (int i = 0; i < arr_len; i++) {
      obj = json_object_array_get_idx(json_field, i);
      if (!obj) {
        continue;
      }

      strncpy(array_value_buf, json_object_get_string(obj), sizeof(array_value_buf) - 1);
      if (mqtt_debug) {
        printf("- index[%d] : [%s]\n", i, array_value_buf);
      }

      f = (!strcasecmp(array_value_buf, "null")) ? 255 :  strtof(array_value_buf, &endptr);
      Analog_Output_Priority_Array_Set2(index, f, (i + 1));
    }
  } else {
    if (mqtt_debug) {
      printf("MQTT Invalid persistent Property for Analog Output (AO): [%s]\n", prop_name);
    }

    goto EXIT;
  }

  rc = 0;
  EXIT:

  return(rc);
}


/*
 * Process Analog Value (AV) write.
 */
int process_av_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid)
{
  BACNET_CHARACTER_STRING bacnet_string;
  float f;
  char *prop_name = topic_tokens[4];
  char *endptr;

  if (!strcasecmp(prop_name, "name")) {
    characterstring_init_ansi(&bacnet_string, value);
    Analog_Value_Set_Object_Name(index, &bacnet_string, uuid, false);
  } else if (!strcasecmp(prop_name, "pv")) {
    f = (!strcasecmp(value, "null")) ? 255 :  strtof(value, &endptr);
    Analog_Value_Present_Value_Set(index, f, BACNET_MAX_PRIORITY, uuid, false);
  } else if (!strcasecmp(prop_name, "pri")) {
    int pri_idx = atoi(topic_tokens[5]);
    int all_items = false;

    if (mqtt_debug) {
      printf("- pri_idx: [%d]\n", pri_idx);
    }

    if ((pri_idx < 1) || (pri_idx > BACNET_MAX_PRIORITY)) {
      if (mqtt_debug) {
        printf("MQTT Invalid Priority Array index for Analog Value (AV): [%d]\n", pri_idx);
      }
      return(1);
    }

    if (strlen(topic_tokens[6]) > 0) {
      if (strcasecmp(topic_tokens[6], "all")) {
        if (mqtt_debug) {
          printf("MQTT Invalid Priority Array value attribute for Analog Value (AV): [%s]\n", topic_tokens[6]);
        }

        return(1);
      }

      all_items = true;
    }

    f = (!strcasecmp(value, "null")) ? 255 :  strtof(value, &endptr);
    if (all_items) {
      for (int i = 1; i <= pri_idx; i++) {
        if (i != pri_idx) {
          Analog_Value_Priority_Array_Set2(index, f, i);
        } else {
          Analog_Value_Priority_Array_Set(index, f, i, uuid);
        }
      }
    } else {
      Analog_Value_Priority_Array_Set(index, f, pri_idx, uuid);
    }
  } else {
    if (mqtt_debug) {
      printf("MQTT Invalid Property for Analog Value (AV): [%s]\n", prop_name);
    }

    return(1);
  }

  return(0);
}


/*
 * Process Analog Value (AV) property persistent value.
 */
int set_av_property_persistent_value(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, json_object *json_field)
{
  BACNET_CHARACTER_STRING bacnet_string;
  float f;
  char array_value_buf[MAX_JSON_VALUE_LENGTH];
  char *prop_name = topic_tokens[3];
  char *endptr;
  int rc = 1;

  if (mqtt_debug) {
    printf("Setting AV property persistent value: %s/%d\n", prop_name, index);
  }

  if (!strcasecmp(prop_name, "name")) {
    characterstring_init_ansi(&bacnet_string, value);
    Analog_Value_Set_Object_Name(index, &bacnet_string, NULL, true);
  } else if (!strcasecmp(prop_name, "pv")) {
    f = (!strcasecmp(value, "null")) ? 255 :  strtof(value, &endptr);
    Analog_Value_Present_Value_Set(index, f, BACNET_MAX_PRIORITY, NULL, true);
  } else if (!strcasecmp(prop_name, "pri")) {
    json_object *obj;
    int arr_len = json_object_array_length(json_field);
    for (int i = 0; i < arr_len; i++) {
      obj = json_object_array_get_idx(json_field, i);
      if (!obj) {
        continue;
      }

      strncpy(array_value_buf, json_object_get_string(obj), sizeof(array_value_buf) - 1);
      if (mqtt_debug) {
        printf("- index[%d] : [%s]\n", i, array_value_buf);
      }

      f = (!strcasecmp(array_value_buf, "null")) ? 255 :  strtof(array_value_buf, &endptr);
      Analog_Value_Priority_Array_Set2(index, f, (i + 1));
    }
  } else {
    if (mqtt_debug) {
      printf("MQTT Invalid persistent Property for Analog Output (AO): [%s]\n", prop_name);
    }

    goto EXIT;
  }

  rc = 0;
  EXIT:

  return(rc);
}


/*
 * Process Binary Input (AI) write.
 */
int process_bi_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid)
{
  BACNET_CHARACTER_STRING bacnet_string;
  char *prop_name = topic_tokens[4];

  if (!strcasecmp(prop_name, "name")) {
    characterstring_init_ansi(&bacnet_string, value);
    Binary_Input_Set_Object_Name(index, &bacnet_string, uuid, false);
  } else if (!strcasecmp(prop_name, "pv")) {
    BACNET_BINARY_PV pv;
    int i_val = atoi(value);
    pv = (i_val == 0) ? 0 : 1;
    Binary_Input_Present_Value_Set(index, pv, uuid, false);
  } else {
    if (mqtt_debug) {
      printf("MQTT Invalid Property for Binary Input (BI): [%s]\n", prop_name);
    }

    return(1);
  }

  return(0);
}


/*    
 * Process Binary Input (AI) property persistent value.
 */   
int set_bi_property_persistent_value(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value)
{
  BACNET_CHARACTER_STRING bacnet_string;
  char *prop_name = topic_tokens[3];

  if (mqtt_debug) {
    printf("Setting BI property persistent value: %s/%d\n", prop_name, index);
  }

  if (!strcasecmp(prop_name, "name")) {
    characterstring_init_ansi(&bacnet_string, value);
    Binary_Input_Set_Object_Name(index, &bacnet_string, NULL, true);
  } else if (!strcasecmp(prop_name, "pv")) {
    BACNET_BINARY_PV pv;
    int i_val = atoi(value);
    pv = (i_val == 0) ? 0 : 1;
    Binary_Input_Present_Value_Set(index, pv, NULL, true);
  } else {
    if (mqtt_debug) {
      printf("MQTT Invalid persistent Property for Binary Input (BI): [%s]\n", prop_name);
    }

    return(1);
  }   

  return(0);
}


/*
 * Process Binary Output (AO) write.
 */
int process_bo_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid)
{
  BACNET_BINARY_PV pv;
  BACNET_CHARACTER_STRING bacnet_string;
  char *prop_name = topic_tokens[4];
  int i_val = atoi(value);

  if (!strcasecmp(prop_name, "name")) {
    characterstring_init_ansi(&bacnet_string, value);
    Binary_Output_Set_Object_Name(index, &bacnet_string, uuid, false);
  } else if (!strcasecmp(prop_name, "pv")) {
    pv = (!strcasecmp(value, "null")) ? 255 : (i_val == 0) ? 0 : 1;
    Binary_Output_Present_Value_Set(index, pv, BACNET_MAX_PRIORITY, uuid, false);
  } else if (!strcasecmp(prop_name, "pri")) {
    int pri_idx = atoi(topic_tokens[5]);
    int all_items = false;

    if (mqtt_debug) {
      printf("- pri_idx: [%d]\n", pri_idx);
    }

    if ((pri_idx < 1) || (pri_idx > BACNET_MAX_PRIORITY)) {
      if (mqtt_debug) {
        printf("MQTT Invalid Priority Array index for Binary Output (BO): [%d]\n", pri_idx);
      }
      return(1);
    }

    if (strlen(topic_tokens[6]) > 0) {
      if (strcasecmp(topic_tokens[6], "all")) {
        if (mqtt_debug) {
          printf("MQTT Invalid Priority Array value attribute for Binary Output (BO): [%s]\n", topic_tokens[6]);
        }

        return(1);
      }

      all_items = true;
    }

    pv = (!strcasecmp(value, "null")) ? 255 : (i_val == 0) ? 0 : 1;
    if (all_items) {
      for (int i = 1; i <= pri_idx; i++) {
        if (i != pri_idx) {
          Binary_Output_Priority_Array_Set2(index, pv, i);
        } else {
          Binary_Output_Priority_Array_Set(index, pv, i, uuid);
        }
      }
    } else {
      Binary_Output_Priority_Array_Set(index, pv, pri_idx, uuid);
    }
  } else {
    if (mqtt_debug) {
      printf("MQTT Invalid Property for Binary Output (BO): [%s]\n", prop_name);
    }

    return(1);
  }

  return(0);
}


/*
 * Process Binary Output (AO) property persistent value.
 */
int set_bo_property_persistent_value(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, json_object *json_field)
{
  BACNET_CHARACTER_STRING bacnet_string;
  BACNET_BINARY_PV pv;
  char array_value_buf[MAX_JSON_VALUE_LENGTH];
  char *prop_name = topic_tokens[3];
  int i_val;
  int rc = 1;
 
  if (mqtt_debug) {
    printf("Setting BO property persistent value: %s/%d\n", prop_name, index);
  }     
 
  if (!strcasecmp(prop_name, "name")) {
    characterstring_init_ansi(&bacnet_string, value);
    Binary_Output_Set_Object_Name(index, &bacnet_string, NULL, true);
  } else if (!strcasecmp(prop_name, "pv")) {
    i_val = atoi(value);
    pv = (!strcasecmp(value, "null")) ? 255 : (i_val == 0) ? 0 : 1;
    Binary_Output_Present_Value_Set(index, pv, BACNET_MAX_PRIORITY, NULL, true);
  } else if (!strcasecmp(prop_name, "pri")) { 
    json_object *obj;
    int arr_len = json_object_array_length(json_field);
    for (int i = 0; i < arr_len; i++) {
      obj = json_object_array_get_idx(json_field, i);
      if (!obj) {
        continue;
      }

      strncpy(array_value_buf, json_object_get_string(obj), sizeof(array_value_buf) - 1);
      if (mqtt_debug) {
        printf("- index[%d] : [%s]\n", i, array_value_buf);
      }

      i_val = atoi(array_value_buf);
      pv = (!strcasecmp(array_value_buf, "null")) ? 255 : (i_val == 0) ? 0 : 1;
      Binary_Output_Priority_Array_Set2(index, pv, (i + 1));
    }
  } else {
    if (mqtt_debug) {
      printf("MQTT Invalid persistent Property for Binary Output (AO): [%s]\n", prop_name);
    }

    goto EXIT;
  }

  rc = 0;
  EXIT:

  return(rc);
}


/*
 * Process Binary Value (BV) write.
 */
int process_bv_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid)
{
  BACNET_BINARY_PV pv;
  BACNET_CHARACTER_STRING bacnet_string;
  char *prop_name = topic_tokens[4];
  int i_val = atoi(value);

  if (!strcasecmp(prop_name, "name")) {
    characterstring_init_ansi(&bacnet_string, value);
    Binary_Value_Set_Object_Name(index, &bacnet_string, uuid, false);
  } else if (!strcasecmp(prop_name, "pv")) {
    pv = (!strcasecmp(value, "null")) ? 255 : (i_val == 0) ? 0 : 1;
    Binary_Value_Present_Value_Set(index, pv, BACNET_MAX_PRIORITY, uuid, false);
  } else if (!strcasecmp(prop_name, "pri")) {
    int pri_idx = atoi(topic_tokens[5]);
    int all_items = false;

    if (mqtt_debug) {
      printf("- pri_idx: [%d]\n", pri_idx);
    }

    if ((pri_idx < 1) || (pri_idx > BACNET_MAX_PRIORITY)) {
      if (mqtt_debug) {
        printf("MQTT Invalid Priority Array index for Binary Value (BV): [%d]\n", pri_idx);
      }
      return(1);
    }

    if (strlen(topic_tokens[6]) > 0) {
      if (strcasecmp(topic_tokens[6], "all")) {
        if (mqtt_debug) {
          printf("MQTT Invalid Priority Array value attribute for Binary Value (BV): [%s]\n", topic_tokens[6]);
        }

        return(1);
      }

      all_items = true;
    }

    pv = (!strcasecmp(value, "null")) ? 255 : (i_val == 0) ? 0 : 1;
    if (all_items) {
      for (int i = 1; i <= pri_idx; i++) {
        if (i != pri_idx) {
          Binary_Value_Priority_Array_Set2(index, pv, i);
        } else {
          Binary_Value_Priority_Array_Set(index, pv, i, uuid);
        }
      }
    } else {
      Binary_Value_Priority_Array_Set(index, pv, pri_idx, uuid);
    }
  } else {
    if (mqtt_debug) {
      printf("MQTT Invalid Property for Binary Value (BV): [%s]\n", prop_name);
    }

    return(1);
  }

  return(0);
}


/*
 * Process Binary Value (BV) property persistent value.
 */
int set_bv_property_persistent_value(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, json_object *json_field)
{
  BACNET_CHARACTER_STRING bacnet_string;
  BACNET_BINARY_PV pv;
  char array_value_buf[MAX_JSON_VALUE_LENGTH];
  char *prop_name = topic_tokens[3];
  int i_val;
  int rc = 1;

  if (mqtt_debug) {
    printf("Setting BV property persistent value: %s/%d\n", prop_name, index);
  }

  if (!strcasecmp(prop_name, "name")) {
    characterstring_init_ansi(&bacnet_string, value);
    Binary_Value_Set_Object_Name(index, &bacnet_string, NULL, true);
  } else if (!strcasecmp(prop_name, "pv")) {
    i_val = atoi(value);
    pv = (!strcasecmp(value, "null")) ? 255 : (i_val == 0) ? 0 : 1;
    Binary_Value_Present_Value_Set(index, pv, BACNET_MAX_PRIORITY, NULL, true);
  } else if (!strcasecmp(prop_name, "pri")) {
    json_object *obj;
    int arr_len = json_object_array_length(json_field);
    for (int i = 0; i < arr_len; i++) {
      obj = json_object_array_get_idx(json_field, i);
      if (!obj) {
        continue;
      }

      strncpy(array_value_buf, json_object_get_string(obj), sizeof(array_value_buf) - 1);
      if (mqtt_debug) {
        printf("- index[%d] : [%s]\n", i, array_value_buf);
      }

      i_val = atoi(array_value_buf);
      pv = (!strcasecmp(array_value_buf, "null")) ? 255 : (i_val == 0) ? 0 : 1;
      Binary_Value_Priority_Array_Set2(index, pv, (i + 1));
    }
  } else {
    if (mqtt_debug) {
      printf("MQTT Invalid persistent Property for Binary Value (BV): [%s]\n", prop_name);
    }

    goto EXIT;
  }

  rc = 0;
  EXIT:

  return(rc);
}


/*
 * Process Bacnet client Whois command.
 */
int process_bacnet_client_whois_command(bacnet_client_cmd_opts *opts)
{
  BACNET_ADDRESS src = { 0 };
  BACNET_MAC_ADDRESS mac = { 0 };
  BACNET_ADDRESS dest = { 0 };
  BACNET_MAC_ADDRESS adr = { 0 };
  long dnet = -1;
  uint16_t pdu_len;
  uint8_t Rx_Buf[MAX_MPDU] = { 0 };
  bool global_broadcast = true;
  unsigned delay_milliseconds = 120;

  if (strlen(opts->mac)) {
    if (address_mac_from_ascii(&mac, opts->mac)) {
      global_broadcast = false;
    }
  }

  if (opts->dnet >= 0 && (opts->dnet <= BACNET_BROADCAST_NETWORK)) {
    dnet = opts->dnet;
    global_broadcast = false;
  }

  if (strlen(opts->dadr)) {
    if (address_mac_from_ascii(&adr, opts->dadr)) {
      global_broadcast = false;
    }
  }

  if (global_broadcast) {
    datalink_get_broadcast_address(&dest);
  } else {
    if (adr.len && mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      memcpy(&dest.adr[0], &adr.adr[0], adr.len);
      dest.len = adr.len;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = BACNET_BROADCAST_NETWORK;
      }
    } else if (mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      dest.len = 0;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = 0;
      }
    } else {
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = BACNET_BROADCAST_NETWORK;
      }
      dest.mac_len = 0;
      dest.len = 0;
    }
  }

  if (opts->device_instance_max < 0) {
    opts->device_instance_max = opts->device_instance_min;
  }

  if (opts->device_instance_min > BACNET_MAX_INSTANCE) {
    printf("Mininum Device-Instance exceeded limit!\n");
    return(1);
  }

  if (opts->device_instance_max > BACNET_MAX_INSTANCE) {
    printf("Maximum Device-Instance exceeded limit!\n");
    return(1);
  }

  if (opts->timeout == 0) {
    opts->timeout = BACNET_CLIENT_WHOIS_TIMEOUT;
  }

  add_bacnet_client_whois(&dest, opts);

  Send_WhoIs_To_Network(
    &dest, opts->device_instance_min, opts->device_instance_max);

  pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU,
    delay_milliseconds);
  /* process */
  if (pdu_len) {
    npdu_handler(&src, &Rx_Buf[0], pdu_len);
  }
  return(0);
}


void mqtt_on_send_success(void* context, MQTTAsync_successData* response)
{
  printf("Message with token value %d delivery confirmed\n", response->token);
}


void mqtt_on_send_failure(void* context, MQTTAsync_failureData* response)
{
  printf("Message send failed token %d error code %d\n", response->token, response->code);
}


/*
 * Publish Bacnet client topic.
 */
int mqtt_publish_command_result(int object_type, int object_instance, int property_id, int priority,
  int vtype, void *vptr, int topic_id, json_key_value_pair *kvps, int kvps_length)
{
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
  BACNET_BINARY_PV b_val;
  json_key_value_pair *kvp;
  char topic[512];
  char topic_value[30000];
  char buf[2048];
  char *first = "";
  float f_val;
  int mqtt_retry_limit = yaml_config_mqtt_connect_retry_limit();
  int mqtt_retry_interval = yaml_config_mqtt_connect_retry_interval();
  int rc, i;

  if (!mqtt_create_topic(object_type, object_instance, property_id, topic, sizeof(topic), topic_id)) {
    printf("Failed to create MQTT topic: %d/%d\n", object_type, property_id);
    return(1);
  }

  if (mqtt_debug) {
    printf("MQTT publish topic: %s\n", topic);
  }

  opts.onSuccess = mqtt_on_send_success;
  opts.onFailure = mqtt_on_send_failure;
  opts.context = mqtt_client;

  switch(vtype) {
    case MQTT_TOPIC_VALUE_STRING:
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" ", (char*)vptr);
      break;

    case MQTT_TOPIC_VALUE_BACNET_STRING:
      characterstring_ansi_copy(buf, sizeof(buf), vptr);
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" ", buf);
      break;

    case MQTT_TOPIC_VALUE_INTEGER:
      sprintf(buf, "%d", *((int*)vptr));
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" ", buf);
      break;

    case MQTT_TOPIC_VALUE_FLOAT:
      sprintf(buf, "%.6f", *((float*)vptr));
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" ", buf);
      break;

    case MQTT_TOPIC_VALUE_FLOAT_MAX_PRIO:
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : [");
      for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
        f_val = ((float*)vptr)[i];
        if (f_val == AO_LEVEL_NULL) {
          sprintf(&topic_value[strlen(topic_value)], "%s\"Null\"", first);
        } else {
          sprintf(&topic_value[strlen(topic_value)], "%s\"%.6f\"", first, f_val);
        }

        first = ",";
      }
      snprintf(&topic_value[strlen(topic_value)],  sizeof(topic_value) - strlen(topic_value) , "]");
      break;

    case MQTT_TOPIC_VALUE_BINARY_MAX_PRIO:
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : [");
      for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
        b_val = ((BACNET_BINARY_PV *)vptr)[i];
        if (b_val == BINARY_NULL) {
          sprintf(&topic_value[strlen(topic_value)], "%s\"Null\"", first);
        } else {
          sprintf(&topic_value[strlen(topic_value)], "%s\"%d\"", first, b_val);
        }

        first = ",";
      }
      snprintf(&topic_value[strlen(topic_value)],  sizeof(topic_value) - strlen(topic_value) , "]");
      break;

    case MQTT_TOPIC_VALUE_OBJECT_LIST:
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : %s ", (char*)vptr);
      break;

    case MQTT_TOPIC_VALUE_FLOAT_PRIO_ARRAY:
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : {");
      for (i = 0, kvp = (json_key_value_pair *)vptr; kvp->index > 0; kvp++, i++) {
        snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value),
          "%s\"%s\" : \"%s\"", (i == 0) ? "" : " , ", kvp->key,
          (strcmp(kvp->value, "255")) ? kvp->value : "Null");
      }
      snprintf(&topic_value[strlen(topic_value)],  sizeof(topic_value) - strlen(topic_value) , "}");
      break;

    case MQTT_TOPIC_VALUE_BINARY_PRIO_ARRAY:
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : {");
      for (i = 0, kvp = (json_key_value_pair *)vptr; kvp->index > 0; kvp++, i++) {
        snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value),
          "%s\"%s\" : \"%s\"", (i == 0) ? "" : " , ", kvp->key,
          (strcmp(kvp->value, "255")) ? kvp->value : "Null");
      }
      snprintf(&topic_value[strlen(topic_value)],  sizeof(topic_value) - strlen(topic_value) , "}");
      break;

    default:
      printf("MQTT unsupported topic value: %d\n", vtype);
      return(1);
  }

  if (priority > 0) {
    snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"priority\" : \"%d\" ",
      priority);
  }

  for (i = 0; i < kvps_length; i++) {
    snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"%s\" : \"%s\" ",
      kvps[i].key, kvps[i].value);
  }

  snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), "}");
  pubmsg.payload = topic_value;
  pubmsg.payloadlen = strlen(topic_value);

  if (yaml_config_mqtt_connect_retry() && mqtt_retry_limit > 0) {
    for (i = 0; i < mqtt_retry_limit && !mqtt_client_connected; i++) {
      if (mqtt_debug) {
        printf("MQTT reconnect retry: %d/%d\n", (i + 1), mqtt_retry_limit);
      }

      mqtt_check_reconnect();
      sleep(mqtt_retry_interval);
    }

    if (i >= mqtt_retry_limit) {
      if (mqtt_debug) {
        printf("MQTT reconnect limit reached: %d\n", i);
      }

      return(1);
    }
  }

  pubmsg.qos = DEFAULT_PUB_QOS;
  pubmsg.retained = 0;
  rc = MQTTAsync_sendMessage(mqtt_client, topic, &pubmsg, &opts);
  if (mqtt_debug) {
    if (rc != MQTTASYNC_SUCCESS) {
      printf("MQTT failed to publish topic: \"%s\" , return code:%d\n", topic, rc);
    } else {
      printf("MQTT published topic: \"%s\"\n", topic);
    }
  }

  return(0);
}


int is_address_us(bacnet_client_cmd_opts *opts)
{
  BACNET_IP_ADDRESS addr = {0};
  BACNET_MAC_ADDRESS mac = {0};
  BACNET_ADDRESS b_addr = {0};

  if (opts->mac) {
    address_mac_from_ascii(&mac, opts->mac);
    memcpy(&b_addr.mac[0], &mac.adr[0], mac.len);
    b_addr.mac_len = mac.len;
    b_addr.len = 0;
    bvlc_ip_address_from_bacnet_local(&addr, &b_addr);

    if (bbmd_address_match_self(&addr)) {
      return(true);
    }
  }

  return(false);
}


/*
 * Return true if the device instance ID in the Bacnet client command is the same
 * as the device ID.
 */
int is_command_for_us(bacnet_client_cmd_opts *opts)
{
  if (Device_Object_Instance_Number() == opts->device_instance &&
    is_address_us(opts)) {
    return(true);
  }

  return(false);
}


char *get_object_type_str(int object_type)
{
  static char strbuf[15];
  char *str = NULL;

  switch(object_type) {
    case OBJECT_ANALOG_INPUT:
      str = "ai";
      break;

    case OBJECT_ANALOG_OUTPUT:
      str = "ao";
      break;

    case OBJECT_ANALOG_VALUE:
      str = "av";
      break;

    case OBJECT_BINARY_INPUT:
      str = "bi";
      break;

    case OBJECT_BINARY_OUTPUT:
      str = "bo";
      break;

    case OBJECT_BINARY_VALUE:
      str = "bv";
      break;

    case OBJECT_CALENDAR:
      str = "cal";
      break;

    case OBJECT_COMMAND:
      str = "com";
      break;

    case OBJECT_DEVICE:
      str = "device";
      break;

    case OBJECT_EVENT_ENROLLMENT:
      str = "ee";
      break;

    case OBJECT_FILE:
      str = "file";
      break;

    case OBJECT_GROUP:
      str = "group";
      break;

    case OBJECT_LOOP:
      str = "loop";
      break;

    case OBJECT_MULTI_STATE_INPUT:
      str = "msi";
      break;

    case OBJECT_MULTI_STATE_OUTPUT:
      str = "mso";
      break;

    case OBJECT_NOTIFICATION_CLASS:
      str = "noti";
      break;

    case OBJECT_PROGRAM:
      str = "prog";
      break;

    case OBJECT_SCHEDULE:
      str = "sched";
      break;

    case OBJECT_AVERAGING:
      str = "ave";
      break;

    case OBJECT_MULTI_STATE_VALUE:
      str = "msv";
      break;

    case OBJECT_TRENDLOG:
      str = "tlog";
      break;

    case OBJECT_LIFE_SAFETY_POINT:
      str = "lsp";
      break;

    case OBJECT_LIFE_SAFETY_ZONE:
      str = "lsz";
      break;

    case OBJECT_ACCUMULATOR:
      str = "accu";
      break;

    case OBJECT_PULSE_CONVERTER:
      str = "pcon";
      break;

    case OBJECT_EVENT_LOG:
      str = "elog";
      break;

    case OBJECT_GLOBAL_GROUP:
      str = "gg";
      break;

    case OBJECT_TREND_LOG_MULTIPLE:
      str = "tlm";
      break;

    case OBJECT_LOAD_CONTROL:
      str = "lc";
      break;

    case OBJECT_STRUCTURED_VIEW:
      str = "sv";
      break;

    case OBJECT_ACCESS_DOOR:
      str = "ad";
      break;

    case OBJECT_TIMER:
      str = "timer";
      break;

    case OBJECT_ACCESS_CREDENTIAL:
      str = "acred";
      break;

    case OBJECT_ACCESS_POINT:
      str = "ap";
      break;

    case OBJECT_ACCESS_RIGHTS:
      str = "ar";
      break;

    case OBJECT_ACCESS_USER:
      str = "au";
      break;

    case OBJECT_ACCESS_ZONE:
      str = "az";
      break;

    case OBJECT_CREDENTIAL_DATA_INPUT:
      str = "cdi";
      break;

    default:
      sprintf(strbuf, "%d", object_type);
      str = &strbuf[0];
      break;
  }

  return(str);
}


bool get_object_type_id(char *name, uint32_t *id)
{
  bool rc = true;

  if (!strcasecmp(name, "ai")) {
    *id = OBJECT_ANALOG_INPUT;
  } else if (!strcasecmp(name, "ao")) {
    *id = OBJECT_ANALOG_OUTPUT;
  } else if (!strcasecmp(name, "av")) {
    *id = OBJECT_ANALOG_VALUE;
  } else if (!strcasecmp(name, "bi")) {
    *id = OBJECT_BINARY_INPUT;
  } else if (!strcasecmp(name, "bo")) {
    *id = OBJECT_BINARY_OUTPUT;
  } else if (!strcasecmp(name, "bv")) { 
    *id = OBJECT_BINARY_VALUE;
  } else if (!strcasecmp(name, "device")) {
    *id = OBJECT_DEVICE;
  } else if (!strcasecmp(name, "msi")) {
    *id = OBJECT_MULTI_STATE_INPUT;
  } else if (!strcasecmp(name, "mso")) {
    *id = OBJECT_MULTI_STATE_OUTPUT;
  } else if (!strcasecmp(name, "msv")) {
    *id = OBJECT_MULTI_STATE_VALUE;
  } else {
    rc = false;
  }

  return(rc);
}


char *get_object_property_str(int object_property)
{
  static char strbuf[15];
  char *str = NULL;

  switch(object_property) {
    case PROP_ACKED_TRANSITIONS:
      str = "ack_trans";
      break;

    case PROP_ACK_REQUIRED:
      str = "ack_req";
      break;

    case PROP_ACTION:
      str = "act";
      break;

    case PROP_ACTION_TEXT:
      str = "actn_text";
      break;

    case PROP_ACTIVE_TEXT:
      str = "actv_text";
      break;

    case PROP_ACTIVE_VT_SESSIONS:
      str = "avs";
      break;

    case PROP_ALARM_VALUE:
      str = "av";
      break;

    case PROP_ALARM_VALUES:
      str = "avs";
      break;

    case PROP_ALL:
      str = "all";
      break;

    case PROP_ALL_WRITES_SUCCESSFUL:
      str = "aws";
      break;

    case PROP_APDU_SEGMENT_TIMEOUT:
      str = "ast";
      break;

    case PROP_APDU_TIMEOUT:
      str = "at";
      break;

    case PROP_APPLICATION_SOFTWARE_VERSION:
      str = "asv";
      break;

    case PROP_PRESENT_VALUE:
      str = "pv";
      break;

    case PROP_OBJECT_NAME:
      str = "name";
      break;

    case PROP_PRIORITY_ARRAY:
      str = "pri";
      break;

    case PROP_OBJECT_IDENTIFIER:
      str = "id";
      break;

    case PROP_SYSTEM_STATUS:
      str = "system_status";
      break;

    case PROP_VENDOR_NAME:
      str = "vendor_name";
      break;

    case PROP_VENDOR_IDENTIFIER:
      str = "vendor_id";
      break;

    case PROP_OBJECT_LIST:
      str = "object_list";
      break;

    default:
      sprintf(strbuf, "%d", object_property);
      str = &strbuf[0];
      break;
  }

  return(str);
}


/*
 * Encode array value results.
 */
void encode_array_value_result(BACNET_READ_PROPERTY_DATA *data, llist_obj_data *obj_data, char *buf, int buf_len)
{
  BACNET_OBJECT_PROPERTY_VALUE object_value;
  BACNET_APPLICATION_DATA_VALUE value;
  uint8_t *application_data;
  char array_val[32][50];
  int array_val_len = 0;
  int application_data_len;
  int len;
  int i;

  application_data = data->application_data;
  application_data_len = data->application_data_len;

  for (;;) {
    len = bacapp_decode_application_data(
      application_data, (unsigned)application_data_len, &value);

    object_value.object_type = data->object_type;
    object_value.object_instance = data->object_instance;
    object_value.object_property = data->object_property;
    object_value.array_index = data->array_index;
    object_value.value = &value;
    bacapp_snprintf_value(array_val[array_val_len], 50, &object_value);
    array_val_len++;

    if (len > 0) {
      if (len < application_data_len) {
        application_data += len;
        application_data_len -= len;

      } else {
        break;
      }
    } else {
      break;
    }
  }

  if (obj_data->index == BACNET_ARRAY_ALL) {
    sprintf(buf, "[");
    for(i = 0; i < array_val_len; i++) {
      snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "%s\"%s\"",
        (i == 0) ? "" : ",", array_val[i]);
    }
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "]");
  } else if (obj_data->index >= 0) {
    snprintf(buf, buf_len, "%s", array_val[0]);
  }
}


/*  
 * Encode read multiple array value results.
 */     
void encode_read_multiple_array_value_result(BACNET_READ_ACCESS_DATA *rpm_data, char *buf, int buf_len)
{
  BACNET_OBJECT_PROPERTY_VALUE object_value;
  BACNET_PROPERTY_REFERENCE *listOfProperties = NULL;
  BACNET_APPLICATION_DATA_VALUE *value = NULL;
  char array_val[32][50];
  int array_val_len = 0;
  int i;  

  listOfProperties = rpm_data->listOfProperties;
  value = listOfProperties->value;

  object_value.object_type = rpm_data->object_type;
  object_value.object_instance = rpm_data->object_instance;

  while (value) {
    object_value.object_property = listOfProperties->propertyIdentifier;
    object_value.array_index = listOfProperties->propertyArrayIndex;
    object_value.value = value;
    bacapp_snprintf_value(array_val[array_val_len], 50, &object_value);
    array_val_len++;
    value = value->next;
  }

  if (listOfProperties->propertyArrayIndex == BACNET_ARRAY_ALL) {
    sprintf(buf, "[");
    for(i = 0; i < array_val_len; i++) {
      snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "%s\"%s\"",
        (i == 0) ? "" : ",", array_val[i]);
    }
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "]");
  } else if (listOfProperties->propertyArrayIndex >= 0) {
    snprintf(buf, buf_len, "%s", array_val[0]);
  }
}


/*  
 * Encode read multiple list value results.
 */
void encode_read_multiple_list_value_result(BACNET_READ_ACCESS_DATA *rpm_data, char **out)
{
  BACNET_PROPERTY_REFERENCE *listOfProperties = NULL;
  BACNET_APPLICATION_DATA_VALUE *value = NULL;
  single_token_cb *array_val = NULL;
  char *buf;
  int buf_len;
  int array_val_len;
  int i;

  listOfProperties = rpm_data->listOfProperties;
  for (array_val_len = 0, value = listOfProperties->value; value != NULL; array_val_len++, value = value->next);

  array_val = calloc(1, sizeof(single_token_cb) * array_val_len);
  value = listOfProperties->value;

  for (i = 0, value = listOfProperties->value; value != NULL && (i < array_val_len); i++, value = value->next) {
    switch (value->tag) {
      case BACNET_APPLICATION_TAG_UNSIGNED_INT:
        snprintf(array_val[i].token, sizeof(array_val[i].token) - 1, "%lu", (unsigned long)value->type.Unsigned_Int);
        break;

      case BACNET_APPLICATION_TAG_OBJECT_ID:
        snprintf(array_val[i].token, sizeof(array_val[i].token) - 1, "[\"%s\", \"%lu\"]",
          bactext_object_type_name(value->type.Object_Id.type), (unsigned long)value->type.Object_Id.instance);
        break;

      default:
        sprintf(array_val[i].token, "\"Unsupported Value Format\"");
        break;
    }
  }

  buf_len = sizeof(single_token_cb) * array_val_len;
  buf = calloc(1, buf_len);
  sprintf(buf, "[");
  for(i = 0; i < array_val_len; i++) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "%s%s",
      (i == 0) ? "" : ",", array_val[i].token);
  }
  snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "]");
  free(array_val);
  *out = buf;
}


/*
 * Encode object list value results.
 */
void encode_object_list_value_result(BACNET_READ_PROPERTY_DATA *data, llist_obj_data *obj_data, char *buf, int buf_len)
{
  BACNET_OBJECT_PROPERTY_VALUE object_value;
  BACNET_APPLICATION_DATA_VALUE value;
  uint8_t *application_data;
  char array_val[300][50];
  int array_val_len = 0;
  int application_data_len;
  int len;
  int i;

  memset((char *)&object_value, 0, sizeof(object_value));
  memset((char *)array_val, 0, sizeof(array_val));
  application_data = data->application_data;
  application_data_len = data->application_data_len;

  for (;;) {
    len = bacapp_decode_application_data(
      application_data, (unsigned)application_data_len, &value);

    object_value.object_type = data->object_type;
    object_value.object_instance = data->object_instance;
    object_value.object_property = data->object_property;
    object_value.array_index = data->array_index;
    object_value.value = &value;
    switch (value.tag) {
      case BACNET_APPLICATION_TAG_UNSIGNED_INT:
        snprintf(array_val[array_val_len], 49, "%lu", (unsigned long)value.type.Unsigned_Int);
        array_val_len++;
        break;

      case BACNET_APPLICATION_TAG_OBJECT_ID:
        snprintf(array_val[array_val_len], 49, "[\"%s\", \"%lu\"]",
          bactext_object_type_name(value.type.Object_Id.type), (unsigned long)value.type.Object_Id.instance);
        array_val_len++;
        break;

      default:
        break;
    }

    if (len > 0) {
      if (len < application_data_len) {
        application_data += len;
        application_data_len -= len;

      } else {
        break;
      }
    } else {
      break;
    }

    if (array_val_len == 300) {
      if (mqtt_debug) {
        printf("- WARNING: Too many objects in the list.\n");
      }

      break;
    }
  }

  if (obj_data->index == BACNET_ARRAY_ALL) {
    sprintf(buf, "[");
    for(i = 0; i < array_val_len; i++) {
      snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "%s%s",
        (i == 0) ? "" : ",", array_val[i]);
    }
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "]");
  } else if (obj_data->index >= 0) {
    snprintf(buf, buf_len, "%s", array_val[0]);
  }
}


static bool append_str(char **str, size_t *rem_str_len, const char *add_str)
{
  bool retval;
  int bytes_written;

  bytes_written = snprintf(*str, *rem_str_len, "%s", add_str);
  if ((bytes_written < 0) || (bytes_written >= *rem_str_len)) {
    /* If there was an error or output was truncated, return error */
    retval = false;
  } else {
    /* Successfully wrote the contents to the string. Let's advance the
     * string pointer to the end, and account for the used space */
    *str += bytes_written;
    *rem_str_len -= bytes_written;
    retval = true;
  }

  return retval;
}


/*
 * Extract character string from bacnet application value string.
 */
int extract_char_string(char *buf, int buf_len, BACNET_APPLICATION_DATA_VALUE *value)
{
  int len, i;
  int rem_str_len = buf_len;
  char temp_str[512];
  char *p_str = buf;
  char *char_str;

  len = characterstring_length(&value->type.Character_String);
  char_str = characterstring_value(&value->type.Character_String);
  for (i = 0; i < len; i++) {
    if (isprint(*((unsigned char *)char_str))) {
      snprintf(temp_str, sizeof(temp_str), "%c", *char_str);
    } else {
      snprintf(temp_str, sizeof(temp_str), "%c", '.');
    }
    if (!append_str(&p_str, (size_t*)&rem_str_len, temp_str)) {
      break;
    }
    char_str++;
  }

  return(0);
}


/*
 * Encode value payload for bacnet client read command.
 */
int encode_read_value_result(BACNET_READ_PROPERTY_DATA *data, llist_obj_data *obj_data, char *buf, int buf_len)
{
  BACNET_OBJECT_PROPERTY_VALUE object_value = {0};
  BACNET_APPLICATION_DATA_VALUE value;
  char tmp[25000] = {0};
  int i;

  bacapp_decode_application_data(
    data->application_data, (unsigned)data->application_data_len, &value);
  
  switch(data->object_type) {
    case OBJECT_ANALOG_INPUT:
    case OBJECT_ANALOG_OUTPUT:
    case OBJECT_ANALOG_VALUE:
      switch(data->object_property) {
        case PROP_PRIORITY_ARRAY:
          encode_array_value_result(data, obj_data, tmp, sizeof(tmp) - 1);
          break;

        default:
          object_value.object_type = data->object_type;
          object_value.object_instance = data->object_instance;
          object_value.object_property = data->object_property;
          object_value.array_index = data->array_index;
          object_value.value = &value;
          bacapp_snprintf_value(tmp, sizeof(tmp) - 1, &object_value);
          break;
      }
      break;

    case OBJECT_BINARY_INPUT:
    case OBJECT_BINARY_OUTPUT:
    case OBJECT_BINARY_VALUE:
      switch(data->object_property) {
        case PROP_PRIORITY_ARRAY:
          encode_array_value_result(data, obj_data, tmp, sizeof(tmp) - 1);
          break;

        default:
          object_value.object_type = data->object_type;
          object_value.object_instance = data->object_instance;
          object_value.object_property = data->object_property;
          object_value.array_index = data->array_index;
          object_value.value = &value;
          bacapp_snprintf_value(tmp, sizeof(tmp) - 1, &object_value);
          if (!strcmp(tmp, "inactive")) {
            strcpy(tmp, "0");
          } else if (!strcmp(tmp, "active")) {
            strcpy(tmp, "1");
          }
          break;
      }
      break;

    case OBJECT_DEVICE:
      switch(data->object_property) {
        case PROP_SYSTEM_STATUS:
          device_status_to_str(*((unsigned int*)&value.type), tmp, sizeof(tmp) - 1);
          break;

        case PROP_OBJECT_LIST:
          encode_object_list_value_result(data, obj_data, tmp, sizeof(tmp) - 1);
          break;

        default:
          object_value.object_type = data->object_type;
          object_value.object_instance = data->object_instance;
          object_value.object_property = data->object_property;
          object_value.array_index = data->array_index;
          object_value.value = &value;
          bacapp_snprintf_value(tmp, sizeof(tmp) - 1, &object_value);
          break;
      }

      break;

    case OBJECT_MULTI_STATE_INPUT:
    case OBJECT_MULTI_STATE_OUTPUT:
    case OBJECT_MULTI_STATE_VALUE:
      object_value.object_type = data->object_type;
      object_value.object_instance = data->object_instance;
      object_value.object_property = data->object_property;
      object_value.array_index = data->array_index;
      object_value.value = &value;
      bacapp_snprintf_value(tmp, sizeof(tmp) - 1, &object_value);
      break;

    default:
      object_value.object_type = data->object_type;
      object_value.object_instance = data->object_instance;
      object_value.object_property = data->object_property;
      object_value.array_index = data->array_index;
      object_value.value = &value;
      bacapp_snprintf_value(tmp, sizeof(tmp) - 1, &object_value);
      break;
  }

  if (data->object_property == PROP_PRIORITY_ARRAY && obj_data->index == BACNET_ARRAY_ALL) {
    snprintf(buf, buf_len, "{ \"value\" : %s , \"deviceInstance\" : \"%d\" ",
      tmp, obj_data->device_instance);
  } else if (data->object_property == PROP_OBJECT_LIST && (obj_data->index == BACNET_ARRAY_ALL ||
    obj_data->index > 0)) {
    snprintf(buf, buf_len, "{ \"value\" : %s , \"deviceInstance\" : \"%d\" ",
      tmp, obj_data->device_instance);
  } else {
    if (tmp[0] == '"') {
      snprintf(buf, buf_len, "{ \"value\" : %s , \"deviceInstance\" : \"%d\" ",
        tmp, obj_data->device_instance);
    } else {
      snprintf(buf, buf_len, "{ \"value\" : \"%s\" , \"deviceInstance\" : \"%d\" ",
        tmp, obj_data->device_instance);
    }
  }

  if (obj_data->dnet >= 0) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"dnet\" : \"%d\" ",
      obj_data->dnet);
  }

  if (strlen(obj_data->dadr)) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"dadr\" : \"%s\" ",
      obj_data->dadr);
  }

  if (strlen(obj_data->mac)) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"mac\" : \"%s\" ",
      obj_data->mac);
  }

  if (obj_data->index >= 0) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"index\" : \"%d\" ",
      obj_data->index);
  }

  for (i = 0; i < obj_data->req_tokens_len; i++) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"%s\" : \"%s\"",
      obj_data->req_tokens[i].key, obj_data->req_tokens[i].value);
  }

  snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "}");

  return(0);
}


/*
 * Encode value payload for bacnet client read command.
 */
int encode_read_multiple_value_result(BACNET_READ_ACCESS_DATA *rpm_data, llist_obj_data *obj_data)
{
  BACNET_OBJECT_PROPERTY_VALUE object_value = {0};
  BACNET_APPLICATION_DATA_VALUE *value = NULL;
  BACNET_PROPERTY_REFERENCE *listOfProperties = NULL;
  char tmp_val[MAX_CMD_STR_OPT_VALUE_LENGTH] = {0};
  char *blob_value = NULL;
  int i;

  listOfProperties = rpm_data->listOfProperties;
  value = listOfProperties->value;

  if (value) {
    switch(rpm_data->object_type) {
      case OBJECT_ANALOG_INPUT:
      case OBJECT_ANALOG_OUTPUT:
      case OBJECT_ANALOG_VALUE:
        switch(listOfProperties->propertyIdentifier) {
          case PROP_PRIORITY_ARRAY:
            encode_read_multiple_array_value_result(rpm_data, tmp_val, sizeof(tmp_val) - 1);
            break;

          default:
            object_value.object_type = rpm_data->object_type;
            object_value.object_instance = rpm_data->object_instance;
            object_value.object_property = listOfProperties->propertyIdentifier;
            object_value.array_index = listOfProperties->propertyArrayIndex;
            object_value.value = value;
            bacapp_snprintf_value(tmp_val, sizeof(tmp_val) - 1, &object_value);
            break;
        }
        break;

      case OBJECT_BINARY_INPUT:
      case OBJECT_BINARY_OUTPUT:
      case OBJECT_BINARY_VALUE:
        switch(listOfProperties->propertyIdentifier) {
          case PROP_PRIORITY_ARRAY:
            encode_read_multiple_array_value_result(rpm_data, tmp_val, sizeof(tmp_val) - 1);
            break;

          default:
            object_value.object_type = rpm_data->object_type;
            object_value.object_instance = rpm_data->object_instance;
            object_value.object_property = listOfProperties->propertyIdentifier;
            object_value.array_index = listOfProperties->propertyArrayIndex;
            object_value.value = value;
            bacapp_snprintf_value(tmp_val, sizeof(tmp_val) - 1, &object_value);
            break;
        }
        break;

      case OBJECT_DEVICE:
        switch(listOfProperties->propertyIdentifier) {
          case PROP_SYSTEM_STATUS:
            device_status_to_str(value->type.Unsigned_Int, tmp_val, sizeof(tmp_val) - 1);
            break;

          case PROP_OBJECT_LIST:
            encode_read_multiple_list_value_result(rpm_data, &blob_value);
            break;

          default:
            object_value.object_type = rpm_data->object_type;
            object_value.object_instance = rpm_data->object_instance;
            object_value.object_property = listOfProperties->propertyIdentifier;
            object_value.array_index = listOfProperties->propertyArrayIndex;
            object_value.value = value;
            bacapp_snprintf_value(tmp_val, sizeof(tmp_val) - 1, &object_value);
            break;
        }

        break;

      case OBJECT_MULTI_STATE_INPUT:
      case OBJECT_MULTI_STATE_OUTPUT:
      case OBJECT_MULTI_STATE_VALUE:
        object_value.object_type = rpm_data->object_type;
        object_value.object_instance = rpm_data->object_instance;
        object_value.object_property = listOfProperties->propertyIdentifier;
        object_value.array_index = listOfProperties->propertyArrayIndex;
        object_value.value = value;
        bacapp_snprintf_value(tmp_val, sizeof(tmp_val) - 1, &object_value);
        break;

      default:
        object_value.object_type = rpm_data->object_type;
        object_value.object_instance = rpm_data->object_instance;
        object_value.object_property = listOfProperties->propertyIdentifier;
        object_value.array_index = listOfProperties->propertyArrayIndex;
        object_value.value = value;
        bacapp_snprintf_value(tmp_val, sizeof(tmp_val) - 1, &object_value);
        break;
    }
  } else {
    snprintf(tmp_val, sizeof(tmp_val) - 1, "BACnet Error: %s: %s",
      bactext_error_class_name((int)listOfProperties->error.error_class),
      bactext_error_code_name((int)listOfProperties->error.error_code));
  }

  for (i = 0; i < obj_data->rpm_objects_len; i++) {
    if ((obj_data->rpm_objects[i].object_type == rpm_data->object_type) &&
      (obj_data->rpm_objects[i].object_instance == rpm_data->object_instance) &&
      (obj_data->rpm_objects[i].property == listOfProperties->propertyIdentifier) &&
      (obj_data->rpm_objects[i].index == listOfProperties->propertyArrayIndex)) {
      switch (listOfProperties->propertyIdentifier) {
        case PROP_OBJECT_LIST:
          obj_data->rpm_objects[i].blob_value = NULL;
          if (blob_value) {
            obj_data->rpm_objects[i].blob_value = blob_value;
          }
          break;

        default:
          if (value) {
            strcpy(obj_data->rpm_objects[i].value, tmp_val);
          } else {
            strcpy(obj_data->rpm_objects[i].error, tmp_val);
          }

          obj_data->rpm_objects[i].blob_value = NULL;
          break;
      }

      break;
    }
  }

  return(0);
}


/*
 * Priority array set to json string.
 */
static int prio_array_set_to_json_string(json_key_value_pair *prio_array, int prio_array_len,
  char *buf, int buf_len)
{
  int i;
  sprintf(buf, "{");
  for (i = 0; i < prio_array_len; i++) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "%s\"%s\" : \"%s\"",
      (i == 0) ? "" : " , ", prio_array[i].key,
      (strcmp(prio_array[i].value, "255")) ? prio_array[i].value : "Null");
  }
  snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "}");

  return(0);
}


/*
 * Encode value payload for bacnet client write command.
 */
int encode_write_value_result(llist_obj_data *obj_data, char *buf, int buf_len)
{
  char prio_array_buf[1024] = {0};
  int i;

  switch(obj_data->object_type) {
    case OBJECT_ANALOG_INPUT:
    case OBJECT_ANALOG_OUTPUT:
    case OBJECT_ANALOG_VALUE:
    case OBJECT_BINARY_INPUT:
    case OBJECT_BINARY_OUTPUT:
    case OBJECT_BINARY_VALUE:
    case OBJECT_MULTI_STATE_INPUT:
    case OBJECT_MULTI_STATE_OUTPUT:
    case OBJECT_MULTI_STATE_VALUE:
      if (obj_data->prio_array_len > 0) {
        prio_array_set_to_json_string(&obj_data->prio_array[0], obj_data->prio_array_len,
          prio_array_buf, sizeof(prio_array_buf) - 1);
        snprintf(buf, buf_len, "{ \"value\" : %s , \"deviceInstance\" : \"%d\" ",
          prio_array_buf, obj_data->device_instance);
      } else {
        snprintf(buf, buf_len, "{ \"value\" : \"%s\" , \"deviceInstance\" : \"%d\" ",
          obj_data->value, obj_data->device_instance);
      }
      if (obj_data->priority) {
        snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"priority\" : \"%d\" ",
          obj_data->priority);
      }
      break;

    default:
      return(1);
  }

  if (obj_data->dnet >= 0) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"dnet\" : \"%d\" ",
      obj_data->dnet);
  }

  if (strlen(obj_data->dadr)) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"dadr\" : \"%s\" ",
      obj_data->dadr);
  }

  if (strlen(obj_data->mac)) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"mac\" : \"%s\" ",
      obj_data->mac);
  }

  if (obj_data->index >= 0 && obj_data->prio_array_len == 0) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"index\" : \"%d\" ",
      obj_data->index);
  }

  for (i = 0; i < obj_data->req_tokens_len; i++) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"%s\" : \"%s\"",
      obj_data->req_tokens[i].key, obj_data->req_tokens[i].value);
  }

  snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "}");

  return(0);
}


/*
 * Print MAC address into string.
 */
static void macaddr_to_str(uint8_t *addr, int len, char *buf, int buf_len)
{
  int j = 0;
    
  while (j < len) {
    if (j != 0) {
      snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ":");
    }
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "%02X", addr[j]);
    j++;
  }
}


/*
 * Print MAC address into string.
 */
static void macaddr_to_ip_port_str(uint8_t *addr, int len, char *buf, int buf_len)
{
  snprintf(buf, buf_len, "%d.%d.%d.%d:%d", addr[0], addr[1], addr[2], addr[3],
   ntohs(*((uint16_t *)&addr[4])));
}


/*
 * Encode value payload for bacnet client whois command.
 */
int encode_whois_result(llist_whois_cb *cb, char *buf, int buf_len)
{
  address_entry_cb *addr;
  char value[20000];
  char mac_buf[51] = {0};
  int i, value_len = sizeof(value);
  uint8_t local_sadr = 0;
  bool first = true;

  sprintf(value, "[ ");
  addr = cb->data.address_table.first;
  while (addr) {
    if (!first) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", ");
    }
    snprintf(&value[strlen(value)], value_len - strlen(value), "{\"device_id\" : \"%u\"", addr->device_id);
    if (addr->address.mac_len > 0) {
      if (addr->address.mac_len == 6) {
        macaddr_to_ip_port_str(addr->address.mac, addr->address.mac_len, mac_buf, sizeof(mac_buf));
      } else {
        macaddr_to_str(addr->address.mac, addr->address.mac_len, mac_buf, sizeof(mac_buf));
      }
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"mac_address\" : \"%s\"", mac_buf);
    }
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"snet\" : \"%u\"", addr->address.net);
    mac_buf[0] = '\0';
    if (addr->address.net) {
      macaddr_to_str(addr->address.adr, addr->address.len, mac_buf, sizeof(mac_buf));
    } else {
      macaddr_to_str(&local_sadr, 1, mac_buf, sizeof(mac_buf));
    }
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"sadr\" : \"%s\"", mac_buf);
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"apdu\" : \"%u\"", addr->max_apdu);
    snprintf(&value[strlen(value)], value_len - strlen(value), "}");

    addr = addr->next;
    first = false;
  }

  snprintf(&value[strlen(value)], value_len - strlen(value), " ]");
  snprintf(buf, buf_len, "{ \"value\" : %s", value);
  if (cb->data.dnet >= 0) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"dnet\" : \"%d\"", cb->data.dnet);
  }

  if (strlen(cb->data.mac)) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"mac\" : \"%s\"", cb->data.mac);
  }

  if (strlen(cb->data.dadr)) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"dadr\" : \"%s\"", cb->data.dadr);
  }

  if (cb->data.device_instance_min >= 0) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"device_instance_min\" : \"%d\"", cb->data.device_instance_min);
  }

  if (cb->data.device_instance_max >= 0 && cb->data.device_instance_min != cb->data.device_instance_max) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"device_instance_max\" : \"%d\"", cb->data.device_instance_max);
  }

  for (i = 0; i < cb->data.req_tokens_len; i++) {
    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"%s\" : \"%s\"",
      cb->data.req_tokens[i].key, cb->data.req_tokens[i].value);
  }

  snprintf(&buf[strlen(buf)], buf_len - strlen(buf), " }");

  return(0);
}


/*
 * Publish bacnet client read value result.
 */
int publish_bacnet_client_read_value_result(BACNET_READ_PROPERTY_DATA *data,
  llist_obj_data *obj_data)
{
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
  char topic[512];
  char topic_value[30000] = {0};
  char *object_type_str;
  char *object_property;
  int mqtt_retry_limit = yaml_config_mqtt_connect_retry_limit();
  int mqtt_retry_interval = yaml_config_mqtt_connect_retry_interval();
  int rc, i;

  object_type_str = get_object_type_str(data->object_type);
  if (!object_type_str) {
    printf("Unsupported object type: %d\n", data->object_type);
    return(1);
  }

  object_property = get_object_property_str(data->object_property);
  if (!object_property) {
    printf("Unsupported object property: %d\n", data->object_property);
    return(1);
  }

  sprintf(topic, "bacnet/cmd_result/read_value/%s/%d/%s",
    object_type_str, data->object_instance, object_property);

  encode_read_value_result(data, obj_data, topic_value, sizeof(topic_value) - 1);

  if (mqtt_debug) {
    printf("- read value result topic: %s\n", topic);
  }

  if (yaml_config_mqtt_connect_retry() && mqtt_retry_limit > 0) {
    for (i = 0; i < mqtt_retry_limit && !mqtt_client_connected; i++) {
      if (mqtt_debug) {
        printf("MQTT reconnect retry: %d/%d\n", (i + 1), mqtt_retry_limit);
      }

      mqtt_check_reconnect();
      sleep(mqtt_retry_interval);
    }

    if (i >= mqtt_retry_limit) {
      if (mqtt_debug) {
        printf("MQTT reconnect limit reached: %d\n", i);
      }

      return(1);
    }
  }

  opts.onSuccess = mqtt_on_send_success;
  opts.onFailure = mqtt_on_send_failure;
  opts.context = mqtt_client;

  pubmsg.payload = topic_value;
  pubmsg.payloadlen = strlen(topic_value);
  pubmsg.qos = DEFAULT_PUB_QOS;
  pubmsg.retained = 0;
  rc = MQTTAsync_sendMessage(mqtt_client, topic, &pubmsg, &opts);
  if (mqtt_debug) {
    if (rc != MQTTASYNC_SUCCESS) {
      printf("MQTT failed to publish topic: \"%s\" , return code:%d\n", topic, rc);
    } else {
      printf("MQTT published topic: \"%s\"\n", topic);
    }
  }

  return(0);
}


/*
 * Publish bacnet client write value result.
 */
int publish_bacnet_client_write_value_result(llist_obj_data *data)
{
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
  char topic[512];
  char topic_value[2048] = {0};
  char *object_type_str;
  char *object_property;
  int mqtt_retry_limit = yaml_config_mqtt_connect_retry_limit();
  int mqtt_retry_interval = yaml_config_mqtt_connect_retry_interval();
  int rc, i;

  object_type_str = get_object_type_str(data->object_type);
  if (!object_type_str) {
    printf("Unsupported object type: %d\n", data->object_type);
    return(1);
  }

  object_property = get_object_property_str(data->object_property);
  if (!object_property) {
    printf("Unsupported object property: %d\n", data->object_property);
    return(1);
  }

  sprintf(topic, "bacnet/cmd_result/write_value/%s/%d/%s",
    object_type_str, data->object_instance, object_property);

  encode_write_value_result(data, topic_value, sizeof(topic_value));

  if (mqtt_debug) {
    printf("- write value result topic: %s\n", topic);
    printf("- write value result topic value: %s\n", topic_value);
  }

  if (yaml_config_mqtt_connect_retry() && mqtt_retry_limit > 0) {
    for (i = 0; i < mqtt_retry_limit && !mqtt_client_connected; i++) {
      if (mqtt_debug) {
        printf("MQTT reconnect retry: %d/%d\n", (i + 1), mqtt_retry_limit);
      }

      mqtt_check_reconnect();
      sleep(mqtt_retry_interval);
    }

    if (i >= mqtt_retry_limit) {
      if (mqtt_debug) {
        printf("MQTT reconnect limit reached: %d\n", i);
      }

      return(1);
    }
  }

  opts.onSuccess = mqtt_on_send_success;
  opts.onFailure = mqtt_on_send_failure;
  opts.context = mqtt_client;

  pubmsg.payload = topic_value;
  pubmsg.payloadlen = strlen(topic_value);
  pubmsg.qos = DEFAULT_PUB_QOS;
  pubmsg.retained = 0;
  rc = MQTTAsync_sendMessage(mqtt_client, topic, &pubmsg, &opts);
  if (mqtt_debug) {
    if (rc != MQTTASYNC_SUCCESS) {
      printf("MQTT failed to publish topic: \"%s\" , return code:%d\n", topic, rc);
    } else {
      printf("MQTT published topic: \"%s\"\n", topic);
    }
  }

  return(0);
}


/*
 * Publish bacnet client write multiple value result.
 */
int publish_bacnet_client_read_multiple_value_result(llist_obj_data *data)
{
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
  char topic[512];
  char *topic_value;
  int topic_value_len;
  int mqtt_retry_limit = yaml_config_mqtt_connect_retry_limit();
  int mqtt_retry_interval = yaml_config_mqtt_connect_retry_interval();
  int rc, i;
  bool is_error = false;

  topic_value_len = (data->rpm_objects_len + 1)* WRITEM_PROP_PUB_LENGTH;
  topic_value = malloc(topic_value_len);
  if (topic_value == NULL) {
    printf("Error allocating memory for write multiple publish.\n");
    goto EXIT;
  }

  sprintf(topic, "bacnet/cmd_result/read_multiple_value");
  sprintf(topic_value, "{ \"deviceInstance\" : \"%u\", \"objects\" : [",
    data->device_instance);

  for (i = 0; i < data->rpm_objects_len; i++) {
    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      "%s{\"objectType\" : \"%d\", \"objectInstance\" : \"%u\", \"property\" : \"%d\"",
      (i == 0) ? "" : ",",
      data->rpm_objects[i].object_type, data->rpm_objects[i].object_instance,
      data->rpm_objects[i].property);
    if (data->rpm_objects[i].index >= 0 ) {
      snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
        ", \"index\" : \"%d\"", data->rpm_objects[i].index);
    }

    if (data->rpm_objects[i].priority > 0 ) {
      snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
        ", \"priority\" : \"%d\"", data->rpm_objects[i].priority);
    }

    if (data->rpm_objects[i].blob_value) {
      snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
        ", \"value\" : %s}", data->rpm_objects[i].blob_value);
      free(data->rpm_objects[i].blob_value);
    } else {
      if (strlen(data->rpm_objects[i].value) > 0) {
        if ((data->rpm_objects[i].value[0] == '"') ||
          (data->rpm_objects[i].value[0] == '[') ||
          (data->rpm_objects[i].value[0] == '{')) {
          snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
            ", \"value\" : %s}", data->rpm_objects[i].value);
        } else {
          snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
            ", \"value\" : \"%s\"}", data->rpm_objects[i].value);
        }
      } else {
        snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
          ", \"error\" : \"%s\"}", data->rpm_objects[i].error);
      }
    }

    if (strlen(data->rpm_objects[i].error) > 0) {
      is_error = true;
    }
  }

  snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value), "]");

  if (data->dnet >= 0) {
    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      ", \"dnet\" : \"%d\" ", data->dnet);
  }

  if (strlen(data->dadr)) {
    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      ", \"dadr\" : \"%s\" ", data->dadr);
  }

  if (strlen(data->mac)) {
    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      ", \"mac\" : \"%s\" ", data->mac);
  }

  if (is_error) {
    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      ", \"error\" : \"Error in reading object / propery value\" ");
  }

  for (i = 0; i < data->req_tokens_len; i++) {
    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      ", \"%s\" : \"%s\"", data->req_tokens[i].key, data->req_tokens[i].value);
  }

  snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value), " }");

  if (yaml_config_mqtt_connect_retry() && mqtt_retry_limit > 0) { 
    for (i = 0; i < mqtt_retry_limit && !mqtt_client_connected; i++) {
      if (mqtt_debug) {
        printf("MQTT reconnect retry: %d/%d\n", (i + 1), mqtt_retry_limit);
      }

      mqtt_check_reconnect();
      sleep(mqtt_retry_interval);
    }

    if (i >= mqtt_retry_limit) {
      if (mqtt_debug) {
        printf("MQTT reconnect limit reached: %d\n", i);
      }

      goto EXIT;
    }
  }

  opts.onSuccess = mqtt_on_send_success;
  opts.onFailure = mqtt_on_send_failure;
  opts.context = mqtt_client;

  pubmsg.payload = topic_value;
  pubmsg.payloadlen = strlen(topic_value);
  pubmsg.qos = DEFAULT_PUB_QOS;
  pubmsg.retained = 0;
  rc = MQTTAsync_sendMessage(mqtt_client, topic, &pubmsg, &opts);
  if (mqtt_debug) {
    if (rc != MQTTASYNC_SUCCESS) {
      printf("MQTT failed to publish topic: \"%s\" , return code:%d\n", topic, rc);
    } else {
      printf("MQTT published topic: \"%s\"\n", topic);
    }
  }

  free(topic_value);

  EXIT:

  if (data->rpm_objects) {
    free(data->rpm_objects);
  }

  return(0);
}


/*
 * Publish bacnet client write multiple value result.
 */
int publish_bacnet_client_write_multiple_value_result(llist_obj_data *data)
{
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
  char topic[512];
  char *topic_value;
  int topic_value_len;
  int mqtt_retry_limit = yaml_config_mqtt_connect_retry_limit();
  int mqtt_retry_interval = yaml_config_mqtt_connect_retry_interval();
  int rc, i;

  topic_value_len = (data->rpm_objects_len + 1)* WRITEM_PROP_PUB_LENGTH;
  topic_value = calloc(1, topic_value_len);
  if (topic_value == NULL) {
    printf("Error allocating memory for write multiple publish.\n");
    goto EXIT;
  }

  sprintf(topic, "bacnet/cmd_result/write_multiple_value");
  sprintf(topic_value, "{ \"deviceInstance\" : \"%u\", \"objects\" : [",
    data->device_instance);

  for (i = 0; i < data->rpm_objects_len; i++) {
    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      "%s{\"objectType\" : \"%d\", \"objectInstance\" : \"%u\", \"property\" : \"%d\"",
      (i == 0) ? "" : ",",
      data->rpm_objects[i].object_type, data->rpm_objects[i].object_instance,
      data->rpm_objects[i].property);
    if (data->rpm_objects[i].index >= 0 ) {
      snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
        ", \"index\" : \"%d\"", data->rpm_objects[i].index);
    }

    if (data->rpm_objects[i].priority > 0 ) {
      snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
        ", \"priority\" : \"%d\"", data->rpm_objects[i].priority);
    }

    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      ", \"value\" : \"%s\" }", data->rpm_objects[i].value);
  }

  snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value), "]");

  if (data->dnet >= 0) {
    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      ", \"dnet\" : \"%d\" ", data->dnet);
  }

  if (strlen(data->dadr)) {
    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      ", \"dadr\" : \"%s\" ", data->dadr);
  }

  if (strlen(data->mac)) {
    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      ", \"mac\" : \"%s\" ", data->mac);
  }

  if (strlen(data->err_msg)) {
    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      ", \"error\" : \"%s\" ", data->err_msg);
  }

  for (i = 0; i < data->req_tokens_len; i++) {
    snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value),
      ", \"%s\" : \"%s\"", data->req_tokens[i].key, data->req_tokens[i].value);
  }

  snprintf(&topic_value[strlen(topic_value)], topic_value_len - strlen(topic_value), " }");

  if (yaml_config_mqtt_connect_retry() && mqtt_retry_limit > 0) {
    for (i = 0; i < mqtt_retry_limit && !mqtt_client_connected; i++) {
      if (mqtt_debug) {
        printf("MQTT reconnect retry: %d/%d\n", (i + 1), mqtt_retry_limit);
      }

      mqtt_check_reconnect();
      sleep(mqtt_retry_interval);
    }

    if (i >= mqtt_retry_limit) {
      if (mqtt_debug) {
        printf("MQTT reconnect limit reached: %d\n", i);
      }

      goto EXIT;
    }
  }

  opts.onSuccess = mqtt_on_send_success;
  opts.onFailure = mqtt_on_send_failure;
  opts.context = mqtt_client;

  pubmsg.payload = topic_value;
  pubmsg.payloadlen = strlen(topic_value);
  pubmsg.qos = DEFAULT_PUB_QOS;
  pubmsg.retained = 0;
  rc = MQTTAsync_sendMessage(mqtt_client, topic, &pubmsg, &opts);
  if (mqtt_debug) {
    if (rc != MQTTASYNC_SUCCESS) {
      printf("MQTT failed to publish topic: \"%s\" , return code:%d\n", topic, rc);
    } else {
      printf("MQTT published topic: \"%s\"\n", topic);
    }
  }

  free(topic_value);

  EXIT:

  if (data->rpm_objects) {
    free(data->rpm_objects);
  }

  return(0);
}


/*
 * Publish bacnet client whois result.
 */
int publish_bacnet_client_whois_result(llist_whois_cb *cb)
{
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
  char topic[512];
  char topic_value[30000] = {0};
  int mqtt_retry_limit = yaml_config_mqtt_connect_retry_limit();
  int mqtt_retry_interval = yaml_config_mqtt_connect_retry_interval();
  int rc, i;

  sprintf(topic, "bacnet/cmd_result/whois");
  encode_whois_result(cb, topic_value, sizeof(topic_value));

  printf("- write value result topic: %s\n", topic);
  printf("- write value result topic value: %s\n", topic_value);

  if (yaml_config_mqtt_connect_retry() && mqtt_retry_limit > 0) {
    for (i = 0; i < mqtt_retry_limit && !mqtt_client_connected; i++) {
      if (mqtt_debug) {
        printf("MQTT reconnect retry: %d/%d\n", (i + 1), mqtt_retry_limit);
      }

      mqtt_check_reconnect();
      sleep(mqtt_retry_interval);
    }

    if (i >= mqtt_retry_limit) {
      if (mqtt_debug) {
        printf("MQTT reconnect limit reached: %d\n", i);
      }

      return(1);
    }
  }

  opts.onSuccess = mqtt_on_send_success;
  opts.onFailure = mqtt_on_send_failure;
  opts.context = mqtt_client;

  pubmsg.payload = topic_value;
  pubmsg.payloadlen = strlen(topic_value);
  pubmsg.qos = DEFAULT_PUB_QOS;
  pubmsg.retained = 0;
  rc = MQTTAsync_sendMessage(mqtt_client, topic, &pubmsg, &opts);
  if (mqtt_debug) {
    if (rc != MQTTASYNC_SUCCESS) {
      printf("MQTT failed to publish topic: \"%s\" , return code:%d\n", topic, rc);
    } else {
      printf("MQTT published topic: \"%s\"\n", topic);
    }
  }

  return(0);
}


/*
 * Publish whois device.
 */
int publish_whois_device(char *topic, char *topic_value)
{
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
  int mqtt_retry_limit = yaml_config_mqtt_connect_retry_limit();
  int mqtt_retry_interval = yaml_config_mqtt_connect_retry_interval();
  int rc, i;

  printf("- write value result topic: %s\n", topic);
  printf("- write value result topic value: %s\n", topic_value);

  if (yaml_config_mqtt_connect_retry() && mqtt_retry_limit > 0) {
    for (i = 0; i < mqtt_retry_limit && !mqtt_client_connected; i++) {
      if (mqtt_debug) {
        printf("MQTT reconnect retry: %d/%d\n", (i + 1), mqtt_retry_limit);
      }

      mqtt_check_reconnect();
      sleep(mqtt_retry_interval);
    }

    if (i >= mqtt_retry_limit) {
      if (mqtt_debug) {
        printf("MQTT reconnect limit reached: %d\n", i);
      }

      return(1);
    }
  }

  opts.onSuccess = mqtt_on_send_success;
  opts.onFailure = mqtt_on_send_failure;
  opts.context = mqtt_client;

  pubmsg.payload = topic_value;
  pubmsg.payloadlen = strlen(topic_value);
  pubmsg.qos = DEFAULT_PUB_QOS;
  pubmsg.retained = 0;
  rc = MQTTAsync_sendMessage(mqtt_client, topic, &pubmsg, &opts);
  if (mqtt_debug) {
    if (rc != MQTTASYNC_SUCCESS) {
      printf("MQTT failed to publish topic: \"%s\" , return code:%d\n", topic, rc);
    } else {
      printf("MQTT published topic: \"%s\"\n", topic);
    }
  }

  return(0);
}


/*
 * Publish topic value.
 */
int publish_topic_value(char *topic, char *topic_value)
{
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
  int mqtt_retry_limit = yaml_config_mqtt_connect_retry_limit();
  int mqtt_retry_interval = yaml_config_mqtt_connect_retry_interval();
  int rc, i;

  if (yaml_config_mqtt_connect_retry() && mqtt_retry_limit > 0) {
    for (i = 0; i < mqtt_retry_limit && !mqtt_client_connected; i++) {
      if (mqtt_debug) {
        printf("MQTT reconnect retry: %d/%d\n", (i + 1), mqtt_retry_limit);
      }

      mqtt_check_reconnect();
      sleep(mqtt_retry_interval);
    }

    if (i >= mqtt_retry_limit) {
      if (mqtt_debug) {
        printf("MQTT reconnect limit reached: %d\n", i);
      }

      return(1);
    }
  }

  opts.onSuccess = mqtt_on_send_success;
  opts.onFailure = mqtt_on_send_failure;
  opts.context = mqtt_client;

  pubmsg.payload = topic_value;
  pubmsg.payloadlen = strlen(topic_value);
  pubmsg.qos = DEFAULT_PUB_QOS;
  pubmsg.retained = 0;
  rc = MQTTAsync_sendMessage(mqtt_client, topic, &pubmsg, &opts);
  if (mqtt_debug) {
    if (rc != MQTTASYNC_SUCCESS) {
      printf("MQTT failed to publish topic: \"%s\" , return code:%d\n", topic, rc);
    } else {
      printf("MQTT published topic: \"%s\"\n", topic);
    }
  }

  return(0);
}


/*
 * Publish bacnet client whois result per device.
 */
int publish_bacnet_client_whois_result_per_device(llist_whois_cb *cb)
{
  address_entry_cb *addr;
  char topic[512];
  char value[20000];
  char mac_buf[51] = {0};
  int i, value_len = sizeof(value);
  uint8_t local_sadr = 0;

  sprintf(topic, "bacnet/cmd_result/whois");
  addr = cb->data.address_table.first;
  if (!addr) {
    sprintf(value, "{ \"value\" : [ ]");
    if (cb->data.dnet >= 0) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"dnet\" : \"%d\"", cb->data.dnet);
    }

    if (strlen(cb->data.mac)) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"mac\" : \"%s\"", cb->data.mac);
    }

    if (strlen(cb->data.dadr)) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"dadr\" : \"%s\"", cb->data.dadr);
    }

    if (cb->data.device_instance_min >= 0) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"device_instance_min\" : \"%d\"", cb->data.device_instance_min);
    }

    if (cb->data.device_instance_max >= 0 && cb->data.device_instance_min != cb->data.device_instance_max) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"device_instance_max\" : \"%d\"", cb->data.device_instance_max);
    }

    for (i = 0; i < cb->data.req_tokens_len; i++) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"%s\" : \"%s\"",
        cb->data.req_tokens[i].key, cb->data.req_tokens[i].value);
    }

    snprintf(&value[strlen(value)], value_len - strlen(value), " }");
    publish_whois_device(topic, value);

    return(0);
  }

  while (addr) {
    sprintf(value, "{ \"value\" : [ {\"device_id\" : \"%u\"", addr->device_id);
    if (addr->address.mac_len > 0) {
      if (addr->address.mac_len == 6) {
        macaddr_to_ip_port_str(addr->address.mac, addr->address.mac_len, mac_buf, sizeof(mac_buf));
      } else {
        macaddr_to_str(addr->address.mac, addr->address.mac_len, mac_buf, sizeof(mac_buf));
      }
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"mac_address\" : \"%s\"", mac_buf);
    }
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"snet\" : \"%u\"", addr->address.net);
    mac_buf[0] = '\0';
    if (addr->address.net) {
      macaddr_to_str(addr->address.adr, addr->address.len, mac_buf, sizeof(mac_buf));
    } else {
      macaddr_to_str(&local_sadr, 1, mac_buf, sizeof(mac_buf));
    }
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"sadr\" : \"%s\"", mac_buf);
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"apdu\" : \"%u\"", addr->max_apdu);
    snprintf(&value[strlen(value)], value_len - strlen(value), "} ]");

    if (cb->data.dnet >= 0) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"dnet\" : \"%d\"", cb->data.dnet);
    }

    if (strlen(cb->data.mac)) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"mac\" : \"%s\"", cb->data.mac);
    }

    if (strlen(cb->data.dadr)) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"dadr\" : \"%s\"", cb->data.dadr);
    }

    if (cb->data.device_instance_min >= 0) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"device_instance_min\" : \"%d\"", cb->data.device_instance_min);
    }

    if (cb->data.device_instance_max >= 0 && cb->data.device_instance_min != cb->data.device_instance_max) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"device_instance_max\" : \"%d\"", cb->data.device_instance_max);
    }

    for (i = 0; i < cb->data.req_tokens_len; i++) {
      snprintf(&value[strlen(value)], value_len - strlen(value), ", \"%s\" : \"%s\"",
        cb->data.req_tokens[i].key, cb->data.req_tokens[i].value);
    }

    snprintf(&value[strlen(value)], value_len - strlen(value), " }");
    publish_whois_device(topic, value);
    addr = addr->next;
  }

  return(0);
}


/*
 * Publish bacnet client pics result.
 */
int publish_bacnet_client_pics_result(bacnet_client_cmd_opts *opts, char *epics_json)
{
  char topic[512];
  char *value;
  int value_len = strlen(epics_json) + 1000;
  int i;

  value = calloc(1, value_len);
  if (value == NULL) {
    fprintf(stderr, "Error: Failed to allocate memory for PICs publish message!\n");
    return(0);
  }

  sprintf(topic, "bacnet/cmd_result/pics");
  sprintf(value, "{ \"value\" : %s", epics_json);

  if (opts->dnet >= 0) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"dnet\" : \"%d\"", opts->dnet);
  }

  if (strlen(opts->mac)) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"mac\" : \"%s\"", opts->mac);
  }

  if (strlen(opts->dadr)) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"dadr\" : \"%s\"", opts->dadr);
  }

  if (opts->device_instance > 0) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"deviceInstance\" : \"%d\"", opts->device_instance);
  }

  for (i = 0; i < opts->req_tokens_len; i++) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"%s\" : \"%s\"",
      opts->req_tokens[i].key, opts->req_tokens[i].value);
  }

  snprintf(&value[strlen(value)], value_len - strlen(value), " }");
  publish_topic_value(topic, value);
  free(value);

  return(0);
}


/*
 * Publish bacnet client point discovery result.
 */
int publish_bacnet_client_point_disc_result(llist_point_disc_cb *pd_cb)
{
  point_entry_cb *pe_ptr;
  char topic[512];
  char *value;
  bool first;
  int value_len = (pd_cb->data.total_points * 50) + 512;
  int i;

  value = calloc(1, value_len);
  if (value == NULL) {
    fprintf(stderr, "Error: Failed to allocate memory for points discovery publish message!\n");
    return(0);
  }
    
  sprintf(topic, "bacnet/cmd_result/point_discovery");
  sprintf(value, "{");

  snprintf(&value[strlen(value)], value_len - strlen(value), "\"deviceInstance\" : \"%d\"",
    pd_cb->data.device_instance);

  if (strlen(pd_cb->data.mac)) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"mac\" : \"%s\"",
      pd_cb->data.mac);
  }

  if (pd_cb->data.dnet >= 0) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"dnet\" : \"%d\"",
      pd_cb->data.dnet);
  }
 
  if (strlen(pd_cb->data.dadr)) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"dadr\" : \"%s\"",
      pd_cb->data.dadr);
  }

  for (i = 0; i < pd_cb->data.req_tokens_len; i++) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"%s\" : \"%s\"",
      pd_cb->data.req_tokens[i].key, pd_cb->data.req_tokens[i].value);
  }

  snprintf(&value[strlen(value)], value_len - strlen(value), ", \"value\" : {\"objects\" : [");

  pe_ptr = pd_cb->data.point_list.head;
  first = true;
  while (pe_ptr != NULL) {
    switch(pe_ptr->object_type) {
      case OBJECT_DEVICE:
        goto NEXT;
        break;

      default:
        pd_cb->data.filtered_points++;
        break;
    }

    snprintf(&value[strlen(value)], value_len - strlen(value), "%s\"%s-%d\"", 
      ((first) ? "" : ","), get_object_type_str(pe_ptr->object_type), pe_ptr->instance);
    if (first) {
      first = false;
    }

    NEXT:
    pe_ptr = pe_ptr->next;
  }

  snprintf(&value[strlen(value)], value_len - strlen(value), "], \"count\" : \"%d\"}}", pd_cb->data.filtered_points);
  publish_topic_value(topic, value);
  free(value);

  return(0);
}


/*
 * Publish bacnet client points info result.
 */
int publish_bacnet_client_points_info_result(llist_points_info_cb *pi_cb)
{
  point_entry_cb *pe_cb;
  char topic[512];
  char *value;
  bool first;
  int value_len = (pi_cb->data.points_len * 100) + 512;
  int i;

  value = calloc(1, value_len);
  if (value == NULL) {
    fprintf(stderr, "Error: Failed to allocate memory for points info publish message!\n");
    return(0);
  }

  sprintf(topic, "bacnet/cmd_result/points_info");
  sprintf(value, "{");

  snprintf(&value[strlen(value)], value_len - strlen(value), "\"deviceInstance\" : \"%d\"",
    pi_cb->data.device_instance);

  if (strlen(pi_cb->data.mac)) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"mac\" : \"%s\"",
      pi_cb->data.mac);
  }

  if (pi_cb->data.dnet >= 0) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"dnet\" : \"%d\"",
      pi_cb->data.dnet);
  }

  if (strlen(pi_cb->data.dadr)) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"dadr\" : \"%s\"",
      pi_cb->data.dadr);
  }

  for (i = 0; i < pi_cb->data.req_tokens_len; i++) {
    snprintf(&value[strlen(value)], value_len - strlen(value), ", \"%s\" : \"%s\"",
      pi_cb->data.req_tokens[i].key, pi_cb->data.req_tokens[i].value);
  }

  snprintf(&value[strlen(value)], value_len - strlen(value), ", \"value\" : {");

  for (i = 0, first = true; i < pi_cb->data.points_len; i++) {
    pe_cb = &pi_cb->data.points[i];
    if (pe_cb->error) {
      snprintf(&value[strlen(value)], value_len - strlen(value),
        "%s\"%s-%d\" : {\"error\" : \"%s\"}", ((first) ? "" : ","),
        get_object_type_str(pe_cb->object_type), pe_cb->instance, pe_cb->err_msg);
    } else {
      if (pe_cb->reply_okay) {
        snprintf(&value[strlen(value)], value_len - strlen(value),
          "%s\"%s-%d\" : {\"name\" : %s}", ((first) ? "" : ","),
          get_object_type_str(pe_cb->object_type), pe_cb->instance,
          pe_cb->point_name);
      } else {
        snprintf(&value[strlen(value)], value_len - strlen(value),
          "%s\"%s-%d\" : {\"error\" : \"%s\"}", ((first) ? "" : ","),
          get_object_type_str(pe_cb->object_type), pe_cb->instance,
          "Unknown object");
      }
    }

    if (first) {
      first = false;
    }
  }

  snprintf(&value[strlen(value)], value_len - strlen(value), "}}");
  publish_topic_value(topic, value);
  free(value);

  return(0);
}


/*
 * Process point discovery response.
 */
void process_point_disc_resp(uint8_t *service_request,
  uint16_t service_len, BACNET_ADDRESS *src, llist_point_disc_cb *pd_data)
{
  BACNET_READ_PROPERTY_DATA data;
  BACNET_OBJECT_PROPERTY_VALUE object_value;
  BACNET_APPLICATION_DATA_VALUE value;
  point_entry_cb *pe_cb;
  uint8_t *application_data;
  int application_data_len;
  int point_idx;
  int len;

  len = rp_ack_decode_service_request(service_request, service_len, &data);
  if (len < 0) {
    printf("<decode failed!>\n");
    return;
  }

  if (mqtt_debug) {
    rp_ack_print_data(&data);
    printf("data.object_type: %d\n", data.object_type);
    printf("data.object_property: %d\n", data.object_property);
  }

  if (data.object_type != OBJECT_DEVICE) {
    printf("Invalid object_type: %d\n", data.object_type);
    return;
  }

  memset((char *)&object_value, 0, sizeof(object_value));
  application_data = data.application_data;
  application_data_len = data.application_data_len;
  if (pd_data->state == POINT_DISC_STATE_GET_SIZE_SENT) {
    len = bacapp_decode_application_data(application_data, (unsigned)application_data_len, &value);
    if (value.tag != BACNET_APPLICATION_TAG_UNSIGNED_INT) {
      printf("Incorrect value.tag: %d\n", value.tag);
      return;
    }

    pd_data->data.total_points = (unsigned long)value.type.Unsigned_Int;
    pd_data->state = POINT_DISC_STATE_GET_POINTS;
    pd_data->timestamp = time(NULL);
  } else if (pd_data->state == POINT_DISC_STATE_GET_POINTS_SENT) {
    for (point_idx = 0; ; point_idx++) {
      len = bacapp_decode_application_data(application_data, (unsigned)application_data_len, &value);
      if (mqtt_debug) {
        printf("- [%d] object_type: %d , instance: %lu\n", point_idx, value.type.Object_Id.type, (unsigned long)value.type.Object_Id.instance);
      }

      pe_cb = calloc(1, sizeof(point_entry_cb));
      if (pe_cb == NULL) {
        printf("Failed to allocated point_entry_cb.\n");
        return;
      }

      pe_cb->object_type = value.type.Object_Id.type;
      pe_cb->instance = value.type.Object_Id.instance;
      if (pd_data->data.point_list.head == NULL) {
        pd_data->data.point_list.head = pd_data->data.point_list.tail = pe_cb;
      } else {
        pd_data->data.point_list.tail->next = pe_cb;
        pd_data->data.point_list.tail = pe_cb;
      }

      if (len > 0) {
        if (len < application_data_len) {
          application_data += len;
          application_data_len -= len;
        } else {
          break;
        }
      } else {
        break;
      }
    }

    pd_data->state = POINT_DISC_STATE_PUBLISH;
  } else {
    printf("- Invalid point discovery state: %d\n", pd_data->state);
  }
}


/*
 * Process points info response.
 */
void process_points_info_resp(uint32_t request_id, uint8_t *service_request,
  uint16_t service_len, BACNET_ADDRESS *src, llist_points_info_cb *pi_data)
{
  BACNET_READ_PROPERTY_DATA data;
  BACNET_OBJECT_PROPERTY_VALUE object_value;
  BACNET_APPLICATION_DATA_VALUE value;
  point_entry_cb *pe_cb = NULL;
  uint8_t *application_data;
  int application_data_len;
  int len;

  len = rp_ack_decode_service_request(service_request, service_len, &data);
  if (len < 0) {
    printf("<decode failed!>\n");
    return;
  }

  if (mqtt_debug) {
    rp_ack_print_data(&data);
    printf("data.object_type: %d\n", data.object_type);
    printf("data.object_property: %d\n", data.object_property);
  }

  for (int i = 0; i < pi_data->data.points_len; i++) {
    if (pi_data->data.points[i].request_id == request_id) {
      pe_cb = &pi_data->data.points[i];
    }
  }

  if (!pe_cb) {
    if (mqtt_debug) {
      printf("- points info entry not found for request_id: %d\n", request_id);
    }

    return;
  }

  pi_data->timestamp = time(NULL);
  pe_cb->reply_okay = true;
  if (data.object_property != PROP_OBJECT_NAME) {
    pe_cb->error = true;
    strcpy(pe_cb->err_msg, "Unexpected property");
    return;
  }

  memset((char *)&object_value, 0, sizeof(object_value));
  application_data = data.application_data;
  application_data_len = data.application_data_len;
  len = bacapp_decode_application_data(application_data, (unsigned)application_data_len, &value);
  object_value.object_type = data.object_type;
  object_value.object_instance = data.object_instance;
  object_value.object_property = data.object_property;
  object_value.array_index = data.array_index;
  object_value.value = &value;
  bacapp_snprintf_value(pe_cb->point_name, sizeof(pe_cb->point_name) - 1, &object_value);
}


/*
 * Bacnet client read value handler.
 */
static void bacnet_client_read_value_handler(uint8_t *service_request,
  uint16_t service_len,
  BACNET_ADDRESS *src,
  BACNET_CONFIRMED_SERVICE_ACK_DATA *service_data)
{
  BACNET_READ_PROPERTY_DATA data;
  BACNET_READ_ACCESS_DATA *rp_data;
  llist_obj_data obj_data = {0};
  llist_point_disc_cb *pd_data;
  llist_points_info_cb *pi_data;
  int len = 0;

  if (mqtt_debug) {
    printf("- bacnet_client_read_value_handler() => %d\n", getpid());
    printf("-- service_data->invoke_id: %d , pics_Request_Invoke_ID: %d\n", service_data->invoke_id, pics_Request_Invoke_ID);
  }

  if (get_bacnet_client_request_obj_data(service_data->invoke_id, &obj_data)) {
    if (mqtt_debug) {
      printf("-- Request with pending reply found!\n");
    }
    del_bacnet_client_request(service_data->invoke_id);
  } else if ((pd_data = get_bacnet_client_point_disc_req(service_data->invoke_id, src))) {
    if (mqtt_debug) {
      printf("-- Point Discovery request with pending reply found!\n");
    }
    process_point_disc_resp(service_request, service_len, src, pd_data);
    return;
  } else if ((pi_data = get_bacnet_client_points_info_req(service_data->invoke_id, src))) {
    if (mqtt_debug) {
      printf("-- Points Info request with pending reply found!\n");
    }
    process_points_info_resp(service_data->invoke_id, service_request, service_len, src, pi_data);
    return;
  } else if (address_match(&pics_Target_Address, src) &&
    (service_data->invoke_id == pics_Request_Invoke_ID)) {
    rp_data = calloc(1, sizeof(BACNET_READ_ACCESS_DATA));
    if (rp_data) {
      len = rp_ack_fully_decode_service_request(
        service_request, service_len, rp_data);
    }
    if (len > 0) {
      memmove(&pics_Read_Property_Multiple_Data.service_data, service_data,
        sizeof(BACNET_CONFIRMED_SERVICE_ACK_DATA));
      pics_Read_Property_Multiple_Data.rpm_data = rp_data;
      pics_Read_Property_Multiple_Data.new_data = true;
    } else {
      if (len < 0) { /* Eg, failed due to no segmentation */
        Error_Detected = true;
      }

      free(rp_data);
    }
    return;
  } else {
    printf("-- Request with pending reply NOT found! Discarding reply!\n");
    return;
  }

  len = rp_ack_decode_service_request(service_request, service_len, &data);
  if (len < 0) {
    printf("<decode failed!>\n");
  } else {
    if (mqtt_debug) {
      rp_ack_print_data(&data);
    }
    publish_bacnet_client_read_value_result(&data, &obj_data);
  }
}


/*
 * Bacnet client write value handler.
 */
static void bacnet_client_write_value_handler(
  BACNET_ADDRESS *src, uint8_t invoke_id)
{
  llist_obj_data obj_data;

  printf("- bacnet_client_write_value_handler() => %d\n", getpid());
  printf("-- invoke_id: %d\n", invoke_id);

  if (get_bacnet_client_request_obj_data(invoke_id, &obj_data)) {
    printf("-- Request with pending reply found!\n");
    del_bacnet_client_request(invoke_id);
  } else {
    printf("-- Request with pending reply NOT found! Discarding reply!\n");
    return;
  }

  if (!obj_data.dont_publish_on_success) {
    publish_bacnet_client_write_value_result(&obj_data);
  }
}


/*
 * Bacnet client whois handler.
 */
static void bacnet_client_whois_handler(uint8_t *service_request, uint16_t service_len,
  BACNET_ADDRESS *src)
{
  address_table_cb *table;
  int len = 0;
  uint32_t device_id = 0;
  unsigned max_apdu = 0;
  int segmentation = 0;
  uint16_t vendor_id = 0;
  unsigned i = 0;

  (void)service_len;
  len = iam_decode_service_request(
    service_request, &device_id, &max_apdu, &segmentation, &vendor_id);
  fprintf(stderr, "Received I-Am Request");

  if (len != -1) {
    fprintf(stderr, " from %lu, MAC = ", (unsigned long)device_id);
    if ((src->mac_len == 6) && (src->len == 0)) {
      fprintf(stderr, "%u.%u.%u.%u %02X%02X\n", (unsigned)src->mac[0],
        (unsigned)src->mac[1], (unsigned)src->mac[2],
        (unsigned)src->mac[3], (unsigned)src->mac[4],
        (unsigned)src->mac[5]);
    } else {
      for (i = 0; i < src->mac_len; i++) {
        fprintf(stderr, "%02X", (unsigned)src->mac[i]);
        if (i < (src->mac_len - 1)) {
          fprintf(stderr, ":");
        }
      }
      fprintf(stderr, "\n");
    }

    table = get_bacnet_client_first_whois_data();
    if (table) {
      printf("- Adding Whois address ...\n");
      whois_address_table_add(table, device_id, max_apdu, src);
    } else {
      printf("- WARNING: No existing whois request found!\n");
      if (len > 0) {
        printf("- Adding address for PICS command\n");
        address_add_binding(device_id, max_apdu, src);
      }
    }
  } else {
    fprintf(stderr, ", but unable to decode it.\n");
  }
}


/*
 * Bacnet client read multiple prop handler.
 */
static void bacnet_client_multiple_ack_handler(uint8_t *service_request,
  uint16_t service_len,
  BACNET_ADDRESS *src,
  BACNET_CONFIRMED_SERVICE_ACK_DATA *service_data)
{
  BACNET_READ_ACCESS_DATA *rpm_data;
  BACNET_READ_ACCESS_DATA *old_rpm_data;
  BACNET_PROPERTY_REFERENCE *rpm_property;
  BACNET_PROPERTY_REFERENCE *old_rpm_property;
  BACNET_APPLICATION_DATA_VALUE *value;
  BACNET_APPLICATION_DATA_VALUE *old_value;
  llist_obj_data obj_data;
  int len = 0;

  printf("- bacnet_client_multiple_ack_handler()\n");
  printf("-- invoke_id: %d , pics_Request_Invoke_ID: %d\n", service_data->invoke_id, pics_Request_Invoke_ID);

  if (address_match(&pics_Target_Address, src) &&
    (service_data->invoke_id == pics_Request_Invoke_ID)) {
    printf("-- Read PICS request start!\n");
    rpm_data = calloc(1, sizeof(BACNET_READ_ACCESS_DATA));
    if (rpm_data) {
      len = rpm_ack_decode_service_request(
        service_request, service_len, rpm_data);
    }

    if (len > 0) {
      memmove(&pics_Read_Property_Multiple_Data.service_data, service_data,
        sizeof(BACNET_CONFIRMED_SERVICE_ACK_DATA));
      pics_Read_Property_Multiple_Data.rpm_data = rpm_data;
      pics_Read_Property_Multiple_Data.new_data = true;
      /* Will process and free the RPM data later */
    } else {
      if (len < 0) { /* Eg, failed due to no segmentation */
        fprintf(stderr, "- Error_Detected\n");
        Error_Detected = true;
      }
      rpm_data = rpm_data_free(rpm_data);
      free(rpm_data);
    }
    printf("-- Read PICS request end!\n");
  } else if (get_bacnet_client_request_obj_data(service_data->invoke_id, &obj_data)) {
    printf("-- Read multiple Request with pending reply found!\n");
    del_bacnet_client_request(service_data->invoke_id);
    rpm_data = calloc(1, sizeof(BACNET_READ_ACCESS_DATA));
    if (rpm_data) {
      len = rpm_ack_decode_service_request(
        service_request, service_len, rpm_data);
    }

    if (len > 0) {
      while (rpm_data) {
        if (mqtt_debug) {
          rpm_ack_print_data(rpm_data);
        }

        encode_read_multiple_value_result(rpm_data, &obj_data);

        rpm_data = rpm_data_free(rpm_data);
      }
    } else {
      fprintf(stderr, "RPM Ack Malformed! Freeing memory...\n");
      while (rpm_data) {
        rpm_property = rpm_data->listOfProperties;
        while (rpm_property) {
          value = rpm_property->value;
          while (value) {
            old_value = value;
            value = value->next;
            free(old_value);
          }

          old_rpm_property = rpm_property;
          rpm_property = rpm_property->next;
          free(old_rpm_property);
        }

        old_rpm_data = rpm_data;
        rpm_data = rpm_data->next;
        free(old_rpm_data);
      }
    }

    publish_bacnet_client_read_multiple_value_result(&obj_data);
  } else {
    printf("-- read multiple request with pending reply NOT found! Discarding reply!\n");
    return;
  }
}


/*
 * Bacnet client write multiple prop handler.
 */
static void bacnet_client_write_multiple_ack_handler(
  BACNET_ADDRESS *src, uint8_t invoke_id)
{
  llist_obj_data obj_data;

  printf("- bacnet_client_write_multiple_ack_handler()\n");
  printf("-- invoke_id: %d\n", invoke_id);

  if (get_bacnet_client_request_obj_data(invoke_id, &obj_data)) {
    printf("-- Write multiple Request with pending reply found!\n");
    del_bacnet_client_request(invoke_id);
  } else {
    printf("-- Write multiple request with pending reply NOT found! Discarding reply!\n");
    return;
  }

  publish_bacnet_client_write_multiple_value_result(&obj_data);
}


/*
 * Bacnet client error handler.
 */
static void bacnet_client_error_handler(BACNET_ADDRESS *src,
    uint8_t invoke_id,
    BACNET_ERROR_CLASS error_class,
    BACNET_ERROR_CODE error_code)
{
  llist_cb *ptr, *tmp = NULL, *prev = NULL;
  char err_msg[1024];
  bacnet_client_cmd_opts opts;

  if (mqtt_debug) {
    printf("BACnet Client Error: %s: %s (invoke_id: %d)\n",
      bactext_error_class_name((int)error_class),
      bactext_error_code_name((int)error_code), invoke_id);
  }

  Error_Detected = true;
  Last_Error_Class = error_class;
  Last_Error_Code = error_code;

  if (is_request_list_locked()) {
    return;
  } else {
    lock_request_list();
  }

  ptr = bc_request_list_head;
  while (ptr != NULL) {
    if (ptr->data.invoke_id == invoke_id) {
      if (mqtt_debug) {
        printf("- Pending Request found: %d\n", ptr->data.invoke_id);
      }

      tmp = ptr;
      if (ptr == bc_request_list_head) {
        bc_request_list_head = ptr->next;
        ptr = bc_request_list_head;
      } else {
        prev->next = ptr->next;
        if (ptr == bc_request_list_tail) {
          bc_request_list_tail = prev;
        }

        ptr = ptr->next;
      }

      break;
    }

    prev = ptr;
    ptr = ptr->next;
  }

  unlock_request_list();

  if (tmp) {
    init_cmd_opts_from_list_cb(&opts, &tmp->data.obj_data);
    sprintf(err_msg, "%s", bactext_error_code_name((int)error_code));
    mqtt_publish_command_error(err_msg, &opts, tmp->data.obj_data.topic_id);
    free(tmp);
  }
}


static void bacnet_client_abort_handler(
  BACNET_ADDRESS *src, uint8_t invoke_id, uint8_t abort_reason, bool server)
{
  llist_cb *ptr, *tmp = NULL, *prev = NULL;
  char err_msg[1024];
  bacnet_client_cmd_opts opts;

  (void)server;
printf("- bacnet_client_abort_handler()\n");
printf("-- invoke_id: %d , pics_Request_Invoke_ID: %d , abort_reason: %d\n",
  invoke_id, pics_Request_Invoke_ID, abort_reason);
  if (address_match(&pics_Target_Address, src) &&
    (invoke_id == pics_Request_Invoke_ID)) {
#if PRINT_ERRORS
    /* It is normal for this to fail, so don't print. */
    if ((myState != GET_ALL_RESPONSE) && !IsLongArray && ShowValues) {
      fprintf(stderr, "-- BACnet Abort: %s \n",
        bactext_abort_reason_name(abort_reason));
    }
#endif
    Error_Detected = true;
    Last_Error_Class = ERROR_CLASS_SERVICES;
    if (abort_reason < MAX_BACNET_ABORT_REASON) {
      Last_Error_Code =
        (ERROR_CODE_ABORT_BUFFER_OVERFLOW - 1) + abort_reason;
    } else {
      Last_Error_Code = ERROR_CODE_ABORT_OTHER;
    }

    return;
  }

  if (is_request_list_locked()) {
    return;
  } else {
    lock_request_list();
  }

  ptr = bc_request_list_head;
  while (ptr != NULL) {
    if (ptr->data.invoke_id == invoke_id) {
      if (mqtt_debug) {
        printf("- Pending Request found: %d\n", ptr->data.invoke_id);
      }

      tmp = ptr;
      if (ptr == bc_request_list_head) {
        bc_request_list_head = ptr->next;
        ptr = bc_request_list_head;
      } else {
        prev->next = ptr->next;
        if (ptr == bc_request_list_tail) {
          bc_request_list_tail = prev;
        }

        ptr = ptr->next;
      }

      break;
    }

    prev = ptr;
    ptr = ptr->next;
  }

  unlock_request_list();

  if (tmp) {
    init_cmd_opts_from_list_cb(&opts, &tmp->data.obj_data);
    snprintf(err_msg, sizeof(err_msg) - 1, "%s", bactext_abort_reason_name(abort_reason));
    mqtt_publish_command_error(err_msg, &opts, tmp->data.obj_data.topic_id);
    free(tmp);
  }
}


static void bacnet_client_reject_handler(
  BACNET_ADDRESS *src, uint8_t invoke_id, uint8_t reject_reason)
{
  llist_cb *ptr, *tmp = NULL, *prev = NULL;
  char err_msg[1024];
  bacnet_client_cmd_opts opts;

printf("- bacnet_client_reject_handler()\n");
printf("-- invoke_id: %d , pics_Request_Invoke_ID: %d , reject_reason: %d\n",
  invoke_id, pics_Request_Invoke_ID, reject_reason);
  if (address_match(&pics_Target_Address, src) &&
    (invoke_id == pics_Request_Invoke_ID)) {
#if PRINT_ERRORS
    if (ShowValues) {
      fprintf(stderr, "BACnet Reject: %s\n",
        bactext_reject_reason_name(reject_reason));
    }
#endif
    Error_Detected = true;
    Last_Error_Class = ERROR_CLASS_SERVICES;
    if (reject_reason < MAX_BACNET_REJECT_REASON) {
      Last_Error_Code =
        (ERROR_CODE_REJECT_BUFFER_OVERFLOW - 1) + reject_reason;
    } else {
      Last_Error_Code = ERROR_CODE_REJECT_OTHER;
    }

    return;
  }

  if (is_request_list_locked()) {
    return;
  } else {
    lock_request_list();
  }

  ptr = bc_request_list_head;
  while (ptr != NULL) {
    if (ptr->data.invoke_id == invoke_id) {
      if (mqtt_debug) {
        printf("- Pending Request found: %d\n", ptr->data.invoke_id);
      }

      tmp = ptr;
      if (ptr == bc_request_list_head) {
        bc_request_list_head = ptr->next;
        ptr = bc_request_list_head;
      } else {
        prev->next = ptr->next;
        if (ptr == bc_request_list_tail) {
          bc_request_list_tail = prev;
        }

        ptr = ptr->next;
      }

      break;
    }

    prev = ptr;
    ptr = ptr->next;
  }

  unlock_request_list();

  if (tmp) {
    init_cmd_opts_from_list_cb(&opts, &tmp->data.obj_data);
    snprintf(err_msg, sizeof(err_msg) - 1, "%s", bactext_reject_reason_name(reject_reason));
    mqtt_publish_command_error(err_msg, &opts, tmp->data.obj_data.topic_id);
    free(tmp);
  }
}


/*
 * Initialize bacnet client service handlers.
 */
void init_bacnet_client_service_handlers(void)
{
  apdu_set_confirmed_handler(
    SERVICE_CONFIRMED_READ_PROPERTY, handler_read_property);
  apdu_set_confirmed_ack_handler(
    SERVICE_CONFIRMED_READ_PROPERTY, bacnet_client_read_value_handler);
  apdu_set_confirmed_simple_ack_handler(
    SERVICE_CONFIRMED_WRITE_PROPERTY, bacnet_client_write_value_handler);
  apdu_set_unconfirmed_handler(SERVICE_UNCONFIRMED_WHO_IS, handler_who_is);
  apdu_set_unconfirmed_handler(SERVICE_UNCONFIRMED_I_AM, bacnet_client_whois_handler);

  apdu_set_confirmed_ack_handler(SERVICE_CONFIRMED_READ_PROP_MULTIPLE,
    bacnet_client_multiple_ack_handler);
  apdu_set_confirmed_simple_ack_handler(SERVICE_CONFIRMED_WRITE_PROP_MULTIPLE,
    bacnet_client_write_multiple_ack_handler);

  apdu_set_error_handler(SERVICE_CONFIRMED_READ_PROPERTY, bacnet_client_error_handler);
  apdu_set_error_handler(SERVICE_CONFIRMED_WRITE_PROPERTY, bacnet_client_error_handler);
  apdu_set_unrecognized_service_handler_handler(handler_unrecognized_service);
  apdu_set_abort_handler(bacnet_client_abort_handler);
  apdu_set_reject_handler(bacnet_client_reject_handler);
}


/*
 * Set Key-Value pairs for mqtty reply payload.
 */
void set_kvps_for_reply(json_key_value_pair *kvps, int kvps_len, int *kvps_outlen, bacnet_client_cmd_opts *opts)
{
  int i;

  for (i = 0; strlen(kvps[i].key); i++);

  if (opts->index != BACNET_ARRAY_ALL) {
    strcpy(kvps[i].key, "index");
    sprintf(kvps[i].value, "%d", opts->index);
    i++;
  }

  if (opts->priority != 0) {
    strcpy(kvps[i].key, "priority");
    sprintf(kvps[i].value, "%d", opts->priority);
    i++;
  }

  *kvps_outlen = i;
}


/*
 * Return object_type name.
 */
static char *object_type_id_to_name(BACNET_OBJECT_TYPE object_type)
{
  char *name = "";

  switch (object_type) {
    case OBJECT_DEVICE:
      name = "device";
      break;

    case OBJECT_ANALOG_INPUT:
      name = "analog-input";
      break;

    case OBJECT_ANALOG_OUTPUT:
      name = "analog-output";
      break;

    case OBJECT_ANALOG_VALUE:
      name = "analog-value";
      break;

    case OBJECT_BINARY_INPUT:
      name = "binary-input";
      break;

    case OBJECT_BINARY_OUTPUT:
      name = "binary-output";
      break;

    case OBJECT_BINARY_VALUE:
      name = "binary-value";
      break;

    case OBJECT_MULTI_STATE_INPUT:
      name = "multi-state-input";
      break;

    case OBJECT_MULTI_STATE_OUTPUT:
      name = "multi-state-output";
      break;

    case OBJECT_MULTI_STATE_VALUE:
      name = "multi-state-value";
      break;

    default:
      break;
  }

  return(name);
}


/*
 * Get device address.
 */
static void get_device_address(char *buf, int buf_len)
{
  BACNET_IP_ADDRESS addr = {0};
  uint16_t port = bip_get_port();

  bip_get_addr(&addr);
  snprintf(buf, buf_len - 1, "%u.%u.%u.%u:%u",
    addr.address[0], addr.address[1], addr.address[2], addr.address[3], port);
}


/*
 * Process local bacnet client read value command.
 */
int process_local_read_value_command(bacnet_client_cmd_opts *opts)
{
  BACNET_CHARACTER_STRING bs_val;
  BACNET_BINARY_PV b_array[BACNET_MAX_PRIORITY] = {0};
  BACNET_DEVICE_STATUS device_status;
  BACNET_OBJECT_TYPE object_type = OBJECT_NONE;
  uint32_t instance = 0;
  json_key_value_pair kvps[MAX_JSON_KEY_VALUE_PAIR] = {0};
  float f_array[BACNET_MAX_PRIORITY] = {0};
  float f_val;
  int i_val;
  int u_val;
  char s_val[MAX_TOPIC_VALUE_LENGTH] = {0};
  char s_obj_list[30000] = {0};
  char *s_ptr;
  bool found;
  int i, obj_n, kvps_length = 0;

  /* include device instance number in the reply */
  u_val = Device_Object_Instance_Number();
  strcpy(kvps[kvps_length].key, "deviceInstance");
  sprintf(kvps[kvps_length++].value, "%d", u_val);

  /* include device IP address in the reply */
  get_device_address(s_val, sizeof(s_val));
  if (strlen(s_val)) {
    strcpy(kvps[kvps_length].key, "mac");
    strcpy(kvps[kvps_length++].value, s_val);
  }

  if (opts->index >= 0) {
    strcpy(kvps[kvps_length].key, "index");
    sprintf(kvps[kvps_length++].value, "%d", opts->index);
  }

  /* include request tokens */
  for (i = 0; i < opts->req_tokens_len; i++) {
    strcpy(kvps[kvps_length].key, opts->req_tokens[i].key);
    strcpy(kvps[kvps_length++].value, opts->req_tokens[i].value);
  }

  switch (opts->object_type) {
    case OBJECT_ANALOG_INPUT:
      if (Analog_Input_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          f_val = Analog_Input_Present_Value(opts->object_instance);
          mqtt_publish_command_result(OBJECT_ANALOG_INPUT, opts->object_instance, PROP_PRESENT_VALUE, -1,
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_OBJECT_NAME:
          Analog_Input_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_ANALOG_INPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        default:
          return(1);
      }

      break;

    case OBJECT_ANALOG_OUTPUT:
      if (Analog_Output_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          f_val = Analog_Output_Present_Value(opts->object_instance);
          mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_PRESENT_VALUE, -1,
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_OBJECT_NAME:
          Analog_Output_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_PRIORITY_ARRAY:
          set_kvps_for_reply(&kvps[0], MAX_JSON_KEY_VALUE_PAIR, &kvps_length, opts);
          get_ao_priority_array(opts->object_instance, f_array, BACNET_MAX_PRIORITY);
          if (opts->index == 0) {
            i_val = BACNET_MAX_PRIORITY;
            mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
              MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          } else if (opts->index > 0 && opts->index <= BACNET_MAX_PRIORITY) {
            f_val = f_array[opts->index -1];
            if (f_val == AO_LEVEL_NULL) {
              mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
                MQTT_TOPIC_VALUE_STRING, "Null", MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            } else {
              mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
                MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            }
          } else if (opts->index == BACNET_ARRAY_ALL) {
            mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
              MQTT_TOPIC_VALUE_FLOAT_MAX_PRIO, f_array, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          } else {
            return(1);
          }
          break;

        default:
          return(1);
      }

      break;

    case OBJECT_ANALOG_VALUE:
      if (Analog_Value_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          f_val = Analog_Value_Present_Value(opts->object_instance);
          mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_PRESENT_VALUE, -1,
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_OBJECT_NAME:
          Analog_Value_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_PRIORITY_ARRAY:
          set_kvps_for_reply(&kvps[0], MAX_JSON_KEY_VALUE_PAIR, &kvps_length, opts);
          get_av_priority_array(opts->object_instance, f_array, BACNET_MAX_PRIORITY);
          if (opts->index == 0) {
            i_val = BACNET_MAX_PRIORITY;
            mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
              MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          } else if (opts->index > 0 && opts->index <= BACNET_MAX_PRIORITY) {
            f_val = f_array[opts->index -1];
            if (f_val == AV_LEVEL_NULL) {
              mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
                MQTT_TOPIC_VALUE_STRING, "Null", MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            } else {
              mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
                MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            }
          } else if (opts->index == BACNET_ARRAY_ALL) {
            mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
              MQTT_TOPIC_VALUE_FLOAT_MAX_PRIO, f_array, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          } else {
            return(1);
          }
          break;

        default:
          return(1);
      }

      break;

    case OBJECT_BINARY_INPUT:
      if (Binary_Input_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          i_val = Binary_Input_Present_Value(opts->object_instance);
          mqtt_publish_command_result(OBJECT_BINARY_INPUT, opts->object_instance, PROP_PRESENT_VALUE, -1,
            MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_OBJECT_NAME:
          Binary_Input_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_BINARY_INPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        default:
          return(1);
      }
        
      break;

    case OBJECT_BINARY_OUTPUT:
      if (Binary_Output_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }
        
      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          i_val = Binary_Output_Present_Value(opts->object_instance);
          mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_PRESENT_VALUE, -1,
            MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_OBJECT_NAME:
          Binary_Output_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_PRIORITY_ARRAY:
          set_kvps_for_reply(&kvps[0], MAX_JSON_KEY_VALUE_PAIR, &kvps_length, opts);
          get_bo_priority_array(opts->object_instance, b_array, BACNET_MAX_PRIORITY);
          if (opts->index == 0) {
            i_val = BACNET_MAX_PRIORITY;
            mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
              MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          } else if (opts->index > 0 && opts->index <= BACNET_MAX_PRIORITY) {
            i_val = b_array[opts->index -1];
            if (i_val == BINARY_NULL) {
              mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
                MQTT_TOPIC_VALUE_STRING, "Null", MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            } else {
              mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
                MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            }
          } else if (opts->index == BACNET_ARRAY_ALL) {
            mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
              MQTT_TOPIC_VALUE_BINARY_MAX_PRIO, b_array, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          } else {
            return(1);
          }
          break;

        default:
          return(1);
      }
        
      break;

    case OBJECT_BINARY_VALUE:
      if (Binary_Value_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          i_val = Binary_Value_Present_Value(opts->object_instance);
          mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_PRESENT_VALUE, -1,
            MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_OBJECT_NAME:
          Binary_Value_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_PRIORITY_ARRAY:
          set_kvps_for_reply(&kvps[0], MAX_JSON_KEY_VALUE_PAIR, &kvps_length, opts);
          get_bv_priority_array(opts->object_instance, b_array, BACNET_MAX_PRIORITY);
          if (opts->index == 0) {
            i_val = BACNET_MAX_PRIORITY;
            mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
              MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          } else if (opts->index > 0 && opts->index <= BACNET_MAX_PRIORITY) {
            i_val = b_array[opts->index -1];
            if (i_val == BINARY_NULL) {
              mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
                MQTT_TOPIC_VALUE_STRING, "Null", MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            } else {
              mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
                MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            }
          } else if (opts->index == BACNET_ARRAY_ALL) {
            mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_PRIORITY_ARRAY, -1,
              MQTT_TOPIC_VALUE_BINARY_MAX_PRIO, b_array, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          } else {
            return(1);
          }
          break;

        default:
          return(1);
      }

      break;

    case OBJECT_DEVICE:
      if (!Device_Valid_Object_Instance_Number(opts->object_instance)) {
        return(1);
      }

      switch(opts->property) {
        case PROP_OBJECT_NAME:
          Device_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_DEVICE, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_OBJECT_IDENTIFIER:
          u_val = Device_Object_Instance_Number();
          mqtt_publish_command_result(OBJECT_DEVICE, opts->object_instance, PROP_OBJECT_IDENTIFIER, -1,
            MQTT_TOPIC_VALUE_INTEGER, &u_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_SYSTEM_STATUS:
          device_status = Device_System_Status();
          device_status_to_str(device_status, s_val, sizeof(s_val));
          mqtt_publish_command_result(OBJECT_DEVICE, opts->object_instance, PROP_SYSTEM_STATUS, -1,
            MQTT_TOPIC_VALUE_STRING, s_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_VENDOR_NAME:
          s_ptr = (char *)Device_Vendor_Name();
          mqtt_publish_command_result(OBJECT_DEVICE, opts->object_instance, PROP_VENDOR_NAME, -1,
            MQTT_TOPIC_VALUE_STRING, s_ptr, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_VENDOR_IDENTIFIER:
          u_val = Device_Vendor_Identifier();
          mqtt_publish_command_result(OBJECT_DEVICE, opts->object_instance, PROP_VENDOR_IDENTIFIER, -1,
            MQTT_TOPIC_VALUE_INTEGER, &u_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_OBJECT_LIST:
          obj_n = Device_Object_List_Count();
          if (opts->index == 0) {
            i_val = obj_n;
            mqtt_publish_command_result(OBJECT_DEVICE, opts->object_instance, PROP_OBJECT_LIST, -1,
              MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          } else if (opts->index == BACNET_ARRAY_ALL) {
            if (obj_n > 0) {
              strcpy(s_obj_list, "[");
            }
            for (i = 1; i <= obj_n; i++) {
              found = Device_Object_List_Identifier(i, &object_type, &instance);
              if (found) {
                s_ptr = object_type_id_to_name(object_type);
                snprintf(&s_obj_list[strlen(s_obj_list)], sizeof(s_obj_list) - strlen(s_obj_list),
                  "%s[\"%s\", \"%d\"]", (i > 1) ? "," : "", s_ptr, instance + 1);
              }
            }

            if (obj_n > 0) {
              snprintf(&s_obj_list[strlen(s_obj_list)], sizeof(s_obj_list) - strlen(s_obj_list), "]");
            }
            mqtt_publish_command_result(OBJECT_DEVICE, opts->object_instance, PROP_OBJECT_LIST, -1,
              MQTT_TOPIC_VALUE_OBJECT_LIST, s_obj_list, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          } else {
            found = Device_Object_List_Identifier(opts->index, &object_type, &instance);
            if (found) {
              s_ptr = object_type_id_to_name(object_type);
              snprintf(&s_obj_list[strlen(s_obj_list)], sizeof(s_obj_list) - strlen(s_obj_list),
                "[\"%s\", \"%d\"]", s_ptr, instance + 1);
              mqtt_publish_command_result(OBJECT_DEVICE, opts->object_instance, PROP_OBJECT_LIST, -1,
              MQTT_TOPIC_VALUE_OBJECT_LIST, s_obj_list, MQTT_READ_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            }
          }
          break;

        default:
          return(1);
      }

      break;

    default:
      if (mqtt_debug) {
        printf("Unknown read value object_type: [%d]\n", opts->object_type);
      }
      return(1);
  }

  return(0);
}


/*
 * Publish read command error.
 */
int mqtt_publish_command_error(char *err_msg, bacnet_client_cmd_opts *opts, int topic_id)
{
  MQTTAsync_responseOptions mqtt_opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
  char topic[512];
  char topic_value[30000];
  int mqtt_retry_limit = yaml_config_mqtt_connect_retry_limit();
  int mqtt_retry_interval = yaml_config_mqtt_connect_retry_interval();
  int rc, i;

  if (!mqtt_create_error_topic(opts->object_type, opts->object_instance, opts->property, topic, sizeof(topic),
    topic_id)) {
    printf("Failed to create MQTT topic: %d/%d\n", opts->object_type, opts->property);
    return(1);
  }

  if (mqtt_debug) {
    printf("MQTT publish topic: %s\n", topic);
  }

  mqtt_opts.onSuccess = mqtt_on_send_success;
  mqtt_opts.onFailure = mqtt_on_send_failure;
  mqtt_opts.context = mqtt_client;

  snprintf(topic_value, sizeof(topic_value), "{ \"error\" : \"%s\" ", err_msg);
  snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"objectType\" : \"%d\" ",
    opts->object_type);
  snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"objectInstance\" : \"%d\" ",
    opts->object_instance);
  snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"property\" : \"%d\" ",
    opts->property);
  if (opts->index >= 0) {
    snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"index\" : \"%d\" ",
      opts->index);
  }
  if (opts->priority >= 0) {
    snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"priority\" : \"%d\" ",
      opts->priority);
  }
  snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"deviceInstance\" : \"%d\" ",
    opts->device_instance);
  if (strlen(opts->mac) > 0) {
    snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"mac\" : \"%s\" ",
      opts->mac);
  }

  if (strlen(opts->dadr) > 0) {
    snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"dadr\" : \"%s\" ",
      opts->dadr);
  }

  if (opts->dnet >= 0) {
    snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"dnet\" : \"%d\" ",
      opts->dnet);
  }

  if (strlen(opts->value) > 0) {
    snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"value\" : \"%s\" ",
      opts->value);
  }

  if (opts->prio_array_len > 0) {
    snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"priorityArray\" : { ");
    for (i = 0; i < opts->prio_array_len; i++) {
      snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), "%s\"%s\" : \"%s\"",
        (i > 0) ? ", " : "", opts->prio_array[i].key, opts->prio_array[i].value);
    }

    snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), "}");
  }

  for (i = 0; i < opts->req_tokens_len; i++) {
    snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), ", \"%s\" : \"%s\" ",
      opts->req_tokens[i].key, opts->req_tokens[i].value);
  }

  snprintf(&topic_value[strlen(topic_value)], sizeof(topic_value) - strlen(topic_value), "}");
  pubmsg.payload = topic_value;
  pubmsg.payloadlen = strlen(topic_value);

  if (yaml_config_mqtt_connect_retry() && mqtt_retry_limit > 0) {
    for (i = 0; i < mqtt_retry_limit && !mqtt_client_connected; i++) {
      if (mqtt_debug) {
        printf("MQTT reconnect retry: %d/%d\n", (i + 1), mqtt_retry_limit);
      }

      mqtt_check_reconnect();
      sleep(mqtt_retry_interval);
    }

    if (i >= mqtt_retry_limit) {
      if (mqtt_debug) {
        printf("MQTT reconnect limit reached: %d\n", i);
      }

      return(1);
    }
  }

  pubmsg.qos = DEFAULT_PUB_QOS;
  pubmsg.retained = 0;
  rc = MQTTAsync_sendMessage(mqtt_client, topic, &pubmsg, &mqtt_opts);
  if (mqtt_debug) {
    if (rc != MQTTASYNC_SUCCESS) {
      printf("MQTT failed to publish topic: \"%s\" , return code:%d\n", topic, rc);
    } else {
      printf("MQTT published topic: \"%s\"\n", topic);
    }
  }

  return(0);
}


/*
 * Process Bacnet client read value command.
 */
int process_bacnet_client_read_value_command(bacnet_client_cmd_opts *opts)
{
  uint8_t Rx_Buf[MAX_MPDU] = { 0 };
  BACNET_ADDRESS src = { 0 };
  BACNET_MAC_ADDRESS mac = { 0 };
  BACNET_MAC_ADDRESS dadr = { 0 };
  BACNET_ADDRESS dest = { 0 };
  int dnet = -1;
  llist_obj_data obj_data = { 0 };
  bool specific_address = false;
  unsigned timeout = 100;
  uint16_t pdu_len = 0;
  char err_msg[1024];
  int request_invoke_id;

  if (opts->rpm_objects_len > 0) {
    process_bacnet_client_read_multiple_value_command(opts);
    return(0);
  }

  if (opts->device_instance < 0 || opts->object_type < 0 || opts->object_instance < 0 ||
    opts->property < 0) {
    sprintf(err_msg, "Empty read value device_instance/object_type/object_instance/property: [%d/%d/%d/%d]",
      opts->device_instance, opts->object_type, opts->object_instance, opts->property);
    if (mqtt_debug) {
      printf("%s\n", err_msg);
    }

    mqtt_publish_command_error(err_msg, opts, MQTT_READ_VALUE_CMD_RESULT_TOPIC);

    return(1);
  }

  if (strlen(opts->mac)) {
    if (address_mac_from_ascii(&mac, opts->mac)) {
      specific_address = true;
    }
  }

  if (strlen(opts->dadr)) {
    if (address_mac_from_ascii(&dadr, opts->dadr)) {
      specific_address = true;
    }
  }

  if (opts->dnet >= 0) {
    dnet = opts->dnet;
    if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
      specific_address = true;
    }
  }

  if (specific_address) {
    if (dadr.len && mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      memcpy(&dest.adr[0], &dadr.adr[0], dadr.len);
      dest.len = dadr.len;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = BACNET_BROADCAST_NETWORK;
      }
    } else if (mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      dest.len = 0;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = 0;
      }
    } else {
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = BACNET_BROADCAST_NETWORK;
      }
      dest.mac_len = 0;
      dest.len = 0;
    }

    address_add(opts->device_instance, MAX_APDU, &dest);
  }

  /* if (is_command_for_us(opts)) {
    printf("*** Command for us\n");
    process_local_read_value_command(opts);
  } else { */
    if (opts->tag_flags & CMD_TAG_FLAG_SLOW_TEST) {
      printf("** Slow test enabled!\n");
      usleep(1000000);
    }

    obj_data.device_instance = opts->device_instance;
    obj_data.object_type = opts->object_type;
    obj_data.object_instance = opts->object_instance;
    obj_data.object_property = opts->property;
    obj_data.priority = opts->priority;
    obj_data.index = opts->index;
    obj_data.dnet = opts->dnet;
    if (strlen(opts->dadr) > 0) {
      strncpy(obj_data.dadr, opts->dadr, sizeof(obj_data.dadr) - 1);
    }
    strncpy(obj_data.mac, opts->mac, sizeof(obj_data.mac) - 1);
    memcpy(&obj_data.value, opts->value, sizeof(opts->value));
    if (opts->req_tokens_len > 0) {
      obj_data.req_tokens_len = opts->req_tokens_len;
      memcpy((char *)&obj_data.req_tokens[0], (char *)&opts->req_tokens[0],
        sizeof(request_token_cb) * opts->req_tokens_len);
    }

    obj_data.topic_id = MQTT_READ_VALUE_CMD_RESULT_TOPIC;

    request_invoke_id = Send_Read_Property_Request(opts->device_instance,
      opts->object_type, opts->object_instance, opts->property, opts->index);
    printf("read request_invoke_id: %d\n", request_invoke_id);

    add_bacnet_client_request(request_invoke_id, &obj_data);

    if (opts->tag_flags & CMD_TAG_FLAG_SLOW_TEST) {
      usleep(1000000);
    }

    if (opts->timeout > 0) {
      timeout = opts->timeout;
    }

    pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, timeout);
    if (pdu_len) {
      npdu_handler(&src, &Rx_Buf[0], pdu_len);
    }
  // }

  return(0);
}


/*
 * Process Bacnet client read multiple value command.
 */
int process_bacnet_client_read_multiple_value_command (bacnet_client_cmd_opts *opts)
{
  BACNET_READ_ACCESS_DATA *rad;
  BACNET_READ_ACCESS_DATA *rpm_object;
  BACNET_READ_ACCESS_DATA *old_rpm_object;
  BACNET_PROPERTY_REFERENCE *rpm_property;
  BACNET_PROPERTY_REFERENCE *old_rpm_property;
  uint8_t buffer[MAX_MPDU] = { 0 };
  uint8_t Rx_Buf[MAX_MPDU] = { 0 };
  BACNET_ADDRESS src = { 0 };
  BACNET_MAC_ADDRESS mac = { 0 };
  BACNET_MAC_ADDRESS dadr = { 0 };
  BACNET_ADDRESS dest = { 0 };
  int dnet = -1;
  llist_obj_data obj_data = { 0 };
  bool specific_address = false;
  unsigned timeout = 100;
  uint16_t pdu_len = 0;
  char err_msg[1024];
  int i, request_invoke_id;

  if (opts->device_instance < 0) {
    sprintf(err_msg, "Empty read multiple value device_instance: [%d]",
      opts->device_instance);
    if (mqtt_debug) {
      printf("%s\n", err_msg);
    }

    mqtt_publish_command_error(err_msg, opts, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
    return(1);
  }

  if (strlen(opts->mac)) {
    if (address_mac_from_ascii(&mac, opts->mac)) {
      specific_address = true;
    }
  }

  if (strlen(opts->dadr)) {
    if (address_mac_from_ascii(&dadr, opts->dadr)) {
      specific_address = true;
    }
  }

  if (opts->dnet >= 0) {
    dnet = opts->dnet;
    if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
      specific_address = true;
    }
  }

  if (specific_address) {
    if (dadr.len && mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      memcpy(&dest.adr[0], &dadr.adr[0], dadr.len);
      dest.len = dadr.len;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = BACNET_BROADCAST_NETWORK;
      }
    } else if (mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      dest.len = 0;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = 0;
      }
    } else {
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = BACNET_BROADCAST_NETWORK;
      }
      dest.mac_len = 0;
      dest.len = 0;
    }

    address_add(opts->device_instance, MAX_APDU, &dest);
  }

  obj_data.device_instance = opts->device_instance;
  obj_data.dnet = opts->dnet;
  if (strlen(opts->dadr) > 0) {
    strncpy(obj_data.dadr, opts->dadr, sizeof(obj_data.dadr) - 1);
  }
  strncpy(obj_data.mac, opts->mac, sizeof(obj_data.mac) - 1);
  obj_data.rpm_objects_len = opts->rpm_objects_len;
  obj_data.rpm_objects = malloc(opts->rpm_objects_len * sizeof(rpm_object_cb));
  memcpy(obj_data.rpm_objects, opts->rpm_objects, (opts->rpm_objects_len * sizeof(rpm_object_cb)));

  rad = calloc(1, sizeof(BACNET_READ_ACCESS_DATA));
  rpm_object = rad;
  for (i = 0; i < opts->rpm_objects_len; i++) {
    rpm_object->object_type = opts->rpm_objects[i].object_type;
    rpm_object->object_instance = opts->rpm_objects[i].object_instance;
    rpm_property = calloc(1, sizeof(BACNET_PROPERTY_REFERENCE));
    rpm_object->listOfProperties = rpm_property;
    rpm_property->propertyIdentifier = opts->rpm_objects[i].property;
    if (opts->rpm_objects[i].index >= 0) {
      rpm_property->propertyArrayIndex = opts->rpm_objects[i].index;
    } else {
      rpm_property->propertyArrayIndex = BACNET_ARRAY_ALL;
    }

    rpm_property->next = NULL;

    if ((i+1) < opts->rpm_objects_len) {
      rpm_object->next = calloc(1, sizeof(BACNET_READ_ACCESS_DATA));
      rpm_object = rpm_object->next;
    }
  }

  request_invoke_id = Send_Read_Property_Multiple_Request(
    &buffer[0], sizeof(buffer), opts->device_instance, rad);

  printf("read multiple request_invoke_id: %d\n", request_invoke_id);

  add_bacnet_client_request(request_invoke_id, &obj_data);

  pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, timeout);
  if (pdu_len) {
    npdu_handler(&src, &Rx_Buf[0], pdu_len);
  }

  rpm_object = rad;
  old_rpm_object = rpm_object;
  while (rpm_object) {
    rpm_property = rpm_object->listOfProperties;
    while (rpm_property) {
      old_rpm_property = rpm_property;
      rpm_property = rpm_property->next;
      free(old_rpm_property);
    }
    old_rpm_object = rpm_object;
    rpm_object = rpm_object->next;
    free(old_rpm_object);
  }

  return(0);
}


/*
 * Extract number for string.
 */
static int extract_num_from_string(char *s, int *num)
{
  char *ptr, *endptr;

  for(ptr = s; *ptr != '\0' && !isdigit(*ptr); ptr++);
  if (*ptr == '\0') {
    return(1);
  }

  *num = (int)strtol(ptr, &endptr, 10);
  if (ptr == endptr) {
    return(1);
  }

  return(0);
}


/*
 * Sanitize and update priority array set.
 */
static int check_and_set_priority_array_set(bacnet_client_cmd_opts *opts)
{
  int i, ret, index;

  for (i = 0; i < opts->prio_array_len; i++) {
    ret = extract_num_from_string(opts->prio_array[i].key, &index);
    if (ret || index < 1 || index > 16) {
      return(1);
    }

    opts->prio_array[i].index = index;
    if (!strcasecmp(opts->prio_array[i].value, "null")) {
      strcpy(opts->prio_array[i].value, "255");
    }

    switch(opts->object_type) {
      case OBJECT_BINARY_INPUT:
      case OBJECT_BINARY_OUTPUT:
      case OBJECT_BINARY_VALUE:
        if (strcmp(opts->prio_array[i].value, "0") &&
          strcmp(opts->prio_array[i].value, "1") &&
          strcmp(opts->prio_array[i].value, "255")) {
          return(1);
        }
        break;

      default:
        break;
    }
  }

  return(0);
}


/*
 * Process local bacnet client write value command.
 */
int process_local_write_value_command(bacnet_client_cmd_opts *opts)
{
  BACNET_CHARACTER_STRING bacnet_string;
  BACNET_BINARY_PV pv;
  json_key_value_pair kvps[MAX_JSON_KEY_VALUE_PAIR] = {0};
  int priority;
  int i_val;
  float f_val;
  int u_val;
  char s_val[MAX_TOPIC_VALUE_LENGTH] = {0};
  char *endptr;
  int i, ret, kvps_length = 0;

  /* check and set priority array set */
  if (opts->prio_array_len > 0) {
    ret = check_and_set_priority_array_set(opts);
    if (ret) {
      if (mqtt_debug) {
        printf("Invalid priority array set!\n");
      }

      return(1);
    }
  }

  /* include device instance number in the reply */
  u_val = Device_Object_Instance_Number();
  strcpy(kvps[kvps_length].key, "deviceInstance");
  sprintf(kvps[kvps_length++].value, "%d", u_val);

  /* include device IP address in the reply */
  get_device_address(s_val, sizeof(s_val));
  if (strlen(s_val)) {
    strcpy(kvps[kvps_length].key, "mac");
    strcpy(kvps[kvps_length++].value, s_val);
  }

  /* include request tokens */
  for (i = 0; i < opts->req_tokens_len; i++) {
    strcpy(kvps[kvps_length].key, opts->req_tokens[i].key);
    strcpy(kvps[kvps_length++].value, opts->req_tokens[i].value);
  }

  switch (opts->object_type) {
    case OBJECT_ANALOG_INPUT:
      if (Analog_Input_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          f_val = strtof(opts->value, &endptr);
          Analog_Input_Present_Value_Set(opts->object_instance, f_val, NULL, true);
          f_val = Analog_Input_Present_Value(opts->object_instance);
          mqtt_publish_command_result(OBJECT_ANALOG_INPUT, opts->object_instance, PROP_PRESENT_VALUE, -1,
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;
        
        case PROP_OBJECT_NAME:
          characterstring_init_ansi(&bacnet_string, opts->value);
          Analog_Input_Set_Object_Name(opts->object_instance, &bacnet_string, NULL, true);
          mqtt_publish_command_result(OBJECT_ANALOG_INPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bacnet_string, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        default:
          return(1);
      }

      break;

    case OBJECT_ANALOG_OUTPUT:
      if (Analog_Output_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      if (opts->prio_array_len > 0) {
        printf("- Writing AO priority array\n");
        for (i = 0; i < opts->prio_array_len; i++) {
          if (mqtt_debug) {
            printf("- [%d] index: [%d] , value: [%s]\n", i, opts->prio_array[i].index, opts->prio_array[i].value);
          }
          f_val = strtof(opts->prio_array[i].value, &endptr);
          Analog_Output_Present_Value_Set(opts->object_instance, f_val, opts->prio_array[i].index, NULL, true);
        }
        mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_PRIORITY_ARRAY, 0,
          MQTT_TOPIC_VALUE_FLOAT_PRIO_ARRAY, &opts->prio_array[0], MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
      } else {
        priority = (opts->priority) ? opts->priority : BACNET_MAX_PRIORITY;
        switch(opts->property) {
          case PROP_PRESENT_VALUE:
            f_val = strtof(opts->value, &endptr);
            Analog_Output_Present_Value_Set(opts->object_instance, f_val, priority, NULL, true);
            f_val = Analog_Output_Present_Value(opts->object_instance);
            mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_PRESENT_VALUE, opts->priority,
              MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            break;

          case PROP_OBJECT_NAME:
            characterstring_init_ansi(&bacnet_string, opts->value);
            Analog_Output_Set_Object_Name(opts->object_instance, &bacnet_string, NULL, true);
            mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
              MQTT_TOPIC_VALUE_BACNET_STRING, &bacnet_string, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            break;

          default:
            return(1);
        }
      }

      break;

    case OBJECT_ANALOG_VALUE:
      if (Analog_Value_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      if (opts->prio_array_len > 0) {
        printf("- Writing AV priority array\n");
        for (i = 0; i < opts->prio_array_len; i++) {
          if (mqtt_debug) {
            printf("- [%d] index: [%d] , value: [%s]\n", i, opts->prio_array[i].index, opts->prio_array[i].value);
          }
          f_val = strtof(opts->prio_array[i].value, &endptr);
          Analog_Value_Present_Value_Set(opts->object_instance, f_val, opts->prio_array[i].index, NULL, true);
        }
        mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_PRIORITY_ARRAY, 0,
          MQTT_TOPIC_VALUE_FLOAT_PRIO_ARRAY, &opts->prio_array[0], MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
      } else {
        priority = (opts->priority) ? opts->priority : BACNET_MAX_PRIORITY;
        switch(opts->property) {
          case PROP_PRESENT_VALUE:
            f_val = strtof(opts->value, &endptr);
            Analog_Value_Present_Value_Set(opts->object_instance, f_val, priority, NULL, true);
            f_val = Analog_Value_Present_Value(opts->object_instance);
            mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_PRESENT_VALUE, opts->priority,
              MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            break;

          case PROP_OBJECT_NAME:
            characterstring_init_ansi(&bacnet_string, opts->value);
            Analog_Value_Set_Object_Name(opts->object_instance, &bacnet_string, NULL, true);
            mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_OBJECT_NAME, -1,
              MQTT_TOPIC_VALUE_BACNET_STRING, &bacnet_string, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            break;

          default:
            return(1);
        }
      }

      break;

    case OBJECT_BINARY_INPUT:
      if (Binary_Input_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          i_val = atoi(opts->value);
          pv = (i_val == 0) ? 0 : 1;
          Binary_Input_Present_Value_Set(opts->object_instance, pv, NULL, true);
          mqtt_publish_command_result(OBJECT_BINARY_INPUT, opts->object_instance, PROP_PRESENT_VALUE, -1,
            MQTT_TOPIC_VALUE_INTEGER, &pv, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        case PROP_OBJECT_NAME:
          characterstring_init_ansi(&bacnet_string, opts->value);
          Binary_Input_Set_Object_Name(opts->object_instance, &bacnet_string, NULL, true);
          mqtt_publish_command_result(OBJECT_BINARY_INPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bacnet_string, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
          break;

        default:
          return(1);
      }

      break;

    case OBJECT_BINARY_OUTPUT:
      if (Binary_Output_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      if (opts->prio_array_len > 0) {
        printf("- Writing BO priority array\n");
        for (i = 0; i < opts->prio_array_len; i++) {
          if (mqtt_debug) {
            printf("- [%d] index: [%d] , value: [%s]\n", i, opts->prio_array[i].index, opts->prio_array[i].value);
          }
          pv = atoi(opts->prio_array[i].value);
          Binary_Output_Present_Value_Set(opts->object_instance, pv, opts->prio_array[i].index, NULL, true);
        }
        mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_PRIORITY_ARRAY, 0,
          MQTT_TOPIC_VALUE_BINARY_PRIO_ARRAY, &opts->prio_array[0], MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
      } else {
        priority = (opts->priority) ? opts->priority : BACNET_MAX_PRIORITY;
        switch(opts->property) {
          case PROP_PRESENT_VALUE:
            i_val = atoi(opts->value);
            pv = (i_val == 0) ? 0 : 1;
            Binary_Output_Present_Value_Set(opts->object_instance, pv, priority, NULL, true);
            mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_PRESENT_VALUE, opts->priority,
              MQTT_TOPIC_VALUE_INTEGER, &pv, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            break;

          case PROP_OBJECT_NAME:
            characterstring_init_ansi(&bacnet_string, opts->value);
            Binary_Output_Set_Object_Name(opts->object_instance, &bacnet_string, NULL, true);
            mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
              MQTT_TOPIC_VALUE_BACNET_STRING, &bacnet_string, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            break;

          default:
            return(1);
        }
      }

      break;

    case OBJECT_BINARY_VALUE:
      if (Binary_Value_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      if (opts->prio_array_len > 0) {
        printf("- Writing BV priority array\n");
        for (i = 0; i < opts->prio_array_len; i++) {
          if (mqtt_debug) {
            printf("- [%d] index: [%d] , value: [%s]\n", i, opts->prio_array[i].index, opts->prio_array[i].value);
          }
          pv = atoi(opts->prio_array[i].value);
          Binary_Value_Present_Value_Set(opts->object_instance, pv, opts->prio_array[i].index, NULL, true);
        }
        mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_PRIORITY_ARRAY, 0,
          MQTT_TOPIC_VALUE_BINARY_PRIO_ARRAY, &opts->prio_array[0], MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
      } else {
        priority = (opts->priority) ? opts->priority : BACNET_MAX_PRIORITY;
        switch(opts->property) {
          case PROP_PRESENT_VALUE:
            i_val = atoi(opts->value);
            pv = (i_val == 0) ? 0 : 1;
            Binary_Value_Present_Value_Set(opts->object_instance, pv, priority, NULL, true);
            mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_PRESENT_VALUE, opts->priority,
              MQTT_TOPIC_VALUE_INTEGER, &pv, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            break;

          case PROP_OBJECT_NAME:
            characterstring_init_ansi(&bacnet_string, opts->value);
            Binary_Value_Set_Object_Name(opts->object_instance, &bacnet_string, NULL, true);
            mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_OBJECT_NAME, -1,
              MQTT_TOPIC_VALUE_BACNET_STRING, &bacnet_string, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, kvps, kvps_length);
            break;

          default:
            return(1);
        }
      }

      break;

    default:
      if (mqtt_debug) {
        printf("Unknown read value object_type: [%d]\n", opts->object_type);
      }
      return(1);
  }

  return(0);
}


/*
 * Set bacnet application data value from a string.
 */
static int set_app_data_value_from_string(int object_type, int object_property, char *str, BACNET_APPLICATION_DATA_VALUE *value)
{
  switch (object_type) {
    case OBJECT_ANALOG_INPUT:
    case OBJECT_ANALOG_OUTPUT:
    case OBJECT_ANALOG_VALUE:
      switch (object_property) {
        case PROP_PRESENT_VALUE:
        case PROP_PRIORITY_ARRAY:
          bacapp_parse_application_data(BACNET_APPLICATION_TAG_REAL, str, value);
          break;

        case PROP_OBJECT_NAME:
          bacapp_parse_application_data(BACNET_APPLICATION_TAG_CHARACTER_STRING, str, value);
          break;

        case PROP_UNITS:
          bacapp_parse_application_data(BACNET_APPLICATION_TAG_ENUMERATED, str, value);
          break;

        default:
          if (mqtt_debug) {
            printf("Unknown object property: %d\n", object_property);
          }
          return(1);
      }
      break;

    case OBJECT_BINARY_INPUT:
    case OBJECT_BINARY_OUTPUT:
    case OBJECT_BINARY_VALUE:
      switch (object_property) {
        case PROP_PRESENT_VALUE:
        case PROP_PRIORITY_ARRAY:
          bacapp_parse_application_data(BACNET_APPLICATION_TAG_ENUMERATED, str, value);
          break;

        case PROP_OBJECT_NAME:
          bacapp_parse_application_data(BACNET_APPLICATION_TAG_CHARACTER_STRING, str, value);
          break;

        default:
          if (mqtt_debug) {
            printf("Unknown object property: %d\n", object_property);
          }
          return(1);
      } 
      break;

    case OBJECT_MULTI_STATE_INPUT:
    case OBJECT_MULTI_STATE_OUTPUT:
    case OBJECT_MULTI_STATE_VALUE:
      switch (object_property) {
        case PROP_PRESENT_VALUE:
          bacapp_parse_application_data(BACNET_APPLICATION_TAG_UNSIGNED_INT, str, value);
          break;

        case PROP_OBJECT_NAME:
          bacapp_parse_application_data(BACNET_APPLICATION_TAG_CHARACTER_STRING, str, value);
          break;

        default:
          if (mqtt_debug) {
            printf("Unknown object property: %d\n", object_property);
          }
          return(1);
      }
      break;

    default:
      if (mqtt_debug) {
        printf("Unknown object type: %d\n", object_type);
      }
      return(1);
  }

  return(0);
}


/*
 * Send bacnet write request.
 */
static int send_write_request(llist_obj_data *obj_data, BACNET_APPLICATION_DATA_VALUE *value,
  bacnet_client_cmd_opts *opts, bool send_reply)
{
  uint8_t Rx_Buf[MAX_MPDU] = {0};
  BACNET_ADDRESS src = {0};
  unsigned timeout = 100;
  uint16_t pdu_len;
  int request_invoke_id;

  request_invoke_id = Send_Write_Property_Request(
    obj_data->device_instance, obj_data->object_type,
    obj_data->object_instance, obj_data->object_property,
    value,
    obj_data->priority,
    obj_data->index);

  if (!send_reply) {
    obj_data->dont_publish_on_success = true;
  }

    printf("- queuing request_invoke_id %d for reply\n", request_invoke_id);
    add_bacnet_client_request(request_invoke_id, obj_data);

    if (opts->timeout > 0) {
      timeout = opts->timeout;
    }

    pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, timeout);
    if (pdu_len) {
      npdu_handler(&src, &Rx_Buf[0], pdu_len);
    }

  return(0);
}


/*
 * Process Bacnet client write multiple value command.
 */
int process_bacnet_client_write_multiple_value_command(bacnet_client_cmd_opts *opts)
{
  BACNET_WRITE_ACCESS_DATA *wad;
  BACNET_WRITE_ACCESS_DATA *wpm_object;
  BACNET_PROPERTY_VALUE *wpm_property;
  BACNET_WRITE_ACCESS_DATA *old_wpm_object;
  BACNET_PROPERTY_VALUE *old_wpm_property;
  uint8_t buffer[MAX_MPDU] = { 0 };
  uint8_t Rx_Buf[MAX_MPDU] = { 0 };
  BACNET_ADDRESS src = { 0 };
  BACNET_MAC_ADDRESS mac = { 0 };
  BACNET_MAC_ADDRESS dadr = { 0 };
  BACNET_ADDRESS dest = { 0 };
  int dnet = -1;
  llist_obj_data obj_data = { 0 };
  bool specific_address = false;
  bool is_error = false;
  unsigned timeout = 100;
  uint16_t pdu_len = 0;
  char err_msg[1024];
  int i, ret, request_invoke_id;

  if (opts->device_instance < 0) {
    sprintf(err_msg, "Empty write value device_instance: [%d]",
      opts->device_instance);
    if (mqtt_debug) {
      printf("%s\n", err_msg);
    }

    mqtt_publish_command_error(err_msg, opts, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
    return(1);
  }

  if (strlen(opts->mac)) {
    if (address_mac_from_ascii(&mac, opts->mac)) {
      specific_address = true;
    }
  }

  if (strlen(opts->dadr)) {
    if (address_mac_from_ascii(&dadr, opts->dadr)) {
      specific_address = true;
    }
  }

  if (opts->dnet >= 0) {
    dnet = opts->dnet;
    if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
      specific_address = true;
    }
  }

  if (specific_address) {
    if (dadr.len && mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      memcpy(&dest.adr[0], &dadr.adr[0], dadr.len);
      dest.len = dadr.len;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = BACNET_BROADCAST_NETWORK;
      }
    } else if (mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      dest.len = 0;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = 0;
      }
    }

    address_add(opts->device_instance, MAX_APDU, &dest);
  }

  obj_data.device_instance = opts->device_instance;
  obj_data.dnet = opts->dnet;
  if (strlen(opts->dadr) > 0) {
    strncpy(obj_data.dadr, opts->dadr, sizeof(obj_data.dadr) - 1);
  }
  strncpy(obj_data.mac, opts->mac, sizeof(obj_data.mac) - 1);
  obj_data.rpm_objects_len = opts->rpm_objects_len;
  obj_data.rpm_objects = malloc(opts->rpm_objects_len * sizeof(rpm_object_cb));
  memcpy(obj_data.rpm_objects, opts->rpm_objects, (opts->rpm_objects_len * sizeof(rpm_object_cb)));

  wad = calloc(1, sizeof(BACNET_WRITE_ACCESS_DATA));
  wpm_object = wad;
  for (i = 0; i < opts->rpm_objects_len; i++) {
    wpm_object->object_type = opts->rpm_objects[i].object_type;
    wpm_object->object_instance = opts->rpm_objects[i].object_instance;
    wpm_property = calloc(1, sizeof(BACNET_PROPERTY_VALUE));
    wpm_object->listOfProperties = wpm_property;
    wpm_property->propertyIdentifier = opts->rpm_objects[i].property;
    if (opts->rpm_objects[i].index > 0) {
      wpm_property->propertyArrayIndex = opts->rpm_objects[i].index;
    } else {
      wpm_property->propertyArrayIndex = BACNET_ARRAY_ALL;
    }

    wpm_property->priority = opts->rpm_objects[i].priority;
    wpm_property->value.context_specific = false;
    ret = set_app_data_value_from_string(
      opts->rpm_objects[i].object_type,
      opts->rpm_objects[i].property,
      opts->rpm_objects[i].value,
      &wpm_property->value);
    if (ret) {
      printf("- Error parsing RPM value: [%s]", opts->rpm_objects[i].value);
      snprintf(err_msg, sizeof(err_msg) - 1,
        "Error parsing RPM value: %s for object: %d , property: %d",
        opts->rpm_objects[i].value,
        opts->rpm_objects[i].object_type,
        opts->rpm_objects[i].property);
      is_error = true;
      goto EXIT;
    }
    wpm_property->value.next = NULL;

    if ((i+1) < opts->rpm_objects_len) {
      wpm_object->next = calloc(1, sizeof(BACNET_WRITE_ACCESS_DATA));
      wpm_object = wpm_object->next;
    }
  }

  if (!is_error) {
    request_invoke_id = Send_Write_Property_Multiple_Request(
      &buffer[0], sizeof(buffer), opts->device_instance, wad);
    printf("write multiple request_invoke_id: %d\n", request_invoke_id);

    add_bacnet_client_request(request_invoke_id, &obj_data);

    pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, timeout);
    if (pdu_len) {
      npdu_handler(&src, &Rx_Buf[0], pdu_len);
    }
  }

  EXIT:

  wpm_object = wad;
  old_wpm_object = wpm_object;
  while (wpm_object) {
    wpm_property = wpm_object->listOfProperties;
    while (wpm_property) {
      old_wpm_property = wpm_property;
      wpm_property = wpm_property->next;
      free(old_wpm_property);
    }
    old_wpm_object = wpm_object;
    wpm_object = wpm_object->next;
    free(old_wpm_object);
  }

  if (is_error) {
    strncpy(obj_data.err_msg, err_msg, sizeof(obj_data.err_msg) - 1);
    publish_bacnet_client_write_multiple_value_result(&obj_data);
  }

  return(0);
}


/*
 * Process Bacnet client write value command.
 */
int process_bacnet_client_write_value_command(bacnet_client_cmd_opts *opts)
{
  uint8_t Rx_Buf[MAX_MPDU] = { 0 };
  BACNET_ADDRESS src = { 0 };
  BACNET_APPLICATION_DATA_VALUE value;
  BACNET_MAC_ADDRESS mac = { 0 };
  BACNET_MAC_ADDRESS dadr = { 0 };
  BACNET_ADDRESS dest = { 0 };
  int dnet = -1;
  llist_obj_data obj_data = { 0 };
  bool send_reply, specific_address = false;
  unsigned timeout = 100;
  uint16_t pdu_len = 0;
  char err_msg[1024];
  int i, ret, request_invoke_id;

  if (opts->rpm_objects_len > 0) {
    process_bacnet_client_write_multiple_value_command(opts);
    return(0);
  }

  if (opts->device_instance < 0 || opts->object_type < 0 || opts->object_instance < 0 ||
    opts->property < 0) {
    sprintf(err_msg, "Empty write value device_instance/object_type/object_instance/property: [%d/%d/%d/%d]",
      opts->device_instance, opts->object_type, opts->object_instance, opts->property);
    if (mqtt_debug) {
      printf("%s\n", err_msg);
    }

    mqtt_publish_command_error(err_msg, opts, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
    return(1);
  }

  if (strlen(opts->mac)) {
    if (address_mac_from_ascii(&mac, opts->mac)) {
      specific_address = true;
    }
  }

  if (strlen(opts->dadr)) {
    if (address_mac_from_ascii(&dadr, opts->dadr)) {
      specific_address = true;
    }
  }

  if (opts->dnet >= 0) {
    dnet = opts->dnet;
    if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
      specific_address = true;
    }
  }

  if (specific_address) {
    if (dadr.len && mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      memcpy(&dest.adr[0], &dadr.adr[0], dadr.len);
      dest.len = dadr.len;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = BACNET_BROADCAST_NETWORK;
      }
    } else if (mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      dest.len = 0;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = 0;
      }
    }

    address_add(opts->device_instance, MAX_APDU, &dest);
  }

  if (!strlen(opts->value) && !opts->prio_array_len) {
    sprintf(err_msg, "Invalid/empty write value object value");
    if (mqtt_debug) {
      printf("%s\n", err_msg);
    }

    mqtt_publish_command_error(err_msg, opts, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
    return(1);
  }

  switch (opts->object_type) {
    case OBJECT_ANALOG_INPUT:
      if (opts->property != PROP_OBJECT_NAME && opts->property != PROP_PRESENT_VALUE &&
        opts->property != PROP_UNITS) {
        sprintf(err_msg, "Unsupported property: %d of object_type %d", opts->property, opts->object_type);
        if (mqtt_debug) {
          printf("%s\n", err_msg);
        }

        mqtt_publish_command_error(err_msg, opts, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
        return(1);
      }
      break;

    case OBJECT_BINARY_INPUT:
      if (opts->property != PROP_OBJECT_NAME && opts->property != PROP_PRESENT_VALUE) {
        sprintf(err_msg, "Unsupported property: %d of object_type %d", opts->property, opts->object_type);
        if (mqtt_debug) {
          printf("%s\n", err_msg);
        }

        mqtt_publish_command_error(err_msg, opts, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
        return(1);
      }
      break;

    case OBJECT_ANALOG_OUTPUT:
    case OBJECT_ANALOG_VALUE:
      if (opts->property != PROP_OBJECT_NAME && opts->property != PROP_PRESENT_VALUE &&
        opts->property != PROP_PRIORITY_ARRAY && opts->property != PROP_UNITS) {
        sprintf(err_msg, "Unsupported property: %d of object_type %d", opts->property, opts->object_type);
        if (mqtt_debug) {
          printf("%s\n", err_msg);
        }

        mqtt_publish_command_error(err_msg, opts, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
        return(1);
      }
      break;

    case OBJECT_BINARY_OUTPUT:
    case OBJECT_BINARY_VALUE:
      if (opts->property != PROP_OBJECT_NAME && opts->property != PROP_PRESENT_VALUE &&
        opts->property != PROP_PRIORITY_ARRAY) {
        sprintf(err_msg, "Unsupported property: %d of object_type %d", opts->property, opts->object_type);
        if (mqtt_debug) {
          printf("%s\n", err_msg);
        }

        mqtt_publish_command_error(err_msg, opts, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
        return(1);
      }
      break;

    case OBJECT_MULTI_STATE_INPUT:
    case OBJECT_MULTI_STATE_OUTPUT:
    case OBJECT_MULTI_STATE_VALUE:
      if (opts->property != PROP_OBJECT_NAME && opts->property != PROP_PRESENT_VALUE) {
        sprintf(err_msg, "Unsupported property: %d of object_type %d", opts->property, opts->object_type);
        if (mqtt_debug) {
          printf("%s\n", err_msg);
        }
    
        mqtt_publish_command_error(err_msg, opts, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
        return(1);
      }
      break;

    default: 
      sprintf(err_msg, "Unknown object type: %d", opts->object_type);
      if (mqtt_debug) {
        printf("%s\n", err_msg);
      }

      mqtt_publish_command_error(err_msg, opts, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
      return(1);
  }

  /* if (is_command_for_us(opts)) {
    printf("command for us\n");
    process_local_write_value_command(opts);
  } else { */
    if (opts->prio_array_len > 0) {
      ret = check_and_set_priority_array_set(opts);
      if (ret) {
        sprintf(err_msg, "Invalid priority array key/value pairs");
        if (mqtt_debug) {
          printf("%s\n", err_msg);
        }

        mqtt_publish_command_error(err_msg, opts, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
        return(1);
      }

      send_reply = false;
      for (i = 0; i < opts->prio_array_len; i++) {
        if (mqtt_debug) {
          printf("- [%d] index: [%d] , value: [%s]\n", i, opts->prio_array[i].index, opts->prio_array[i].value);
        }

        memset(&value, 0, sizeof(value));
        value.context_specific = false;
        set_app_data_value_from_string(opts->object_type, PROP_PRIORITY_ARRAY, opts->prio_array[i].value, &value);

        memset(&obj_data, 0, sizeof(obj_data));
        obj_data.device_instance = opts->device_instance;
        obj_data.object_type = opts->object_type;
        obj_data.object_instance = opts->object_instance;
        obj_data.object_property = PROP_PRIORITY_ARRAY;
        obj_data.priority = 0;
        obj_data.index = opts->prio_array[i].index;

        if ((i + 1) == opts->prio_array_len) {
          send_reply = true;
        }

          obj_data.dnet = opts->dnet;
          if (strlen(opts->dadr) > 0) {
            strncpy(obj_data.dadr, opts->dadr, sizeof(obj_data.dadr) - 1);
          }
          strncpy(obj_data.mac, opts->mac, sizeof(obj_data.mac) - 1);
          obj_data.prio_array_len = opts->prio_array_len;
          memcpy((char *)&obj_data.prio_array[0], (char *)&opts->prio_array[0],
            sizeof(json_key_value_pair) * opts->prio_array_len);

          if (opts->req_tokens_len > 0) {
            obj_data.req_tokens_len = opts->req_tokens_len;
            memcpy((char *)&obj_data.req_tokens[0], (char *)&opts->req_tokens[0],
              sizeof(request_token_cb) * opts->req_tokens_len);
          }

        send_write_request(&obj_data, &value, opts, send_reply);
      }
    } else {
    memset(&obj_data, 0, sizeof(obj_data));
    obj_data.device_instance = opts->device_instance;
    obj_data.object_type = opts->object_type;
    obj_data.object_instance = opts->object_instance;
    obj_data.object_property = opts->property;
    obj_data.priority = opts->priority;
    obj_data.index = opts->index;
    obj_data.dnet = opts->dnet;
    if (strlen(opts->dadr) > 0) {
      strncpy(obj_data.dadr, opts->dadr, sizeof(obj_data.dadr) - 1);
    }
    strncpy(obj_data.mac, opts->mac, sizeof(obj_data.mac) - 1);
    memcpy(&obj_data.value, opts->value, sizeof(opts->value));
    if (opts->req_tokens_len > 0) {
      obj_data.req_tokens_len = opts->req_tokens_len;
      memcpy((char *)&obj_data.req_tokens[0], (char *)&opts->req_tokens[0],
        sizeof(request_token_cb) * opts->req_tokens_len);
    }

    memset(&value, 0, sizeof(value));
    value.context_specific = false;
    set_app_data_value_from_string(opts->object_type, opts->property, opts->value, &value);

    obj_data.topic_id = MQTT_WRITE_VALUE_CMD_RESULT_TOPIC;

    request_invoke_id = Send_Write_Property_Request(
      opts->device_instance, opts->object_type,
      opts->object_instance, opts->property,
      &value,
      obj_data.priority,
      opts->index);
    printf("write request_invoke_id: %d\n", request_invoke_id);

    add_bacnet_client_request(request_invoke_id, &obj_data);

    if (opts->timeout > 0) {
      timeout = opts->timeout;
    }

    pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, timeout);
    if (pdu_len) {
      npdu_handler(&src, &Rx_Buf[0], pdu_len);
    }
  }
  // }

  return(0);
}


/*
 * Process Bacnet client commands.
 */
int process_bacnet_client_command(char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], int n_topic_tokens,
  bacnet_client_cmd_opts *opts)
{
  if (!strcasecmp(topic_tokens[2], "whois")) {
    process_bacnet_client_whois_command(opts);
  } else if (!strcasecmp(topic_tokens[2], "read_value")) {
    process_bacnet_client_read_value_command(opts);
  } else if (!strcasecmp(topic_tokens[2], "write_value")) {
    process_bacnet_client_write_value_command(opts);
  } else if (!strcasecmp(topic_tokens[2], "pics")) {
    process_bacnet_client_pics_command(opts);
  } else if (!strcasecmp(topic_tokens[2], "point_discovery")) {
    process_bacnet_client_point_discovery_command(opts);
  } else if (!strcasecmp(topic_tokens[2], "points_info")) {
    process_bacnet_client_points_info_command(opts);
  } else {
    if (mqtt_debug) {
      printf("Unknown Bacnet client command: [%s]\n", topic_tokens[2]);
    }

    goto EXIT;
  }

  EXIT:

  return(0);
}


/*
 * Extract json fields.
 */
int extract_json_fields_to_cmd_opts(json_object *json_root, bacnet_client_cmd_opts *cmd_opts)
{
  json_object *json_field;
  struct json_object_iterator it;
  struct json_object_iterator itEnd;
  struct json_object *obj;
  struct json_object *kv_obj;
  char *key;
  char value[MAX_CMD_STR_OPT_VALUE_LENGTH];
  char kvp_key[MAX_JSON_KEY_LENGTH];
  char kvp_value[MAX_CMD_STR_OPT_VALUE_LENGTH];
  int array_len;
  int i, ii;
  char *keys[] = {
    "objectType",
    "objectInstance",
    "property",
    "index",
    "priority",
    "deviceInstance",
    "dnet",
    "mac",
    "dadr",
    "tag",
    "value",
    "uuid",
    "tags",
    "device_instance_min",
    "device_instance_max",
    "timeout",
    "priorityArray",
    "device_per_publish",
    "objects",
    "page_number",
    "page_size",
    "points",
    NULL
  };

  for (i = 0; keys[i] ;i++) {
    key = keys[i];
    json_field = json_object_object_get(json_root, key);
    if (!json_field) {
      continue;
    }

    if (!strcmp(key, "objectType")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->object_type = (unsigned)strtol(value, NULL, 0);
    } else if (!strcmp(key, "objectInstance")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->object_instance = strtol(value, NULL, 0);
    } else if (!strcmp(key, "property")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->property = (unsigned)strtol(value, NULL, 0);
    } else if (!strcmp(key, "index")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->index = atoi(value);
    } else if (!strcmp(key, "priority")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->priority = atoi(value);
    } else if (!strcmp(key, "deviceInstance")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->device_instance = strtol(value, NULL, 0);
    } else if (!strcmp(key, "dnet")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->dnet = atoi(value);
    } else if (!strcmp(key, "mac")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      strncpy(cmd_opts->mac, value, sizeof(cmd_opts->mac) - 1);
    } else if (!strcmp(key, "dadr")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      strncpy(cmd_opts->dadr, value, sizeof(cmd_opts->dadr) - 1);
    } else if (!strcmp(key, "value")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      strncpy(cmd_opts->value, value, sizeof(cmd_opts->value) - 1);
    } else if (!strcmp(key, "uuid")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      strncpy(cmd_opts->uuid, value, sizeof(cmd_opts->uuid) - 1);
    } else if (!strcmp(key, "tags")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      if (!strcmp(value, "slow_test")) {
        cmd_opts->tag_flags |= CMD_TAG_FLAG_SLOW_TEST;
      }
    } else if (!strcmp(key, "device_instance_min")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->device_instance_min = atoi(value);
    } else if (!strcmp(key, "device_instance_max")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->device_instance_max = atoi(value);
    } else if (!strcmp(key, "timeout")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->timeout = atoi(value);
    } else if (!strcmp(key, "device_per_publish")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->publish_per_device = (!strcasecmp(value, "true")) ? 1 : 0;
    } else if (!strcmp(key, "priorityArray")) {
      it = json_object_iter_begin(json_field);
      itEnd = json_object_iter_end(json_field);
      while (!json_object_iter_equal(&it, &itEnd)) {
        obj = json_object_iter_peek_value(&it);
        strncpy(cmd_opts->prio_array[cmd_opts->prio_array_len].key,
          json_object_iter_peek_name(&it), MAX_JSON_KEY_LENGTH - 1);
        strncpy(cmd_opts->prio_array[cmd_opts->prio_array_len].value,
          json_object_get_string(obj), MAX_CMD_STR_OPT_VALUE_LENGTH - 1);
        cmd_opts->prio_array_len++;
        json_object_iter_next(&it);
      }
    } else if (!strcmp(key, "objects")) {
      rpm_object_cb *rpm_cb;
      array_len = json_object_array_length(json_field);
      cmd_opts->rpm_objects = malloc((sizeof(rpm_object_cb)) * array_len);
      cmd_opts->rpm_objects_len = array_len;
      for (ii = 0; ii < array_len; ii++) {
        rpm_cb = &cmd_opts->rpm_objects[ii];
        memset(rpm_cb, 0, sizeof(rpm_object_cb));
        rpm_cb->index = -1; 
        obj = json_object_array_get_idx(json_field, ii);
        it = json_object_iter_begin(obj);
        itEnd = json_object_iter_end(obj);
        while (!json_object_iter_equal(&it, &itEnd)) {
          kv_obj = json_object_iter_peek_value(&it);
          strncpy(kvp_key, json_object_iter_peek_name(&it), MAX_JSON_KEY_LENGTH - 1);
          strncpy(kvp_value, json_object_get_string(kv_obj), MAX_CMD_STR_OPT_VALUE_LENGTH - 1);

          if (!strcmp(kvp_key, "objectType")) {
            rpm_cb->object_type = (unsigned)strtol(kvp_value, NULL, 0);
          } else if (!strcmp(kvp_key, "objectInstance")) {
            rpm_cb->object_instance = strtol(kvp_value, NULL, 0);
          }  else if (!strcmp(kvp_key, "property")) {
            rpm_cb->property = (unsigned)strtol(kvp_value, NULL, 0);
          } else if (!strcmp(kvp_key, "index")) {
            rpm_cb->index = atoi(kvp_value);
          } else if (!strcmp(kvp_key, "priority")) {
            rpm_cb->priority = atoi(kvp_value);
          } else if (!strcmp(kvp_key, "value")) {
            strncpy(rpm_cb->value, kvp_value, sizeof(rpm_cb->value) - 1);
          } else {
            printf("- Unsupported KV pair: [%s] => [%s]\n", kvp_key, kvp_value);
          }

          json_object_iter_next(&it);
        }
      }
    } else if (!strcmp(key, "page_number")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->page_number = atoi(value);
    } else if (!strcmp(key, "page_size")) {
      strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
      cmd_opts->page_size = atoi(value);
    } else if (!strcmp(key, "points")) {
      point_entry_cb *point_cb;
      array_len = json_object_array_length(json_field); 
      cmd_opts->points = malloc((sizeof(point_entry_cb)) * array_len);
      cmd_opts->points_len = array_len;
      printf("- points_len: %d\n", cmd_opts->points_len);
      for (ii = 0; ii < array_len; ii++) {
        point_cb = &cmd_opts->points[ii];
        memset(point_cb, 0, sizeof(point_entry_cb));
        obj = json_object_array_get_idx(json_field, ii);
        strncpy(&point_cb->obj_name[0], json_object_get_string(obj), sizeof(point_cb->obj_name) - 1);
        printf("- [%d] => [%s]\n", ii, point_cb->obj_name);
      }
    }
  }

  for (i = 0; i < req_tokens_len; i++) {
    key = req_tokens[i];
    printf("- checking key: [%s]\n", key);
    json_field = json_object_object_get(json_root, key);
    if (!json_field) {
      continue;
    }

    strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
    if (is_reqquest_token_present(key)) {
      strncpy(cmd_opts->req_tokens[cmd_opts->req_tokens_len].key, key, (MAX_JSON_KEY_LENGTH - 1));
      strncpy(cmd_opts->req_tokens[cmd_opts->req_tokens_len].value, value, (MAX_JSON_VALUE_LENGTH - 1));
      cmd_opts->req_tokens_len++;

      if (cmd_opts->req_tokens_len == MAX_JSON_KEY_VALUE_PAIR) {
        break;
      }
    }
  }

  return(0);
}


/*
 * MQTT message arrived callback.
 */
int mqtt_msg_arrived(void *context, char *topic, int topic_len, MQTTAsync_message *message)
{
  mqtt_msg_cb *mqtt_msg = NULL;
  bacnet_client_cmd_opts *cmd_opts = NULL;
  json_object *json_root = NULL;
  char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH];
  int n_topic_tokens;
  int rc = 1;

  mqtt_msg = malloc(sizeof(mqtt_msg_cb));
  if (!mqtt_msg) {
    printf("Failed to allocation memory for mqtt_msg_cb!\n");
    goto EXIT;
  }

  cmd_opts = &mqtt_msg->cmd_opts;
  memcpy(cmd_opts, &init_bacnet_client_cmd_opts, sizeof(*cmd_opts));

  memset(&cmd_opts->prio_array[0], 0, sizeof(cmd_opts->prio_array));
  cmd_opts->rpm_objects_len = 0;
  cmd_opts->rpm_objects = NULL;
  memset(mqtt_msg->topic_value, 0, sizeof(mqtt_msg->topic_value));
  strncpy(mqtt_msg->topic_value, (char*)message->payload,
    (message->payloadlen > sizeof(mqtt_msg->topic_value)) ? sizeof(mqtt_msg->topic_value) -1 : message->payloadlen);

  if (mqtt_debug) {
     printf("MQTT message arrived: => %d\n", getpid());
     printf("- version: [%d]\n", message->struct_version);
     printf("- payloadLen: [%d]\n", message->payloadlen);
     printf("- topic  : [%s]\n", topic);
     printf("- value  : [%s]\n", mqtt_msg->topic_value);
  }

  memset(topic_tokens, 0, sizeof(topic_tokens));
  n_topic_tokens = tokenize_topic(topic, topic_tokens);
  if (n_topic_tokens == 0) {
    goto EXIT;
  }

  if (mqtt_debug) {
    dump_topic_tokens(n_topic_tokens, topic_tokens);
  }

  json_root = json_tokener_parse(mqtt_msg->topic_value);
  if (json_root == NULL) {
    if (mqtt_debug) {
      printf("Failed to parse topic value. Must be a JSON formatted string!\n");
    }

    goto EXIT;
  }

  if (mqtt_debug) {
    printf("The json string:\n\n%s\n\n", json_object_to_json_string(json_root));
  }

  if (extract_json_fields_to_cmd_opts(json_root,
    cmd_opts)) {
    if (mqtt_debug) {
      printf("Error extracting command options!\n");
    }

    goto EXIT;
  }

  memcpy(mqtt_msg->topic_tokens, topic_tokens, sizeof(mqtt_msg->topic_tokens));
  mqtt_msg->topic_tokens_length = n_topic_tokens;

  rc = mqtt_msg_push(mqtt_msg);

  EXIT:

  if (rc) {
    if (mqtt_msg) {
      if (cmd_opts) {
        if (cmd_opts->rpm_objects) {
          free(cmd_opts->rpm_objects);
        }

        if (cmd_opts->points) {
          free(cmd_opts->points);
        }
      }

      free(mqtt_msg);
    }
  }
      
  if (json_root) {
    json_object_put(json_root);
  } 

  MQTTAsync_freeMessage(&message);
  MQTTAsync_free(topic);

  return(1);
}

 
/*
 * On connect handler.
 */
void mqtt_on_connect(void* context, MQTTAsync_successData* response)
{
  int rc;

  printf("MQTT client connected!\n");

  mqtt_client_connected = true;

  if (!yaml_config_mqtt_disable_persistence() && !is_restore_persistent_done()) {
    restore_persistent_values(context);
  }

  if (yaml_config_mqtt_write_via_subscribe()) {
    printf("MQTT write via subscribe enabled\n");
    rc = mqtt_subscribe_to_topics(context);
    if (rc) {
      printf("- Failed to subscribe to one of the topics\n");
    }
  }

  if (yaml_config_bacnet_client_enable()) {
    printf("Bacnet client enabled\n");
    rc = mqtt_subscribe_to_bacnet_client_topics(context);
    if (rc) {
      printf("- Failed to subscribe to one of the Bacnet client topics\n");
    }

    init_bacnet_client_service_handlers();
  }
}


/*
 * On connect failure handler.
 */
void mqtt_on_connect_failure(void* context, MQTTAsync_failureData* response)
{
  printf("Connect failed, rc %d\n", response->code);
  mqtt_client_connected = false;
}


/*
 * Connect to MQTT broker.
 */
int mqtt_connect_to_broker(void)
{
  MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer;
  int rc;

  conn_opts.keepAliveInterval = 600;
  conn_opts.cleansession = 1;
  conn_opts.context = mqtt_client;
  conn_opts.onSuccess = mqtt_on_connect;
  conn_opts.onFailure = mqtt_on_connect_failure;
  rc = MQTTAsync_connect(mqtt_client, &conn_opts);
  if (rc != MQTTASYNC_SUCCESS)
  {
    printf("MQTT failed to connect to server: %s\n", MQTTAsync_strerror(rc));
    return(1);
  }

  return(0);
}


/*
 * Check and reconnect to MQTT broker.
 */
void mqtt_check_reconnect(void)
{
  int rc;

  rc = MQTTAsync_isConnected(mqtt_client);
  if (!rc) {
    printf("WARNING: MQTT client not connected!\n");
  }

  if (!mqtt_client_connected) {
    if (mqtt_debug) {
      printf("MQTT connection was lost. Reconnecting to MQTT broker...\n");
    }

    if (mqtt_connect_to_broker()) {
      if (mqtt_debug) {
        printf("Failed to connect to MQTT broker. Please check network connection.\n");
      }

      return;
    }

    mqtt_client_connected = true;

    if (yaml_config_mqtt_write_via_subscribe()) {
      printf("MQTT write via subscribe enabled\n");
      rc = mqtt_subscribe_to_topics(mqtt_client);
      if (rc) {
        printf("- Failed to subscribe to one of the topics\n");
        return;
      }
    }
  }
}


/*
 * Intialize bacnet client request list.
 */
void init_bacnet_client_request_list(void)
{
  bc_request_list_head = bc_request_list_tail = NULL;
}


/*
 * Shutdown bacnet client request list.
 */
void shutdown_bacnet_client_request_list(void)
{
  llist_cb *tmp, *ptr = bc_request_list_head;

  while (ptr != NULL) {
    tmp = ptr;
    ptr = ptr->next;
    free(tmp);
  }
}


/*
 * Check if bacnet client request is present.
 */
int is_bacnet_client_request_present(uint8_t invoke_id)
{
  llist_cb *ptr = bc_request_list_head;
  int present = false;

  while (ptr != NULL) {
    if (ptr->data.invoke_id == invoke_id) {
      present = true;
      break;
    }

    ptr = ptr->next;
  }

  return(present);
}


/*
 * Get bacnet client request object data.
 */
llist_obj_data *get_bacnet_client_request_obj_data(uint8_t invoke_id, llist_obj_data *buf)
{
  llist_cb *ptr = bc_request_list_head;
  int len;

  while (ptr != NULL) {
    if (ptr->data.invoke_id == invoke_id) {
      memcpy(buf, &ptr->data.obj_data, sizeof(llist_obj_data));
      if (ptr->data.obj_data.rpm_objects) {
        buf->rpm_objects_len = ptr->data.obj_data.rpm_objects_len;
        len = ptr->data.obj_data.rpm_objects_len * sizeof(rpm_object_cb);
        buf->rpm_objects = malloc(len);
        memcpy(buf->rpm_objects, ptr->data.obj_data.rpm_objects, len);
      }
      return(&ptr->data.obj_data);
    }
    
    ptr = ptr->next;
  }

  return(NULL);
}


/*
 * Add bacnet client request.
 */
int add_bacnet_client_request(uint8_t invoke_id, llist_obj_data *obj_data)
{
  llist_cb *ptr;

  ptr = malloc(sizeof(llist_cb));
  if (!ptr) {
    printf("Error allocating memory for llist_cb!\n");
    return(1);
  }

  ptr->data.invoke_id = invoke_id;
  if (obj_data) {
    memcpy(&ptr->data.obj_data, obj_data, sizeof(llist_obj_data));
    if (obj_data->rpm_objects) {
      ptr->data.obj_data.rpm_objects_len = obj_data->rpm_objects_len;
      /* free this in handler */
      ptr->data.obj_data.rpm_objects = obj_data->rpm_objects;
    }
  }
  ptr->timestamp = time(NULL);
  ptr->next = NULL;

  if (!bc_request_list_head) {
    bc_request_list_head = bc_request_list_tail = ptr;
  } else {
    bc_request_list_tail->next = ptr;
    bc_request_list_tail = ptr;
  }

  return(0);
}


/*
 * Delete backet client request.
 */
int del_bacnet_client_request(uint8_t invoke_id)
{
  llist_cb *prev = NULL;
  llist_cb *ptr = bc_request_list_head;

  while (ptr != NULL) {
    if (ptr->data.invoke_id == invoke_id) {
      break;
    }

    prev = ptr;
    ptr = ptr->next;
  }

  if (ptr) {
    printf("- request with ID: %d found for deleting\n", ptr->data.invoke_id);
    if (ptr == bc_request_list_head) {
      bc_request_list_head = ptr->next;
    } else {
      prev->next = ptr->next;
      if (ptr == bc_request_list_tail) {
        bc_request_list_tail = prev;
      }
    }

    if (ptr->data.obj_data.rpm_objects != NULL) {
      free(ptr->data.obj_data.rpm_objects);
    }
    free(ptr);
  }

  return(0);
}


/*
 * Is request list locked.
 */
int is_request_list_locked(void)
{
  return(request_list_locked);
}


/*
 * Lock request list.
 */
void lock_request_list(void)
{
  request_list_locked = true;
}


/*
 * Unlock request list.
 */
void unlock_request_list(void)
{
  request_list_locked = false;
}


/*
 * Init command opts.
 */
void init_cmd_opts_from_list_cb(bacnet_client_cmd_opts *opts, llist_obj_data *obj)
{
  memset(opts, 0, sizeof(bacnet_client_cmd_opts));
  opts->object_type = obj->object_type;
  opts->object_instance = obj->object_instance;
  opts->property = obj->object_property;
  opts->index = obj->index;
  opts->priority = obj->priority;
  opts->device_instance = obj->device_instance;
  opts->dnet = obj->dnet;
  memcpy(&opts->mac[0], &obj->mac[0], sizeof(opts->mac));
  memcpy(&opts->dadr[0], &obj->dadr[0], sizeof(opts->dadr));
  memcpy(&opts->value[0], &obj->value[0], sizeof(opts->value));
  opts->req_tokens_len = obj->req_tokens_len;
  opts->prio_array_len = obj->prio_array_len;
  memcpy(&opts->req_tokens[0], &obj->req_tokens[0], sizeof(opts->req_tokens));
  memcpy(&opts->prio_array[0], &obj->prio_array[0], sizeof(opts->prio_array));
}


/*
 * Sweep aged bacnet client requests.
 */
void sweep_bacnet_client_aged_requests(void)
{
  llist_cb *ptr, *tmp, *prev = NULL;
  time_t cur_tt = time(NULL);
  char err_msg[1024];
  bacnet_client_cmd_opts opts;

  if (is_request_list_locked()) {
    return;
  } else {
    lock_request_list();
  }

  ptr = bc_request_list_head;

  while (ptr != NULL) {
    if ((cur_tt - ptr->timestamp) >= BACNET_CLIENT_REQUEST_TTL) {
      printf("- Aged request found: %d\n", ptr->data.invoke_id);

      sprintf(err_msg, "No response from target device");
      init_cmd_opts_from_list_cb(&opts, &ptr->data.obj_data);
      mqtt_publish_command_error(err_msg, &opts, ptr->data.obj_data.topic_id);

      tmp = ptr;
      if (ptr == bc_request_list_head) {
        bc_request_list_head = ptr->next;
        ptr = bc_request_list_head;
      } else {
        prev->next = ptr->next;
        if (ptr == bc_request_list_tail) {
          bc_request_list_tail = prev;
        }

        ptr = ptr->next;
      }

      if (tmp->data.obj_data.rpm_objects) {
        free(tmp->data.obj_data.rpm_objects);
      }

      free(tmp);
      continue;
    }

    prev = ptr;
    ptr = ptr->next;
  }

  unlock_request_list();
}


/*
 * Intialize bacnet client whois list.
 */
void init_bacnet_client_whois_list(void)
{
  bc_whois_head = bc_whois_tail = NULL;
}


/*
 * Shutdown bacnet client whois list.
 */
void shutdown_bacnet_client_whois_list(void)
{
  llist_whois_cb *tmp, *ptr = bc_whois_head;

  while (ptr != NULL) {
    tmp = ptr;
    ptr = ptr->next;
    free(tmp);
  }
}


/*
 * Check if bacnet client whois list is not empty.
 */
int is_bacnet_client_whois_present(void)
{
  return(bc_whois_head != NULL);
}


/*
 * Get bacnet client first whois data.
 */
address_table_cb *get_bacnet_client_first_whois_data(void)
{
  llist_whois_cb *ptr = bc_whois_head;

  if (ptr != NULL) {
    return(&ptr->data.address_table);
  }

  return(NULL);
}


/*
 * Add bacnet client whois.
 */
int add_bacnet_client_whois(BACNET_ADDRESS *dest, bacnet_client_cmd_opts *opts)
{
  llist_whois_cb *ptr;

  ptr = malloc(sizeof(llist_whois_cb));
  if (!ptr) {
    printf("Error allocating memory for llist_whois_cb!\n");
    return(1);
  }

  memset(ptr, 0, sizeof(llist_whois_cb));
  ptr->request_id = bc_whois_request_ctr++;
  ptr->timestamp = time(NULL);
  ptr->data.timeout = opts->timeout;
  memcpy(&ptr->data.dest, dest, sizeof(BACNET_ADDRESS));
  ptr->data.device_instance_min = opts->device_instance_min;
  ptr->data.device_instance_max = opts->device_instance_max;
  ptr->data.dnet = opts->dnet;
  if (strlen(opts->mac)) {
    strcpy(ptr->data.mac, opts->mac);
  }

  if (strlen(opts->dadr)) {
    strcpy(ptr->data.dadr, opts->dadr);
  }

  if (opts->req_tokens_len > 0) {
    ptr->data.req_tokens_len = opts->req_tokens_len;
    memcpy((char *)&ptr->data.req_tokens[0], (char *)&opts->req_tokens[0],
      sizeof(request_token_cb) * opts->req_tokens_len);
  }

  ptr->data.publish_per_device = opts->publish_per_device;

  ptr->next = NULL;

  if (!bc_whois_head) {
    bc_whois_head = bc_whois_tail = ptr;
  } else {
    bc_whois_tail->next = ptr;
    bc_whois_tail = ptr;
  }

  printf("- Added whois request: %d\n", ptr->request_id);

  return(0);
}


/*
 * Delete backet client whois.
 */
int del_bacnet_client_first_whois_request(void)
{
  llist_whois_cb *ptr = bc_whois_head;

  if (ptr) {
    printf("- whois request found!\n");
    bc_whois_head = ptr->next;
    free(ptr);
  }

  return(0);
}


/*
 * Add whois address.
 */
void whois_address_table_add(address_table_cb *table, uint32_t device_id,
  unsigned max_apdu, BACNET_ADDRESS *src)
{
  address_entry_cb *pMatch;
  uint8_t flags = 0;

  pMatch = table->first;
  while (pMatch) {
    if (pMatch->device_id == device_id) {
      if (bacnet_address_same(&pMatch->address, src)) {
        return;
      }

      flags |= BAC_ADDRESS_MULT;
      pMatch->Flags |= BAC_ADDRESS_MULT;
    }

    pMatch = pMatch->next;
  }
    
  // pMatch = alloc_address_entry();
  pMatch = (address_entry_cb *)malloc(sizeof(address_entry_cb));
  memset(pMatch, 0, sizeof(address_entry_cb));
  if (table->first) {
    table->last->next = pMatch;
    table->last = pMatch;
  } else {
    table->first = table->last = pMatch;
  }
    
  pMatch->Flags = flags;
  pMatch->device_id = device_id;
  pMatch->max_apdu = max_apdu;
  pMatch->address = *src;
}


/*
 * Sweep aged bacnet client whois requests.
 */
void sweep_bacnet_client_whois_requests(void)
{
  llist_whois_cb *tmp, *prev = NULL;
  llist_whois_cb *ptr = bc_whois_head;
  address_entry_cb *ae_ptr, *ae_tmp;
  time_t cur_tt = time(NULL);

  while (ptr != NULL) {
    if ((ptr->timestamp + ptr->data.timeout) < cur_tt) {
      printf("- Whois Aged request found: %d\n", ptr->request_id);

      tmp = ptr;
      if (ptr == bc_whois_head) {
        bc_whois_head = ptr->next;
        ptr = bc_whois_head;
      } else {
        prev->next = ptr->next;
        if (ptr == bc_whois_tail) {
          bc_whois_tail = prev;
        }

        ptr = ptr->next;
      }

      if (tmp->data.publish_per_device) {
        publish_bacnet_client_whois_result_per_device(tmp);
      } else {
        publish_bacnet_client_whois_result(tmp);
      }

      ae_ptr = tmp->data.address_table.first;
      while (ae_ptr) {
        ae_tmp = ae_ptr;
        ae_ptr = ae_ptr->next;
        free(ae_tmp);
      }

      free(tmp);
      continue;
    }

    prev = ptr;
    ptr = ptr->next;
  }
}


/*
 * Sweep aged bacnet client point discovery requests.
 */
void sweep_bacnet_client_point_disc_requests(void)
{
  llist_point_disc_cb *tmp, *prev = NULL;
  llist_point_disc_cb *ptr = bc_point_disc_head;
  point_entry_cb *pe_ptr, *pe_tmp;
  time_t cur_tt = time(NULL);

  while (ptr != NULL) {
    if ((ptr->timestamp + ptr->data.timeout) < cur_tt) {
      printf("- Point discovery Aged request found: %d\n", ptr->request_id);

      tmp = ptr;
      if (ptr == bc_point_disc_head) {
        bc_point_disc_head = ptr->next;
        ptr = bc_point_disc_head;
      } else {
        prev->next = ptr->next;
        if (ptr == bc_point_disc_tail) {
          bc_point_disc_tail = prev;
        }

        ptr = ptr->next;
      }

      publish_bacnet_client_point_disc_result(tmp);

      pe_ptr = tmp->data.point_list.head;
      while (pe_ptr) {
        pe_tmp = pe_ptr;
        pe_ptr = pe_ptr->next;
        free(pe_tmp);
      }

      free(tmp);
      continue;
    } else {
      /* perform state machine */
      switch (ptr->state) {
        case POINT_DISC_STATE_GET_POINTS:
          get_bacnet_client_point_disc_points(ptr);
          break;
      }
    }

    prev = ptr;
    ptr = ptr->next;
  }
}


/*
 * Sweep aged bacnet client points info requests.
 */
void sweep_bacnet_client_points_info_requests(void)
{
  llist_points_info_cb *tmp, *prev = NULL;
  llist_points_info_cb *ptr = bc_points_info_head;
  time_t cur_tt = time(NULL);
      
  while (ptr != NULL) {
    if ((ptr->timestamp + ptr->data.timeout) < cur_tt) {
      printf("- Points info Aged request found: %d\n", ptr->request_object_id);
        
      tmp = ptr;
      if (ptr == bc_points_info_head) {
        bc_points_info_head = ptr->next;
        ptr = bc_points_info_head;
      } else {
        prev->next = ptr->next;
        if (ptr == bc_points_info_tail) {
          bc_points_info_tail = prev;
        } 
          
        ptr = ptr->next;
      }

      publish_bacnet_client_points_info_result(tmp);
    
      if (tmp->data.points) {
        free(tmp->data.points);
      }

      free(tmp);
      continue;
    }

    prev = ptr;
    ptr = ptr->next;
  }
}


/*
 * Intialize bacnet client pics list.
 */
void init_bacnet_client_pics_list(void)
{
  bc_pics_head = bc_pics_tail = NULL;
}


/*
 * Shutdown bacnet client pics list.
 */
void shutdown_bacnet_client_pics_list(void)
{
  llist_pics_cb *tmp, *ptr = bc_pics_head;

  while (ptr != NULL) {
    tmp = ptr;
    ptr = ptr->next;
    free(tmp);
  }
}


/*
 * Check if bacnet client pics request is present.
 */
int is_bacnet_client_pics_present(uint8_t invoke_id)
{
  llist_pics_cb *ptr = bc_pics_head;
  int present = false;

  while (ptr != NULL) {
    if (ptr->data.invoke_id == invoke_id) {
      present = true;
      break;
    }

    ptr = ptr->next;
  }

  return(present);
}


/*
 * Add bacnet client pics request.
 */
int add_bacnet_client_pics(uint8_t invoke_id)
{
  llist_pics_cb *ptr;

  ptr = malloc(sizeof(llist_pics_cb));
  if (!ptr) {
    printf("Error allocating memory for llist_pics_cb!\n");
    return(1);
  }

  ptr->data.invoke_id = invoke_id;
  ptr->timestamp = time(NULL);
  ptr->next = NULL;

  if (!bc_pics_head) {
    bc_pics_head = bc_pics_tail = ptr;
  } else {
    bc_pics_tail->next = ptr;
    bc_pics_tail = ptr;
  }

  return(0);
}


/*
 * Delete backet client pics request.
 */
int del_bacnet_client_pics(uint8_t invoke_id)
{
  llist_pics_cb *prev = NULL;
  llist_pics_cb *ptr = bc_pics_head;

  while (ptr != NULL) {
    if (ptr->data.invoke_id == invoke_id) {
      break;
    }

    prev = ptr;
    ptr = ptr->next;
  }

  if (ptr) {
    printf("- PICS request with ID: %d found for deleting\n", ptr->data.invoke_id);
    if (ptr == bc_pics_head) {
      bc_pics_head = ptr->next;
    } else {
      prev->next = ptr->next;
      if (ptr == bc_pics_tail) {
        bc_pics_tail = prev;
      }
    }

    free(ptr);
  }

  return(0);
}


/*
 * Intialize bacnet client point discovery list.
 */
void init_bacnet_client_point_disc_list(void)
{
  bc_point_disc_head = bc_point_disc_tail = NULL;
}


/*
 * Shutdown bacnet client point discovery list.
 */
void shutdown_bacnet_client_point_disc_list(void)
{
  llist_point_disc_cb *tmp, *ptr = bc_point_disc_head;

  while (ptr != NULL) {
    tmp = ptr;
    ptr = ptr->next;
    free(tmp);
  }
}


/*
 * Get bacnet client point discovery request.
 */
llist_point_disc_cb *get_bacnet_client_point_disc_req(uint32_t request_id, BACNET_ADDRESS *src)
{
  llist_point_disc_cb *ptr = bc_point_disc_head;

  while (ptr != NULL) {
    if (ptr->request_id == request_id && !memcmp(src, &ptr->data.dest, sizeof(BACNET_ADDRESS))) {
      ptr->data_received = true;
      ptr->timestamp = time(NULL);
      return(ptr);
    }

    ptr = ptr->next;
  }

  return(NULL);
}


/*
 * Intialize bacnet client points info list.
 */
void init_bacnet_client_points_info_list(void)
{
  bc_points_info_head = bc_points_info_tail = NULL;
}


/*
 * Shutdown bacnet client points info list.
 */
void shutdown_bacnet_client_points_info_list(void)
{
  llist_points_info_cb *tmp, *ptr = bc_points_info_head;

  while (ptr != NULL) {
    tmp = ptr;
    ptr = ptr->next;
    free(tmp);
  }
}


/*
 * Get bacnet client point discovery request.
 */
llist_points_info_cb *get_bacnet_client_points_info_req(uint32_t request_id, BACNET_ADDRESS *src)
{
  llist_points_info_cb *ptr = bc_points_info_head;

  while (ptr != NULL) {
    if (!memcmp(src, &ptr->data.dest, sizeof(BACNET_ADDRESS))) {
      for (int i = 0; i < ptr->data.points_len; i++) {
        if (ptr->data.points[i].request_id == request_id) {
          ptr->timestamp = time(NULL);
          return(ptr);
        }
      }
    }

    ptr = ptr->next;
  }

  return(NULL);
}


/*
 * Initialize mqtt client module.
 */
int mqtt_client_init(void)
{
  pthread_mutexattr_t mutex_attr;
  MQTTAsync_createOptions createOpts = MQTTAsync_createOptions_initializer;
  char mqtt_broker_endpoint[100];
  char buf[100];
  char *pEnv;
  int rc;

  enable_pics_filter_objects = yaml_config_bacnet_client_filter_objects();

#if defined(YAML_CONFIG)
  mqtt_debug = yaml_config_mqtt_debug();
  if (!mqtt_debug) {
#endif
  pEnv = getenv("MQTT_DEBUG");
  if (pEnv) {
    mqtt_debug = true;
  }
#if defined(YAML_CONFIG)
  }
#endif

#if defined(YAML_CONFIG)
  pEnv = (char *)yaml_config_mqtt_broker_ip();
  if (pEnv) {
    strncpy(mqtt_broker_ip, pEnv, sizeof(mqtt_broker_ip) -1);
  } else {
#endif
  pEnv = getenv("MQTT_BROKER_IP");
  if (pEnv) {
    strncpy(mqtt_broker_ip, pEnv, sizeof(mqtt_broker_ip) -1);
  } else {
    strcpy(mqtt_broker_ip, DEFAULT_MQTT_BROKER_IP);
  }
#if defined(YAML_CONFIG)
  }     
#endif

#if defined(YAML_CONFIG)
  mqtt_broker_port = yaml_config_mqtt_broker_port();
  if (!mqtt_broker_port) {
#endif
  pEnv = getenv("MQTT_BROKER_PORT");
  if (pEnv) {
    mqtt_broker_port = atoi(pEnv);
  }
#if defined(YAML_CONFIG)
  }
#endif

  sprintf(mqtt_broker_endpoint, "tcp://%s:%d", mqtt_broker_ip, mqtt_broker_port);

  pEnv = getenv("MQTT_CLIENT_ID");
  if (pEnv) {
    strncpy(mqtt_client_id, pEnv, sizeof(mqtt_client_id) -1);
  } else {
    gethostname(buf, sizeof(buf) -1);
    sprintf(mqtt_client_id, "%s-mqtt-cid", buf);
  }

  if (mqtt_debug) {
    printf("MQTT broker endpoint: %s\n", mqtt_broker_endpoint);
    printf("MQTT client ID: %s\n", mqtt_client_id);
  }

  rc = MQTTAsync_createWithOptions(&mqtt_client, mqtt_broker_endpoint, mqtt_client_id, MQTTCLIENT_PERSISTENCE_NONE, NULL, &createOpts);
  if (rc != MQTTASYNC_SUCCESS) {
    printf("MQTT error creating client instance: %s\n", mqtt_broker_endpoint);
    return(1);
  }

  rc = MQTTAsync_setCallbacks(mqtt_client, mqtt_client, mqtt_connection_lost, mqtt_msg_arrived, NULL);
  if (rc != MQTTASYNC_SUCCESS) {
    printf("MQTT error setting up callbacks\n");
    return(1);
  }

  init_bacnet_client_request_list();
  init_bacnet_client_whois_list();
  init_request_tokens();
  init_bacnet_client_pics_list();
  init_bacnet_client_point_disc_list();
  init_bacnet_client_points_info_list();

  pthread_mutexattr_init(&mutex_attr);
  rc = pthread_mutex_init(&mqtt_mutex, &mutex_attr);
  if (rc != 0) {
    printf("Error creating MQTT mutex!\n");
    return(1);
  }

  pthread_mutexattr_init(&mutex_attr);
  rc = pthread_mutex_init(&mqtt_msg_queue_mutex, &mutex_attr);
  if (rc != 0) {
    printf("Error creating MQTT Message Queue mutex!\n");
    return(1);
  }

  mqtt_msg_queue_init();

  if (mqtt_connect_to_broker()) {
    return(1);
  }

  return(0);
}


/*
 * On subscribe success handler.
 */
void mqtt_on_subscribe(void* context, MQTTAsync_successData* response)
{
  printf("Subscribe succeeded\n");
}


/*
 * On subscribe failure handler.
 */
void mqtt_on_subscribe_failure(void* context, MQTTAsync_failureData* response)
{
  printf("Subscribe failed, rc %d\n", response->code);
}


/*
 * Subscribe to write property name topics.
 */
int subscribe_write_prop_name(void* context)
{
  MQTTAsync client = (MQTTAsync)context;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  int rc;
  const char *topics[] = {
    "bacnet/+/+/write/name",
    NULL
  };

  if (mqtt_debug) {
     printf("MQTT subscribing to write property name topic\n");
  }

  for (int i = 0; topics[i] != NULL; i++) {
    if (mqtt_debug) {
      printf("- topic[%d] = [%s]\n", i, topics[i]);
    }

    opts.onSuccess = mqtt_on_subscribe;
    opts.onFailure = mqtt_on_subscribe_failure;
    opts.context = client;
    rc = MQTTAsync_subscribe(mqtt_client, topics[i], 0, &opts);
    if (rc != MQTTASYNC_SUCCESS) {
      if (mqtt_debug) {
        printf("- WARNING: Failed to subscribe: %s\n", MQTTAsync_strerror(rc));
      }

      // return(1);
    }
  }

  return(0);
}


/*
 * Subscribe to write property present value topics.
 */
int subscribe_write_prop_present_value(void* context)
{
  MQTTAsync client = (MQTTAsync)context;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  int rc;
  const char *topics[] = {
    "bacnet/+/+/write/pv",
    NULL
  };

  if (mqtt_debug) {
     printf("MQTT subscribing to write present value topic\n");
  }

  for (int i = 0; topics[i] != NULL; i++) {
    if (mqtt_debug) {
      printf("- topic[%d] = [%s]\n", i, topics[i]);
    }

    opts.onSuccess = mqtt_on_subscribe;
    opts.onFailure = mqtt_on_subscribe_failure;
    opts.context = client;
    rc = MQTTAsync_subscribe(mqtt_client, topics[i], 0, &opts);
    if (rc != MQTTASYNC_SUCCESS) {
      if (mqtt_debug) {
        printf("- WARNING: Failed to subscribe: %s\n", MQTTAsync_strerror(rc));
      }
      
      // return(1);
    }
  }

  return(0);
}


/*
 * Subscribe to write property name topics.
 */
int subscribe_write_prop_priority_array(void* context)
{
  MQTTAsync client = (MQTTAsync)context;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  int rc;
  const char *topics[] = {
    "bacnet/+/+/write/pri/+",
    "bacnet/+/+/write/pri/+/all",
    NULL
  };

  if (mqtt_debug) {
     printf("MQTT subscribing to write priority array topic\n");
  }

  for (int i = 0; topics[i] != NULL; i++) {
    if (mqtt_debug) {
      printf("- topic[%d] = [%s]\n", i, topics[i]);
    }

    opts.onSuccess = mqtt_on_subscribe;
    opts.onFailure = mqtt_on_subscribe_failure;
    opts.context = client;
    rc = MQTTAsync_subscribe(mqtt_client, topics[i], 0, &opts);
    if (rc != MQTTASYNC_SUCCESS) {
      if (mqtt_debug) {
        printf("- WARNING: Failed to subscribe: %s\n", MQTTAsync_strerror(rc));
      }

      // return(1);
    }
  }

  return(0);
}


/*
 * Subscribe to topics.
 */
int mqtt_subscribe_to_topics(void* context)
{
  int i, n_topics = 0;
  char **topics;

  topics = yaml_config_properties(&n_topics);
  if ((topics == NULL) || (n_topics == 0)) {
    return(0);
  }

  for (i = 0; i < n_topics; i++) {
    if (!strncmp(topics[i], "name", 4)) {
      if (subscribe_write_prop_name(context)) {
        // return(1);
      }
    } else if (!strncmp(topics[i], "pv", 2)) {
      if (subscribe_write_prop_present_value(context)) {
        // return(1);
      }
    } else if (!strncmp(topics[i], "pri", 3)) {
      if (subscribe_write_prop_priority_array(context)) {
        // return(1);
      }
    }
  }

  return(0);
}


void mqtt_on_disconnect(void* context, MQTTAsync_successData* response)
{
  printf("- MQTT client disconnected!\n");
}


void mqtt_on_disconnect_failure(void* context, MQTTAsync_failureData* response)
{
  printf("- MQTT client failed to disconnect!\n");
}


/*
 * Shutdown mqtt client module.
 */
void mqtt_client_shutdown(void)
{
  MQTTAsync_disconnectOptions disc_opts = MQTTAsync_disconnectOptions_initializer;

  disc_opts.onSuccess = mqtt_on_disconnect;
  disc_opts.onFailure = mqtt_on_disconnect_failure;

  MQTTAsync_disconnect(mqtt_client, &disc_opts);
  MQTTAsync_destroy(&mqtt_client);

  shutdown_bacnet_client_request_list();
  shutdown_bacnet_client_whois_list();
  shutdown_bacnet_client_pics_list();
  shutdown_bacnet_client_point_disc_list();
  shutdown_bacnet_client_points_info_list();

  mqtt_msg_queue_shutdown();
}


/*
 * Form publish topic.
 */
char *mqtt_form_publish_topic(char *device_id, char *object_name)
{
  return(NULL);
}


/*
 * Create mqtt topic from the topic mappings.
 */
char *mqtt_create_topic(int object_type, int object_instance, int property_id, char *buf, int buf_len, int topic_type)
{
  char *object_type_str;
  char *property_id_str;

  switch(object_type) {
    case OBJECT_ANALOG_INPUT:
      object_type_str = "ai";
      break;

    case OBJECT_ANALOG_OUTPUT:
      object_type_str = "ao";
      break;

    case OBJECT_ANALOG_VALUE:
      object_type_str = "av";
      break;

    case OBJECT_BINARY_INPUT:
      object_type_str = "bi";
      break;

    case OBJECT_BINARY_OUTPUT:
      object_type_str = "bo";
      break;

    case OBJECT_BINARY_VALUE:
      object_type_str = "bv";
      break;

    case OBJECT_DEVICE:
      object_type_str = "device";
      break;

    case OBJECT_PROGRAM:
      object_type_str = "program";
      break;

    default:
      object_type_str = "unknown";
      break;
  }

  switch(property_id) {
    case PROP_OBJECT_NAME:
      property_id_str = "name";
      break;

    case PROP_PRESENT_VALUE:
      property_id_str = "pv";
      break;

    case PROP_PRIORITY_ARRAY:
      property_id_str = "pri";
      break;

    case PROP_RELINQUISH_DEFAULT:
      property_id_str = "rel";
      break;

    case PROP_PROGRAM_STATE:
      property_id_str = "state";
      break;

    case PROP_OBJECT_IDENTIFIER:
      property_id_str = "id";
      break;

    case PROP_SYSTEM_STATUS:
      property_id_str = "system_status";
      break;

    case PROP_VENDOR_NAME:
      property_id_str = "vendor_name";
      break;

    case PROP_VENDOR_IDENTIFIER:
      property_id_str = "vendor_id";
      break;

    case PROP_OBJECT_LIST:
      property_id_str = "object_list";
      break;

    default:
      property_id_str = "unknown";
      break;
  }

  switch(topic_type) {
    case MQTT_READ_VALUE_CMD_RESULT_TOPIC:
      snprintf(buf, buf_len - 1, "bacnet/cmd_result/read_value/%s/%d/%s", object_type_str,
        object_instance, property_id_str);
      break;

    case MQTT_WRITE_VALUE_CMD_RESULT_TOPIC:
      snprintf(buf, buf_len - 1, "bacnet/cmd_result/write_value/%s/%d/%s", object_type_str,
        object_instance, property_id_str);
      break;

    default:
      snprintf(buf, buf_len - 1, "bacnet/%s/%d/%s", object_type_str,
        object_instance, property_id_str);
      break;
  }

  return(buf); 
}


/*
 * Create mqtt error topic from the topic mappings.
 */
char *mqtt_create_error_topic(int object_type, int object_instance, int property_id, char *buf, int buf_len, int topic_type)
{
  char object_type_buf[128];
  char property_id_buf[128];
  char *object_type_str;
  char *property_id_str;

  switch(object_type) {
    case OBJECT_ANALOG_INPUT:
      object_type_str = "ai";
      break;

    case OBJECT_ANALOG_OUTPUT:
      object_type_str = "ao";
      break;

    case OBJECT_ANALOG_VALUE:
      object_type_str = "av";
      break;

    case OBJECT_BINARY_INPUT:
      object_type_str = "bi";
      break;

    case OBJECT_BINARY_OUTPUT:
      object_type_str = "bo";
      break;

    case OBJECT_BINARY_VALUE:
      object_type_str = "bv";
      break;

    case OBJECT_DEVICE:
      object_type_str = "device";
      break;

    case OBJECT_PROGRAM:
      object_type_str = "program";
      break;

    default:
      snprintf(object_type_buf, sizeof(object_type_buf) - 1, "%d", object_type);
      object_type_str = &object_type_buf[0];
      break;
  }

  switch(property_id) {
    case PROP_OBJECT_NAME:
      property_id_str = "name";
      break;

    case PROP_PRESENT_VALUE:
      property_id_str = "pv";
      break;

    case PROP_PRIORITY_ARRAY:
      property_id_str = "pri";
      break;

    case PROP_RELINQUISH_DEFAULT:
      property_id_str = "rel";
      break;

    case PROP_PROGRAM_STATE:
      property_id_str = "state";
      break;

    case PROP_OBJECT_IDENTIFIER:
      property_id_str = "id";
      break;

    case PROP_SYSTEM_STATUS:
      property_id_str = "system_status";
      break;

    case PROP_VENDOR_NAME:
      property_id_str = "vendor_name";
      break;

    case PROP_VENDOR_IDENTIFIER:
      property_id_str = "vendor_id";
      break;

    case PROP_OBJECT_LIST:
      property_id_str = "object_list";
      break;

    default:
      snprintf(property_id_buf, sizeof(property_id_buf) - 1, "%d", property_id);
      property_id_str = &property_id_buf[0];
      break;
  }

  switch(topic_type) {
    case MQTT_READ_VALUE_CMD_RESULT_TOPIC:
      snprintf(buf, buf_len - 1, "bacnet/cmd_result/read_value/%s/%d/%s", object_type_str,
        object_instance, property_id_str);
      break;

    case MQTT_WRITE_VALUE_CMD_RESULT_TOPIC:
      snprintf(buf, buf_len - 1, "bacnet/cmd_result/write_value/%s/%d/%s", object_type_str,
        object_instance, property_id_str);
      break;

    default:
      snprintf(buf, buf_len - 1, "bacnet/%s/%d", object_type_str,
        object_instance);
      break;
  }

  return(buf);
}


/*
 * Publish topic.
 */
int mqtt_publish_topic(int object_type, int object_instance, int property_id, int vtype, void *vptr, char *uuid_value)
{
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
  char topic[512];
  char topic_value[30000];
  char buf[2048];
  char *sptr;
  int mqtt_retry_limit = yaml_config_mqtt_connect_retry_limit();
  int mqtt_retry_interval = yaml_config_mqtt_connect_retry_interval();
  int rc, i;

  if (!mqtt_create_topic(object_type, object_instance, property_id, topic, sizeof(topic), MQTT_UPDATE_RESULT_TOPIC)) {
    printf("Failed to create MQTT topic: %d/%d\n", object_type, property_id);
    return(1);
  }

  if (mqtt_debug) {
    printf("MQTT publish topic: %s\n", topic);
  }

  switch(vtype) {
    case MQTT_TOPIC_VALUE_STRING:
      sptr = (char *)vptr;
      if (*sptr == '[' || *sptr == '{') {
        snprintf(topic_value, sizeof(topic_value), "{ \"value\" : %s , \"uuid\" : \"%s\" }", (char*)vptr,
          (uuid_value == NULL || !strlen(uuid_value)) ? "" : uuid_value);
      } else {
        snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" , \"uuid\" : \"%s\" }", (char*)vptr,
          (uuid_value == NULL || !strlen(uuid_value)) ? "" : uuid_value);
      }
      pubmsg.payload = topic_value;
      pubmsg.payloadlen = strlen(topic_value);
      break;

    case MQTT_TOPIC_VALUE_BACNET_STRING:
      characterstring_ansi_copy(buf, sizeof(buf), vptr);
      sptr = &buf[0];
      if (*sptr == '[' || *sptr == '{') {
        snprintf(topic_value, sizeof(topic_value), "{ \"value\" : %s , \"uuid\" : \"%s\" }", buf,
          (uuid_value == NULL || !strlen(uuid_value)) ? "" : uuid_value);
      } else {
        snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" , \"uuid\" : \"%s\" }", buf,
          (uuid_value == NULL || !strlen(uuid_value)) ? "" : uuid_value);
      }
      pubmsg.payload = topic_value;
      pubmsg.payloadlen = strlen(topic_value);
      break;

    case MQTT_TOPIC_VALUE_INTEGER:
      sprintf(buf, "%d", *((int*)vptr));
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" , \"uuid\" : \"%s\" }", buf,
        (uuid_value == NULL || !strlen(uuid_value)) ? "" : uuid_value);
      pubmsg.payload = topic_value;
      pubmsg.payloadlen = strlen(topic_value);
      break;

    case MQTT_TOPIC_VALUE_FLOAT:
      sprintf(buf, "%.6f", *((float*)vptr));
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" , \"uuid\" : \"%s\" }", buf,
        (uuid_value == NULL || !strlen(uuid_value)) ? "" : uuid_value);
      pubmsg.payload = topic_value;
      pubmsg.payloadlen = strlen(topic_value);
      break;

    default:
      printf("MQTT unsupported topic value: %d\n", vtype);
      return(1);
  }

  if (yaml_config_mqtt_connect_retry() && mqtt_retry_limit > 0) {
    for (i = 0; i < mqtt_retry_limit && !mqtt_client_connected; i++) {
      if (mqtt_debug) {
        printf("MQTT reconnect retry: %d/%d\n", (i + 1), mqtt_retry_limit);
      }

      mqtt_check_reconnect();
      sleep(mqtt_retry_interval);
    }

    if (i >= mqtt_retry_limit) {
      if (mqtt_debug) {
        printf("MQTT reconnect limit reached: %d\n", i);
      }

      return(1);
    }
  }

  opts.onSuccess = mqtt_on_send_success;
  opts.onFailure = mqtt_on_send_failure;
  opts.context = mqtt_client;

  pubmsg.qos = DEFAULT_PUB_QOS;
  pubmsg.retained = (yaml_config_mqtt_disable_persistence()) ? 0 : 1;
  rc = MQTTAsync_sendMessage(mqtt_client, topic, &pubmsg, &opts);
  if (mqtt_debug) {
    if (rc != MQTTASYNC_SUCCESS) {
      printf("MQTT failed to publish topic: \"%s\" , return code:%d\n", topic, rc);
    } else {
      printf("MQTT published topic: \"%s\"\n", topic);
    }
  }

  return(0);
}


/*
 * Subscribe to bacnet client whois command topics.
 */
int subscribe_bacnet_client_whois_command(void *context)
{
  MQTTAsync client = (MQTTAsync)context;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  int rc;
  const char *topics[] = {
    "bacnet/cmd/whois",
    NULL
  };

  if (mqtt_debug) {
     printf("MQTT subscribing to bacnet client command topic\n");
  }

  for (int i = 0; topics[i] != NULL; i++) {
    if (mqtt_debug) {
      printf("- topic[%d] = [%s]\n", i, topics[i]);
    }

    opts.onSuccess = mqtt_on_subscribe;
    opts.onFailure = mqtt_on_subscribe_failure;
    opts.context = client;
    rc = MQTTAsync_subscribe(mqtt_client, topics[i], 0, &opts);
    if (rc != MQTTASYNC_SUCCESS) {
      if (mqtt_debug) {
        printf("- WARNING: Failed to subscribe: %s\n", MQTTAsync_strerror(rc));
      }

      // return(1);
    }
  }

  return(0);
}


/*
 * Subscribe to bacnet client read value command topics.
 */
int subscribe_bacnet_client_read_value_command(void *context)
{
  MQTTAsync client = (MQTTAsync)context;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  int rc;
  const char *topics[] = {
    "bacnet/cmd/read_value",
    NULL
  };
    
  if (mqtt_debug) {
     printf("MQTT subscribing to bacnet client read value topic\n");
  }     
      
  for (int i = 0; topics[i] != NULL; i++) {
    if (mqtt_debug) {
      printf("- topic[%d] = [%s]\n", i, topics[i]);
    } 
        
    opts.onSuccess = mqtt_on_subscribe;
    opts.onFailure = mqtt_on_subscribe_failure;
    opts.context = client;
    rc = MQTTAsync_subscribe(mqtt_client, topics[i], 0, &opts);
    if (rc != MQTTASYNC_SUCCESS) {
      if (mqtt_debug) {
        printf("- WARNING: Failed to subscribe: %s\n", MQTTAsync_strerror(rc));
      }

      // return(1);
    }
  }

  return(0);
}


/*
 * Subscribe to bacnet client write value command topics.
 */
int subscribe_bacnet_client_write_value_command(void *context)
{
  MQTTAsync client = (MQTTAsync)context;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  int rc;
  const char *topics[] = {
    "bacnet/cmd/write_value",
    NULL
  };

  if (mqtt_debug) {
     printf("MQTT subscribing to bacnet client write value topic\n");
  }

  for (int i = 0; topics[i] != NULL; i++) {
    if (mqtt_debug) {
      printf("- topic[%d] = [%s]\n", i, topics[i]);
    }

    opts.onSuccess = mqtt_on_subscribe;
    opts.onFailure = mqtt_on_subscribe_failure;
    opts.context = client;
    rc = MQTTAsync_subscribe(mqtt_client, topics[i], 0, &opts);
    if (rc != MQTTASYNC_SUCCESS) {
      if (mqtt_debug) {
        printf("- WARNING: Failed to subscribe: %s\n", MQTTAsync_strerror(rc));
      }

      // return(1);
    }
  }

  return(0);
}


/*
 * Subscribe to bacnet client pics command topics.
 */
int subscribe_bacnet_client_pics_command(void *context)
{
  MQTTAsync client = (MQTTAsync)context;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  int rc;
  const char *topics[] = {
    "bacnet/cmd/pics",
    NULL
  };

  if (mqtt_debug) {
     printf("MQTT subscribing to bacnet client command topic\n");
  }

  for (int i = 0; topics[i] != NULL; i++) {
    if (mqtt_debug) {
      printf("- topic[%d] = [%s]\n", i, topics[i]);
    }

    opts.onSuccess = mqtt_on_subscribe;
    opts.onFailure = mqtt_on_subscribe_failure;
    opts.context = client;
    rc = MQTTAsync_subscribe(mqtt_client, topics[i], 0, &opts);
    if (rc != MQTTASYNC_SUCCESS) {
      if (mqtt_debug) {
        printf("- WARNING: Failed to subscribe: %s\n", MQTTAsync_strerror(rc));
      }

      // return(1);
    }
  }

  return(0);
}


/*
 * Subscribe to bacnet client point discovery command topics.
 */
int subscribe_bacnet_client_point_discovery_command(void *context)
{
  MQTTAsync client = (MQTTAsync)context;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  int rc;
  const char *topics[] = {
    "bacnet/cmd/point_discovery",
    "bacnet/cmd/points_info",
    NULL
  };

  if (mqtt_debug) {
     printf("MQTT subscribing to bacnet client command topic\n");
  }

  for (int i = 0; topics[i] != NULL; i++) {
    if (mqtt_debug) {
      printf("- topic[%d] = [%s]\n", i, topics[i]);
    }

    opts.onSuccess = mqtt_on_subscribe;
    opts.onFailure = mqtt_on_subscribe_failure;
    opts.context = client;
    rc = MQTTAsync_subscribe(mqtt_client, topics[i], 0, &opts);
    if (rc != MQTTASYNC_SUCCESS) {
      if (mqtt_debug) {
        printf("- WARNING: Failed to subscribe: %s\n", MQTTAsync_strerror(rc));
      }

      // return(1);
    }
  }

  return(0);
}


/*
 * Subscribe to bacnet client point list command topics.
 */
int subscribe_bacnet_client_point_list_command(void *context)
{
  MQTTAsync client = (MQTTAsync)context;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  int rc;
  const char *topics[] = {
    "bacnet/cmd/point_list",
    NULL
  };

  if (mqtt_debug) {
     printf("MQTT subscribing to bacnet client command topic\n");
  }

  for (int i = 0; topics[i] != NULL; i++) {
    if (mqtt_debug) {
      printf("- topic[%d] = [%s]\n", i, topics[i]);
    }

    opts.onSuccess = mqtt_on_subscribe;
    opts.onFailure = mqtt_on_subscribe_failure;
    opts.context = client;
    rc = MQTTAsync_subscribe(mqtt_client, topics[i], 0, &opts);
    if (rc != MQTTASYNC_SUCCESS) {
      if (mqtt_debug) {
        printf("- WARNING: Failed to subscribe: %s\n", MQTTAsync_strerror(rc));
      }

      // return(1);
    }
  }

  return(0);
}


/*
 * Subscribe to bacnet client topics.
 */ 
int mqtt_subscribe_to_bacnet_client_topics(void *context)
{   
  int i, n_commands = 0;
  char **commands;
    
  commands = yaml_config_bacnet_client_commands(&n_commands);
  if ((commands == NULL) || (n_commands == 0)) {
    return(0);
  } 
    
  for (i = 0; i < n_commands; i++) {
    if (!strncmp(commands[i], "whois", 5)) {
      if (subscribe_bacnet_client_whois_command(context)) {
        // return(1);
      }
    } else if (!strncmp(commands[i], "read_value", 10)) {
      if (subscribe_bacnet_client_read_value_command(context)) {
        // return(1);
      }
    } else if (!strncmp(commands[i], "write_value", 11)) {
      if (subscribe_bacnet_client_write_value_command(context)) {
        // return(1);
      }
    } else if (!strncmp(commands[i], "pics", 4)) {
      if (subscribe_bacnet_client_pics_command(context)) {
        // return(1);
      }
    }
  }

  /* subscribe to point discovery */
  subscribe_bacnet_client_point_discovery_command(context);

  /* subscribe to point list */
  subscribe_bacnet_client_point_list_command(context);

  return(0);
}


/*
 * Check if token is present.
 */
int is_reqquest_token_present(char *token)
{
  int i;

  for (i = 0; i < req_tokens_len; i++) {
    if (!strcmp(req_tokens[i], token)) {
      return(true);
    }
  }

  return(false);
}


/*
 * Initialize request tokens.
 */
void init_request_tokens(void)
{
  int i;
  char **tokens;

  tokens = yaml_config_bacnet_client_tokens(&req_tokens_len);
  for (i = 0; i < req_tokens_len; i++) {
    strcpy(req_tokens[i], tokens[i]);
  }
}


static BACNET_APPLICATION_DATA_VALUE *object_property_value(int32_t property_id)
{
  BACNET_APPLICATION_DATA_VALUE *value = NULL;
  int32_t index = 0;

  do {
    if (Property_Value_List[index].property_id == property_id) {
      value = Property_Value_List[index].value;
      break;
    }
    index++;
  } while (Property_Value_List[index].property_id != -1);

  return value;
}


/* Open JSON Output file */
static int OpenJsonFile(void)
{
  char tmp_name[128] = {0};
  char *str = "{\n";
  int fd, ret;

  strcpy(tmp_name, "/tmp/bacnet-server-pics-XXXXXX");
  fd = mkstemp(tmp_name);
  if (fd < 0) {
    printf("Error: Failed to create %s temp file!\n", tmp_name);
    return(1);
  }

  JsonFileFd = fd;
  strncpy(JsonFilePath, tmp_name, sizeof(JsonFilePath) - 1);

  ret = write(JsonFileFd, str, strlen(str));
  if (ret < 0){
    printf("Warning: Error writing to file: %s\n", JsonFilePath);
  }

  return(0);
}


/* Close JSON Output file */
static void CloseJsonFile(void)
{
  if (JsonFileFd >= 0) {
    close(JsonFileFd);
    JsonFileFd = -1;
    unlink(JsonFilePath);
  }
}


/* Write key/value pair into JSON Output file */
int WriteKeyValueToJsonFile(char *key, char *value, int comma_end)
{
  char *buf;
  int len = 128;

  if (key != NULL) {
    len += strlen(key);
  }

  if (value != NULL) {
    len += strlen(value);
  }

  buf = calloc(1, len);
  if (buf == NULL) {
    return(1);
  }

  if (value == NULL) {
    snprintf(buf, len - 1, "%s%s\n", key,
      (comma_end) ? "," : "");
  } else if (key == NULL) {
    snprintf(buf, len - 1, "%s%s\n", value,
      (comma_end) ? "," : "");
  } else if ((strlen(value) == 1) && (*value == '{' || *value == '[')) {
    snprintf(buf, len - 1, "\"%s\" : %s\n", key, value);
  } else {
    if (*value == '[' || *value == '{') {
      snprintf(buf, len - 1, "\"%s\" : %s%s\n", key, value,
        (comma_end) ? "," : "");
    } else {
      snprintf(buf, len - 1, "\"%s\" : \"%s\"%s\n", key, value,
        (comma_end) ? "," : "");
    }
  }

  if (write(JsonFileFd, buf, strlen(buf)) < 0) {
    free(buf);
    return(1);
  }

  free(buf);

  return(0);
}


/* Write key/value pair into JSON Output file */
int WriteKeyValueNoNLToJsonFile(char *key, char *value, int comma_end)
{
  char *buf;
  int len = 128;

  if (key != NULL) {
    len += strlen(key);
  }

  if (value != NULL) {
    len += strlen(value);
  }

  buf = calloc(1, len);
  if (buf == NULL) {
    return(1);
  }

  if (value == NULL) {
    snprintf(buf, len - 1, "%s%s", key,
      (comma_end) ? "," : "");
  } else if (key == NULL) {
    snprintf(buf, len - 1, "%s%s", value,
      (comma_end) ? "," : "");
  } else if ((strlen(value) == 1) && (*value == '{' || *value == '[')) {
    snprintf(buf, len - 1, "\"%s\" : %s", key, value);
  } else {
    if (*value == '[' || *value == '{') {
      snprintf(buf, len - 1, "\"%s\" : %s%s", key, value,
        (comma_end) ? "," : "");
    } else {
      snprintf(buf, len - 1, "\"%s\" : \"%s\"%s", key, value,
        (comma_end) ? "," : "");
    }
  }

  if (write(JsonFileFd, buf, strlen(buf)) < 0) {
    free(buf);
    return(1);
  }

  free(buf);

  return(0);
}


/*
 * Write command and NL to JSON file.
 */
int WriteCommaNLToJsonFile(void)
{
  char *str = ",\n";

  if (write(JsonFileFd, str, strlen(str)) < 0) {
    return(1);
  }

  return(0);
}


/* Strip double quotes in string */
void StripDoubleQuotes(char *str)
{
  char *ptr, *runner;

  if (*str == '"') {
    for(runner = str; *runner == '"'; runner++);
    for(ptr = str; *runner != '\0'; runner++, ptr++) {
      *ptr = *runner;
    }
    *ptr = '\0';
  }

  for (runner = &str[strlen(str) - 1]; *runner == '"'; *runner = '\0', runner--);
}


/* Strip double quotes in string */
void StripLastDoubleQuote(char *str)
{
  char *runner;

  for (runner = &str[strlen(str) - 1]; *runner == '"'; *runner = '\0', runner--);
}


int get_pics_target_address(bacnet_client_cmd_opts *opts)
{
  BACNET_MAC_ADDRESS mac = {0};

  if (opts->device_instance >= 0) {
    pics_Target_Device_Object_Instance = opts->device_instance;
  }

  if (strlen(opts->mac)) {
    if (address_mac_from_ascii(&mac, opts->mac)) {
      memcpy(&pics_Target_Address.mac[0], &mac.adr[0], mac.len);
      pics_Target_Address.mac_len = mac.len;
      pics_Target_Address.len = 0;
      pics_Target_Address.net = 0;
      pics_Provided_Targ_MAC = true;
    }
  }

  if (opts->dnet >= 0 && (opts->dnet <= BACNET_BROADCAST_NETWORK)) {
    pics_Target_Address.net = opts->dnet;
  }

  return(0);
}


static const char *protocol_services_supported_text(size_t bit_index)
{
    bool is_confirmed = false;
    size_t text_index = 0;
    bool found = false;
    const char *services_text = "unknown";

    found =
        apdu_service_supported_to_index(bit_index, &text_index, &is_confirmed);
    if (found) {
        if (is_confirmed) {
            services_text = bactext_confirmed_service_name(text_index);
        } else {
            services_text = bactext_unconfirmed_service_name(text_index);
        }
    }

    return services_text;
}


/* Initialize fields for a new Object */
static void StartNextObject(
    BACNET_READ_ACCESS_DATA *rpm_object, BACNET_OBJECT_ID *pNewObject)
{
    BACNET_PROPERTY_REFERENCE *rpm_property;
    Error_Detected = false;
    Property_List_Index = Property_List_Length = 0;
    rpm_object->object_type = pNewObject->type;
    rpm_object->object_instance = pNewObject->instance;
    rpm_property = calloc(1, sizeof(BACNET_PROPERTY_REFERENCE));
    rpm_object->listOfProperties = rpm_property;
    rpm_object->next = NULL;
    assert(rpm_property);
    rpm_property->propertyIdentifier = PROP_ALL;
    rpm_property->propertyArrayIndex = BACNET_ARRAY_ALL;
}


/* Build a list of device properties to request with RPM. */
static void BuildPropRequest(BACNET_READ_ACCESS_DATA *rpm_object)
{
    int i;
    /* To start with, StartNextObject() has prepopulated one propEntry,
     * but we will overwrite it and link more to it
     */
    BACNET_PROPERTY_REFERENCE *propEntry = rpm_object->listOfProperties;
    BACNET_PROPERTY_REFERENCE *oldEntry = rpm_object->listOfProperties;
    for (i = 0; Property_Value_List[i].property_id != -1; i++) {
        if (propEntry == NULL) {
            propEntry = calloc(1, sizeof(BACNET_PROPERTY_REFERENCE));
            assert(propEntry);
            oldEntry->next = propEntry;
        }
        propEntry->propertyIdentifier = Property_Value_List[i].property_id;
        propEntry->propertyArrayIndex = BACNET_ARRAY_ALL;
        propEntry->next = NULL;
        oldEntry = propEntry;
        propEntry = NULL;
    }
}


/** Print the property identifier name to stdout,
 *  handling the proprietary property numbers.
 * @param propertyIdentifier [in] The property identifier number.
 */
static void PPrint_Property_Identifier(unsigned propertyIdentifier)
{
    if (propertyIdentifier < 512) {
        fprintf(stdout, "%s", bactext_property_name(propertyIdentifier));
    } else {
        fprintf(stdout, "-- proprietary %u", propertyIdentifier);
    }
}


/** Send an RP request to read one property from the current Object.
 * Singly process large arrays too, like the Device Object's Object_List.
 * If GET_LIST_OF_ALL_RESPONSE failed, we will fall back to using just
 * the list of known Required properties for this type of object.
 *
 * @param device_instance [in] Our target device's instance.
 * @param pMyObject [in] The current Object's type and instance numbers.
 * @return The invokeID of the message sent, or 0 if reached the end
 *         of the property list.
 */
static uint8_t Read_Properties(
  uint32_t device_instance, BACNET_OBJECT_ID *pMyObject)
{
  uint8_t invoke_id = 0;
  struct special_property_list_t PropertyListStruct;
  unsigned int i = 0, j = 0;

  if ((!Has_RPM && (Property_List_Index == 0)) ||
    (Property_List_Length == 0)) {
    /* If we failed to get the Properties with RPM, just settle for what we
     * know is the fixed list of Required and Optional properties.
     * In practice, this should only happen for simple devices that don't
     * implement RPM or have really limited MAX_APDU size.
     */
    property_list_special(pMyObject->type, &PropertyListStruct);
    if (Optional_Properties) {
      Property_List_Length = PropertyListStruct.Required.count +
        PropertyListStruct.Optional.count;
    } else {
      Property_List_Length = PropertyListStruct.Required.count;
    }
    if (Property_List_Length > MAX_PROPS) {
      Property_List_Length = MAX_PROPS;
    }
    /* Copy this list for later one-by-one processing */
    for (i = 0; i < Property_List_Length; i++) {
      if (i < PropertyListStruct.Required.count) {
        Property_List[i] = PropertyListStruct.Required.pList[i];
      } else if (Optional_Properties) {
        Property_List[i] = PropertyListStruct.Optional.pList[j];
        j++;
      }
    }
    /* Just to be sure we terminate */
    Property_List[i] = -1;
  }
  if (Property_List[Property_List_Index] != -1) {
    int prop = Property_List[Property_List_Index];
    uint32_t array_index;
    IsLongArray = false;
    if (Using_Walked_List) {
      if (Walked_List_Length == 0) {
        array_index = 0;
      } else {
        array_index = Walked_List_Index;
      }
    } else {
      fprintf(stdout, "    ");
      PPrint_Property_Identifier(prop);
      fprintf(stdout, ": ");
      array_index = BACNET_ARRAY_ALL;

      switch (prop) {
        /* These are all potentially long arrays, so they may abort
         */
        case PROP_OBJECT_LIST:
        case PROP_STATE_TEXT:
        case PROP_STRUCTURED_OBJECT_LIST:
        case PROP_SUBORDINATE_ANNOTATIONS:
        case PROP_SUBORDINATE_LIST:
          IsLongArray = true;
          break;
      }
    }
    invoke_id = Send_Read_Property_Request(device_instance, pMyObject->type,
      pMyObject->instance, prop, array_index);
  }

  return invoke_id;
}


static void PrintHeading(void)
{
  BACNET_APPLICATION_DATA_VALUE *value = NULL;
  BACNET_OBJECT_PROPERTY_VALUE property_value;
  char buf[256];
  char str_value[128];
  int last_element_index = 0;

  printf("PICS 0\n");
  printf("BACnet Protocol Implementation Conformance Statement\n\n");

  value = object_property_value(PROP_VENDOR_NAME);
  if ((value != NULL) &&
    (value->tag == BACNET_APPLICATION_TAG_CHARACTER_STRING)) {
    printf("Vendor Name: \"%s\"\n",
      characterstring_value(&value->type.Character_String));
    if (WriteResultToJsonFile) {
      WriteKeyValueToJsonFile("Vendor Name", characterstring_value(&value->type.Character_String), true);
    }
  } else {
    printf("Vendor Name: \"your vendor name here\"\n");
    if (WriteResultToJsonFile) {
      WriteKeyValueToJsonFile("Vendor Name", "", true);
    }
  }

  value = object_property_value(PROP_PRODUCT_NAME);
  /* Best we can do with Product Name and Model Number is use the same text */
  if ((value != NULL) &&
    (value->tag == BACNET_APPLICATION_TAG_CHARACTER_STRING)) {
    printf("Product Name: \"%s\"\n",
      characterstring_value(&value->type.Character_String));
    if (WriteResultToJsonFile) {
      WriteKeyValueToJsonFile("Product Name", characterstring_value(&value->type.Character_String), true);
    }
  } else {
    printf("Product Name: \"your product name here\"\n");
    if (WriteResultToJsonFile) {
      WriteKeyValueToJsonFile("Product Name", "", true);
    }
  }

  value = object_property_value(PROP_MODEL_NAME);
  if ((value != NULL) &&
    (value->tag == BACNET_APPLICATION_TAG_CHARACTER_STRING)) {
    printf("Product Model Number: \"%s\"\n",
      characterstring_value(&value->type.Character_String));
    if (WriteResultToJsonFile) {
      WriteKeyValueToJsonFile("Product Model Number", characterstring_value(&value->type.Character_String), true);
    }
  } else {
    printf("Product Model Number: \"your model number here\"\n");
    if (WriteResultToJsonFile) {
      WriteKeyValueToJsonFile("Product Model Number", "", true);
    }
  }

  value = object_property_value(PROP_DESCRIPTION);
  if ((value != NULL) &&
    (value->tag == BACNET_APPLICATION_TAG_CHARACTER_STRING)) {
    printf("Product Description: \"%s\"\n\n",
      characterstring_value(&value->type.Character_String));
    if (WriteResultToJsonFile) {
      WriteKeyValueToJsonFile("Product Description", characterstring_value(&value->type.Character_String), true);
    }
  } else {
    printf("Product Description: "
      "\"your product description here\"\n\n");
    if (WriteResultToJsonFile) {
      WriteKeyValueToJsonFile("Product Description", "", true);
    }
  }

  printf("BIBBs Supported:\n");
  printf("{\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("BIBBs Supported", "[", false);
  }
  printf(" DS-RP-B\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"DS-RP-B\"", NULL, false);
  }

  printf("}\n\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("]", NULL, true);
  }
  printf("BACnet Standard Application Services Supported:\n");
  printf("{\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("BACnet Standard Application Services Supported", "[", false);
  }
  value = object_property_value(PROP_PROTOCOL_SERVICES_SUPPORTED);
  /* We have to process this bit string and determine which Object Types we
   * have, and show them
   */
  if ((value != NULL) && (value->tag == BACNET_APPLICATION_TAG_BIT_STRING)) {
    int i, len = bitstring_bits_used(&value->type.Bit_String);
    printf("-- services reported by this device\n");
    for (i = 0, last_element_index = len - 1; i < len; i++) {
      if (bitstring_bit(&value->type.Bit_String, (uint8_t)i)) {
        last_element_index = i;
      }
    }
    for (i = 0; i < len; i++) {
      if (bitstring_bit(&value->type.Bit_String, (uint8_t)i)) {
        printf(" %s\n", protocol_services_supported_text(i));
        if (WriteResultToJsonFile) {
          snprintf(buf, sizeof(buf) - 1, "\"%s\"", protocol_services_supported_text(i));
          WriteKeyValueToJsonFile(buf, NULL, (i != last_element_index));
        }
      }
    }
  } else {
    printf("-- use \'Initiate\' or \'Execute\' or both for services.\n");
    printf(" ReadProperty                   Execute\n");
    printf("-- ReadPropertyMultiple           Initiate Execute\n");
    printf("-- WriteProperty                  Initiate Execute\n");
    printf("-- DeviceCommunicationControl     Initiate Execute\n");
    printf("-- Who-Has                        Initiate Execute\n");
    printf("-- I-Have                         Initiate Execute\n");
    printf("-- Who-Is                         Initiate Execute\n");
    printf("-- I-Am                           Initiate Execute\n");
    printf("-- ReinitializeDevice             Initiate Execute\n");
    printf("-- AcknowledgeAlarm               Initiate Execute\n");
    printf("-- ConfirmedCOVNotification       Initiate Execute\n");
    printf("-- UnconfirmedCOVNotification     Initiate Execute\n");
    printf("-- ConfirmedEventNotification     Initiate Execute\n");
    printf("-- UnconfirmedEventNotification   Initiate Execute\n");
    printf("-- GetAlarmSummary                Initiate Execute\n");
    printf("-- GetEnrollmentSummary           Initiate Execute\n");
    printf("-- WritePropertyMultiple          Initiate Execute\n");
    printf("-- ReadRange                      Initiate Execute\n");
    printf("-- GetEventInformation            Initiate Execute\n");
    printf("-- SubscribeCOVProperty           Initiate Execute\n");
#ifdef BAC_ROUTING
    if (Target_Address.net == 0) {
      printf("-- Note: The following Routing Services are Supported:\n");
      printf("-- Who-Is-Router-To-Network    Initiate Execute\n");
      printf("-- I-Am-Router-To-Network      Initiate Execute\n");
      printf("-- Initialize-Routing-Table    Execute\n");
      printf("-- Initialize-Routing-Table-Ack Initiate\n");
    }
#endif
  }
  printf("}\n\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("]", NULL, true);
  }

  printf("Standard Object-Types Supported:\n");
  printf("{\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("Standard Object-Types Supported", "[", false);
  }
  value = object_property_value(PROP_PROTOCOL_OBJECT_TYPES_SUPPORTED);
  /* We have to process this bit string and determine which Object Types we
   * have, and show them
   */
  if ((value != NULL) && (value->tag == BACNET_APPLICATION_TAG_BIT_STRING)) {
    int i, len = bitstring_bits_used(&value->type.Bit_String);
    printf("-- objects reported by this device\n");
    for (i = 0, last_element_index = len - 1; i < len; i++) {
      if (bitstring_bit(&value->type.Bit_String, (uint8_t)i)) {
        last_element_index = i;
      }
    }

    for (i = 0; i < len; i++) {
      if (bitstring_bit(&value->type.Bit_String, (uint8_t)i)) {
        printf(" %s\n", bactext_object_type_name(i));
        if (WriteResultToJsonFile) {
          snprintf(buf, sizeof(buf) - 1, "\"%s\"", bactext_object_type_name(i));
          WriteKeyValueToJsonFile(buf, NULL, (i != last_element_index));
        }
      }
    }
  } else {
    printf("-- possible objects in this device\n");
    printf("-- use \'Createable\' or \'Deleteable\' or both or none.\n");
    printf("-- Analog Input            Createable Deleteable\n");
    printf("-- Analog Output           Createable Deleteable\n");
    printf("-- Analog Value            Createable Deleteable\n");
    printf("-- Binary Input            Createable Deleteable\n");
    printf("-- Binary Output           Createable Deleteable\n");
    printf("-- Binary Value            Createable Deleteable\n");
    printf("-- Device                  Createable Deleteable\n");
    printf("-- Multi-state Input       Createable Deleteable\n");
    printf("-- Multi-state Output      Createable Deleteable\n");
    printf("-- Multi-state Value       Createable Deleteable\n");
    printf("-- Structured View         Createable Deleteable\n");
    printf("-- Characterstring Value\n");
    printf("-- Datetime Value\n");
    printf("-- Integer Value\n");
    printf("-- Positive Integer Value\n");
    printf("-- Trend Log\n");
    printf("-- Load Control\n");
    printf("-- Bitstring Value\n");
    printf("-- Date Pattern Value\n");
    printf("-- Date Value\n");
    printf("-- Datetime Pattern Value\n");
    printf("-- Large Analog Value\n");
    printf("-- Octetstring Value\n");
    printf("-- Time Pattern Value\n");
    printf("-- Time Value\n");
  }
  printf("}\n\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("]", NULL, true);
  }

  printf("Data Link Layer Option:\n");
  printf("{\n");
  printf("-- choose the data link options supported\n");
  printf("-- ISO 8802-3, 10BASE5\n");
  printf("-- ISO 8802-3, 10BASE2\n");
  printf("-- ISO 8802-3, 10BASET\n");
  printf("-- ISO 8802-3, fiber\n");
  printf("-- ARCNET, coax star\n");
  printf("-- ARCNET, coax bus\n");
  printf("-- ARCNET, twisted pair star \n");
  printf("-- ARCNET, twisted pair bus\n");
  printf("-- ARCNET, fiber star\n");
  printf("-- ARCNET, twisted pair, EIA-485, Baud rate(s): 156000\n");
  printf("-- MS/TP master. Baud rate(s): 9600, 38400\n");
  printf("-- MS/TP slave. Baud rate(s): 9600, 38400\n");
  printf("-- Point-To-Point. EIA 232, Baud rate(s): 9600\n");
  printf("-- Point-To-Point. Modem, Baud rate(s): 9600\n");
  printf("-- Point-To-Point. Modem, Baud rate(s): 9600 to 115200\n");
  printf("-- BACnet/IP, 'DIX' Ethernet\n");
  printf("-- BACnet/IP, Other\n");
  printf("-- Other\n");
  printf("}\n\n");

  printf("Character Sets Supported:\n");
  printf("{\n");
  printf("-- choose any character sets supported\n");
  printf("-- ANSI X3.4\n");
  printf("-- IBM/Microsoft DBCS\n");
  printf("-- JIS C 6226\n");
  printf("-- ISO 8859-1\n");
  printf("-- ISO 10646 (UCS-4)\n");
  printf("-- ISO 10646 (UCS2)\n");
  printf("}\n\n");

  printf("Special Functionality:\n");
  printf("{\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("Special Functionality", "{", false);
  }
  value = object_property_value(PROP_MAX_APDU_LENGTH_ACCEPTED);
  printf(" Maximum APDU size in octets: ");
  if (value != NULL) {
    property_value.object_type = OBJECT_DEVICE;
    property_value.object_instance = 0;
    property_value.object_property = PROP_MAX_APDU_LENGTH_ACCEPTED;
    property_value.array_index = BACNET_ARRAY_ALL;
    property_value.value = value;
    bacapp_print_value(stdout, &property_value);
    bacapp_snprintf_value(str_value, sizeof(str_value), &property_value);
    if (WriteResultToJsonFile) {
      WriteKeyValueToJsonFile("Maximum APDU size in octets", str_value, false);
    }
  } else {
    printf("?");
  }
  printf("\n}\n\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("}", NULL, true);
  }

  printf("Default Property Value Restrictions:\n");
  printf("{\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("Default Property Value Restrictions", "{", false);
  }
  printf("  unsigned-integer: <minimum: 0; maximum: 4294967295>\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"unsigned-integer\" : {\"minimum\" : \"0\", \"maximum\": \"4294967295\"}", NULL, true);
  }
  printf("  signed-integer: <minimum: -2147483647; maximum: 2147483647>\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"signed-integer\" : {\"minimum\" : \"-2147483647\", \"maximum\" : \"2147483647\"}", NULL, true);
  }
  printf(
    "  real: <minimum: -3.40282347E38; maximum: 3.40282347E38; resolution: "
    "1.0>\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"real\" : {\"minimum\" : \"-3.40282347E38\" , \"maximum\": \"3.40282347E38\"}", NULL, true);
  }
  printf("  double: <minimum: 2.2250738585072016E-38; maximum: "
    "1.7976931348623157E38; resolution: 0.0001>\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"double\" : {\"minimum\" : \"2.2250738585072016E-38\" , \"maximum\" : \"1.7976931348623157E38\" , \"resolution\" : \"0.0001\"}", NULL, true);
  }
  printf("  date: <minimum: 01-January-1970; maximum: 31-December-2038>\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"date\" : {\"minimum\" : \"01-January-1970\" , \"maximum\" : \"31-December-2038\"}", NULL, true);
  }
  printf("  octet-string: <maximum length string: 122>\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"octet-string\" : {\"maximum length string\" : \"122\"}", NULL, true);
  }
  printf("  character-string: <maximum length string: 122>\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"character-string\" : {\"maximum length string\" : \"122\"}", NULL, true);
  }
  printf("  list: <maximum length list: 10>\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"list\" : {\"maximum length list\" : \"10\"}", NULL, true);
  }
  printf("  variable-length-array: <maximum length array: 10>\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"variable-length-array\" : {\"maximum length array\" : \"10\"}", NULL, false);
  }
  printf("}\n\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("}", NULL, true);
  }

  printf("Fail Times:\n");
  printf("{\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("Fail Times", "{", false);
  }
  printf("  Notification Fail Time: 2\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"Notification Fail Time\" : \"2\"", NULL, true);
  }
  printf("  Internal Processing Fail Time: 0.5\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"Internal Processing Fail Time\" : \"0.5\"", NULL, true);
  }
  printf("  Minimum ON/OFF Time: 5\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"Minimum ON/OFF Time\" : \"5\"", NULL, true);
  }
  printf("  Schedule Evaluation Fail Time: 1\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"Schedule Evaluation Fail Time\" : \"1\"", NULL, true);
  }
  printf("  External Command Fail Time: 1\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"External Command Fail Time\" : \"1\"", NULL, true);
  }
  printf("  Program Object State Change Fail Time: 2\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"Program Object State Change Fail Time\" : \"2\"", NULL, true);
  }
  printf("  Acknowledgement Fail Time: 2\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("\"Acknowledgement Fail Time\" : \"2\"", NULL, false);
  }
  printf("}\n\n");
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("}", NULL, true);
  }
}


static void Print_Device_Heading(void)
{
  printf("List of Objects in Test Device:\n");
  /* Print Opening brace, then kick off the Device Object */
  printf("{\n");  
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("List of Objects in Test Device", "[", false);
  }           
  printf("  { <<<\n"); /* And opening brace for the first object */
  if (WriteResultToJsonFile) {
    WriteKeyValueToJsonFile("{", NULL, false);
    FirstJsonItem = true;
    pics_object_json_start = true;
  }
}


/* Get property identifier name. */
static void get_property_identifier_name(unsigned propertyIdentifier, char *buf, int buf_len)
{
  if (propertyIdentifier < 512) {
    snprintf(buf, buf_len - 1, "%s", bactext_property_name(propertyIdentifier));
  } else {
    snprintf(buf, buf_len - 1, "proprietary %u", propertyIdentifier);
  }
}


/** Determine if this is a writable property, and, if so,
 * note that in the EPICS output.
 * This function may need a lot of customization for different implementations.
 *
 * @param object_type [in] The BACnet Object type of this object.
 * @note  object_instance [in] The ID number for this object.
 * @param rpm_property [in] Points to structure holding the Property,
 *                          Value, and Error information.
 */
static void CheckIsWritableProperty(BACNET_OBJECT_TYPE object_type,
    /* uint32_t object_instance, */
    BACNET_PROPERTY_REFERENCE *rpm_property,
    char *buf, int buf_len)
{
    bool bIsWritable = false;
    if ((object_type == OBJECT_ANALOG_OUTPUT) ||
        (object_type == OBJECT_BINARY_OUTPUT) ||
        (object_type == OBJECT_COMMAND) ||
        (object_type == OBJECT_MULTI_STATE_OUTPUT) ||
        (object_type == OBJECT_ACCESS_DOOR)) {
        if (rpm_property->propertyIdentifier == PROP_PRESENT_VALUE) {
            bIsWritable = true;
        }
    } else if (object_type == OBJECT_AVERAGING) {
        if ((rpm_property->propertyIdentifier == PROP_ATTEMPTED_SAMPLES) ||
            (rpm_property->propertyIdentifier == PROP_WINDOW_INTERVAL) ||
            (rpm_property->propertyIdentifier == PROP_WINDOW_SAMPLES)) {
            bIsWritable = true;
        }
    } else if (object_type == OBJECT_FILE) {
        if (rpm_property->propertyIdentifier == PROP_ARCHIVE) {
            bIsWritable = true;
        }
    } else if ((object_type == OBJECT_LIFE_SAFETY_POINT) ||
        (object_type == OBJECT_LIFE_SAFETY_ZONE)) {
        if (rpm_property->propertyIdentifier == PROP_MODE) {
            bIsWritable = true;
        }
    } else if (object_type == OBJECT_PROGRAM) {
        if (rpm_property->propertyIdentifier == PROP_PROGRAM_CHANGE) {
            bIsWritable = true;
        }
    } else if (object_type == OBJECT_PULSE_CONVERTER) {
        if (rpm_property->propertyIdentifier == PROP_ADJUST_VALUE) {
            bIsWritable = true;
        }
    } else if ((object_type == OBJECT_TRENDLOG) ||
        (object_type == OBJECT_EVENT_LOG) ||
        (object_type == OBJECT_TREND_LOG_MULTIPLE)) {
        if ((rpm_property->propertyIdentifier == PROP_ENABLE) ||
            (rpm_property->propertyIdentifier == PROP_RECORD_COUNT)) {
            bIsWritable = true;
        }
    } else if (object_type == OBJECT_LOAD_CONTROL) {
        if ((rpm_property->propertyIdentifier == PROP_REQUESTED_SHED_LEVEL) ||
            (rpm_property->propertyIdentifier == PROP_START_TIME) ||
            (rpm_property->propertyIdentifier == PROP_SHED_DURATION) ||
            (rpm_property->propertyIdentifier == PROP_DUTY_WINDOW) ||
            (rpm_property->propertyIdentifier == PROP_SHED_LEVELS)) {
            bIsWritable = true;
        }
    } else if ((object_type == OBJECT_ACCESS_ZONE) ||
        (object_type == OBJECT_ACCESS_USER) ||
        (object_type == OBJECT_ACCESS_RIGHTS) ||
        (object_type == OBJECT_ACCESS_CREDENTIAL)) {
        if (rpm_property->propertyIdentifier == PROP_GLOBAL_IDENTIFIER) {
            bIsWritable = true;
        }
    } else if (object_type == OBJECT_NETWORK_SECURITY) {
        if ((rpm_property->propertyIdentifier ==
                PROP_BASE_DEVICE_SECURITY_POLICY) ||
            (rpm_property->propertyIdentifier ==
                PROP_NETWORK_ACCESS_SECURITY_POLICIES) ||
            (rpm_property->propertyIdentifier == PROP_SECURITY_TIME_WINDOW) ||
            (rpm_property->propertyIdentifier == PROP_PACKET_REORDER_TIME) ||
            (rpm_property->propertyIdentifier == PROP_LAST_KEY_SERVER) ||
            (rpm_property->propertyIdentifier == PROP_SECURITY_PDU_TIMEOUT) ||
            (rpm_property->propertyIdentifier == PROP_DO_NOT_HIDE)) {
            bIsWritable = true;
        }
    }
    /* Add more checking here, eg for Time_Synchronization_Recipients,
     * Manual_Slave_Address_Binding, Object_Property_Reference,
     * Life Safety Tracking_Value, Reliability, Mode,
     * or Present_Value when Out_Of_Service is TRUE.
     */
    if (bIsWritable) {
        fprintf(stdout, " Writable");
        if (WriteResultToJsonFile) {
            StripLastDoubleQuote(buf);
            snprintf(&buf[strlen(buf)], buf_len - strlen(buf), " Writable");
        }
    }
}


/** Provide a nicer output for Supported Services and Object Types bitfields
 * and Date fields.
 * We have to override the library's normal bitfield print because the
 * EPICS format wants just T and F, and we want to provide (as comments)
 * the names of the active types.
 * These bitfields use opening and closing parentheses instead of braces.
 * We also limit the output to 4 bit fields per line.
 *
 * @param stream [in] Normally stdout
 * @param object_value [in] The structure holding this property's description
 *                          and value.
 * @return True if success.  Or otherwise.
 */
static bool PrettyPrintPropertyValue(
    FILE *stream, BACNET_OBJECT_PROPERTY_VALUE *object_value, char *buf, int buf_len)
{
    BACNET_APPLICATION_DATA_VALUE *value = NULL;
    bool status = true; /*return value */
    size_t len = 0, i = 0, j = 0;
    BACNET_PROPERTY_ID property = PROP_ALL;
    char short_month[4];

    value = object_value->value;
    property = object_value->object_property;
    if ((value != NULL) && (value->tag == BACNET_APPLICATION_TAG_BIT_STRING) &&
        ((property == PROP_PROTOCOL_OBJECT_TYPES_SUPPORTED) ||
            (property == PROP_PROTOCOL_SERVICES_SUPPORTED))) {
        len = bitstring_bits_used(&value->type.Bit_String);
        fprintf(stream, "( \n        ");
        if (WriteResultToJsonFile) {
            snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "[");
        }
        for (i = 0; i < len; i++) {
            fprintf(stream, "%s",
                bitstring_bit(&value->type.Bit_String, (uint8_t)i) ? "T" : "F");
            if (WriteResultToJsonFile) {
                snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "%s\"%s\"",
                    ((i == 0) ? "" : ","), bitstring_bit(&value->type.Bit_String, (uint8_t)i) ? "T" : "F");
            }
            if (i < len - 1) {
                fprintf(stream, ",");
            } else {
                fprintf(stream, " ");
            }
            /* Tried with 8 per line, but with the comments, got way too long.
             */
            if ((i == (len - 1)) || ((i % 4) == 3)) { /* line break every 4 */
                fprintf(stream, "   -- "); /* EPICS comments begin with "--" */
                /* Now rerun the same 4 bits, but print labels for true ones */
                for (j = i - (i % 4); j <= i; j++) {
                    if (bitstring_bit(&value->type.Bit_String, (uint8_t)j)) {
                        if (property == PROP_PROTOCOL_OBJECT_TYPES_SUPPORTED) {
                            fprintf(
                                stream, " %s,", bactext_object_type_name(j));
                        } else {
                            /* PROP_PROTOCOL_SERVICES_SUPPORTED */
                            fprintf(stream, " %s,",
                                protocol_services_supported_text(j));
                        }
                    } else { /* not supported */
                        fprintf(stream, ",");
                    }
                }
                fprintf(stream, "\n        ");
            }
        }
        fprintf(stream, ") \n");
        if (WriteResultToJsonFile) {
            snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "]");
        }
    } else if ((value != NULL) && (value->tag == BACNET_APPLICATION_TAG_DATE)) {
        /* eg, property == PROP_LOCAL_DATE
         * VTS needs (3-Aug-2011,4) or (8/3/11,4), so we'll use the
         * clearer, international form. */
        strncpy(short_month, bactext_month_name(value->type.Date.month), 3);
        short_month[3] = 0;
        fprintf(stream, "(%u-%3s-%u, %u)", (unsigned)value->type.Date.day,
            short_month, (unsigned)value->type.Date.year,
            (unsigned)value->type.Date.wday);
        snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "(%u-%3s-%u, %u)", (unsigned)value->type.Date.day,
            short_month, (unsigned)value->type.Date.year,
            (unsigned)value->type.Date.wday);
    } else if (value != NULL) {
        assert(false); /* How did I get here?  Fix your code. */
        /* Meanwhile, a fallback plan */
        status = bacapp_print_value(stdout, object_value);
    } else {
        fprintf(stream, "? \n");
        snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "?");
    }

    return status;
}


static int count_prop_data(BACNET_PROPERTY_REFERENCE *rpm_property)
{
  BACNET_APPLICATION_DATA_VALUE *value;
  int ctr  = 0;

  value = rpm_property->value;
  while (value != NULL) {
    ctr++;
    value = value->next;
  }

  return(ctr);
}


/** Print out the value(s) for one Property.
 * This function may be called repeatedly for one property if we are walking
 * through a list (Using_Walked_List is True) to show just one value of the
 * array per call. 
 *              
 * @param object_type [in] The BACnet Object type of this object.
 * @param object_instance [in] The ID number for this object.
 * @param rpm_property [in] Points to structure holding the Property,
 *                          Value, and Error information.
 */             
static void PrintReadPropertyData(BACNET_OBJECT_TYPE object_type,
    uint32_t object_instance,
    BACNET_PROPERTY_REFERENCE *rpm_property, char *buf, int buf_len)
{
    BACNET_OBJECT_PROPERTY_VALUE object_value; /* for bacapp printing */
    BACNET_APPLICATION_DATA_VALUE *value, *old_value;
    bool print_brace = false;
    KEY object_list_element;
    bool isSequence = false; /* Ie, will need bracketing braces {} */
    bool in_json_list = false;
    // int num_prop_data;
                    
    if (rpm_property == NULL) {
        fprintf(stdout, "    -- Null Property data \n");
        return;
    }       
    value = rpm_property->value;
    if (value == NULL) {
        /* Then we print the error information */
        fprintf(stdout, "?  -- BACnet Error: %s: %s\n",
            bactext_error_class_name((int)rpm_property->error.error_class),
            bactext_error_code_name((int)rpm_property->error.error_code));
        return;
    }

    // num_prop_data = count_prop_data(rpm_property);
    // printf("** num_prop_data: %d\n", num_prop_data);

    object_value.object_type = object_type;
    object_value.object_instance = object_instance;
    if ((value != NULL) && (value->next != NULL)) {
        /* Then this is an array of values.
         * But are we showing Values?  We (VTS3) want ? instead of {?,?} to show
         * up. */
        switch (rpm_property->propertyIdentifier) {
                /* Screen the Properties that can be arrays or Sequences */
            case PROP_PRESENT_VALUE:
            case PROP_PRIORITY_ARRAY:
                if (!ShowValues) {
                    fprintf(stdout, "? \n");
                    /* We want the Values freed below, but don't want to
                     * print anything for them.  To achieve this, swap
                     * out the Property for a non-existent Property
                     * and catch that below.  */
                    rpm_property->propertyIdentifier =
                        PROP_PROTOCOL_CONFORMANCE_CLASS;
                    break;
                }
                if (object_type == OBJECT_DATETIME_VALUE) {
                    break; /* A special case - no braces for this pair */
                }
                /* Else, fall through to normal processing. */
            default:
                /* Normal array: open brace */
                fprintf(stdout, "{ ");
                if (WriteResultToJsonFile) {
                    snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "[");
                }
                print_brace = true; /* remember to close it */
                break;
        }
    }

    if (!Using_Walked_List) {
        Walked_List_Index = Walked_List_Length = 0; /* In case we need this. */
    }
    /* value(s) loop until there is no "next" ... */
    while (value != NULL) {
        object_value.object_property = rpm_property->propertyIdentifier;
        object_value.array_index = rpm_property->propertyArrayIndex;
        object_value.value = value;
// printf("** object_value.object_type: %d\n", object_value.object_type);
        switch (rpm_property->propertyIdentifier) {
                /* These are all arrays, so they open and close with braces */
            case PROP_OBJECT_LIST:
            case PROP_STATE_TEXT:
            case PROP_STRUCTURED_OBJECT_LIST:
            case PROP_SUBORDINATE_ANNOTATIONS:
            case PROP_SUBORDINATE_LIST:
                if (Using_Walked_List) {
                    if ((rpm_property->propertyArrayIndex == 0) &&
                        (value->tag == BACNET_APPLICATION_TAG_UNSIGNED_INT)) {
                        /* Grab the value of the Object List length - don't
                         * print it! */
                        Walked_List_Length = value->type.Unsigned_Int;
                        if (rpm_property->propertyIdentifier ==
                            PROP_OBJECT_LIST) {
                            Object_List_Length = value->type.Unsigned_Int;
                        }
                        break;
                    } else {
                        assert(Walked_List_Index ==
                            (uint32_t)rpm_property->propertyArrayIndex);
                    }
                } else {
                    Walked_List_Index++;
                    /* If we got the whole Object List array in one RP call,
                     * keep the Index and List_Length in sync as we cycle
                     * through. */
                    if (rpm_property->propertyIdentifier == PROP_OBJECT_LIST) {
                        Object_List_Length = ++Object_List_Index;
                    }
                }
                if (Walked_List_Index == 1) {
                    /* If the array is empty (nothing for this first entry),
                     * Make it VTS3-friendly and don't show "Null" as a value.
                     */
                    if (value->tag == BACNET_APPLICATION_TAG_NULL) {
                        fprintf(stdout, "?\n        ");
                        break;
                    }

                    /* Open this Array of Objects for the first entry (unless
                     * opening brace has already printed, since this is an array
                     * of values[] ) */
                    if (value->next == NULL) {
                        fprintf(stdout, "{ \n        ");
                        if (WriteResultToJsonFile) {
                            snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "[");
                        }
                    } else {
                        fprintf(stdout, "\n        ");
                    }
                }

                if (rpm_property->propertyIdentifier == PROP_OBJECT_LIST) {
                    if (value->tag != BACNET_APPLICATION_TAG_OBJECT_ID) {
                        assert(value->tag ==
                            BACNET_APPLICATION_TAG_OBJECT_ID); /* Something
                                                                  not right
                                                                  here */
                        break;
                    }
                    /* Store the object list so we can interrogate
                       each object. */
                    object_list_element = KEY_ENCODE(value->type.Object_Id.type,
                        value->type.Object_Id.instance);
                    /* We don't have anything to put in the data pointer
                     * yet, so just leave it null.  The key is Key here. */
                    Keylist_Data_Add(Object_List, object_list_element, NULL);
                } else if (rpm_property->propertyIdentifier ==
                    PROP_STATE_TEXT) {
                    /* Make sure it fits within 31 chars for original VTS3
                     * limitation. If longer, take first 15 dash, and last 15
                     * chars. */
                    if (value->type.Character_String.length > 31) {
                        int iLast15idx =
                            value->type.Character_String.length - 15;
                        value->type.Character_String.value[15] = '-';
                        memcpy(&value->type.Character_String.value[16],
                            &value->type.Character_String.value[iLast15idx],
                            15);
                        value->type.Character_String.value[31] = 0;
                        value->type.Character_String.length = 31;
                    }
                } else if (rpm_property->propertyIdentifier ==
                    PROP_SUBORDINATE_LIST) {
                    if (value->tag != BACNET_APPLICATION_TAG_OBJECT_ID) {
                        assert(value->tag ==
                            BACNET_APPLICATION_TAG_OBJECT_ID); /* Something
                                                                  not right
                                                                  here */
                        break;
                    }
                    /* TODO: handle Sequence of { Device ObjID, Object ID }, */
                    isSequence = true;
                }

                /* If the object is a Sequence, it needs its own bracketing
                 * braces */
                if (isSequence) {
                    fprintf(stdout, "{");
                    if (WriteResultToJsonFile) {
                        snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "[");
                    }
                }
                bacapp_print_value(stdout, &object_value);
                in_json_list = true;
                if (WriteResultToJsonFile) {
                    if (rpm_property->propertyIdentifier == PROP_OBJECT_LIST &&
                      value->tag == BACNET_APPLICATION_TAG_OBJECT_ID &&
                      !is_object_type_supported(value->type.Object_Id.type)) {
                      printf("+++ Object not supported: %d\n", value->type.Object_Id.type);
                      in_json_list = false;
                    } else {
                      bacapp_snprintf_value2(&buf[strlen(buf)], buf_len - strlen(buf), &object_value);
                    }
                }
                if (isSequence) {
                    fprintf(stdout, "}");
                    if (WriteResultToJsonFile) {
                        snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "]");
                    }
                }

                if ((Walked_List_Index < Walked_List_Length) ||
                    (value->next != NULL)) {
                    /* There are more. */
                    fprintf(stdout, ", ");
                    if (WriteResultToJsonFile && in_json_list) {
                        snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ",");
                    }
                    if (!(Walked_List_Index % 3)) {
                        fprintf(stdout, "\n        ");
                    }
                } else {
                    fprintf(stdout, " } \n");
                    if (WriteResultToJsonFile) {
                        snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "]");
                    }
                }
                break;

            case PROP_PROTOCOL_OBJECT_TYPES_SUPPORTED:
            case PROP_PROTOCOL_SERVICES_SUPPORTED:
                PrettyPrintPropertyValue(stdout, &object_value, buf, buf_len);
                break;

                /* Our special non-existent case; do nothing further here. */
            case PROP_PROTOCOL_CONFORMANCE_CLASS:
                break;

            default:
                /* First, if this is a date type, it needs a different format
                 * for VTS, so pretty print it. */
                if (ShowValues &&
                    (object_value.value->tag == BACNET_APPLICATION_TAG_DATE)) {
                    /* This would be PROP_LOCAL_DATE, or OBJECT_DATETIME_VALUE,
                     * or OBJECT_DATE_VALUE                     */
                    PrettyPrintPropertyValue(stdout, &object_value, buf, buf_len);
                } else {
                    /* Some properties are presented just as '?' in an EPICS;
                     * screen these out here, unless ShowValues is true.  */
                    switch (rpm_property->propertyIdentifier) {
                        case PROP_DEVICE_ADDRESS_BINDING:
                            /* Make it VTS3-friendly and don't show "Null"
                             * as a value. */
                            if (value->tag == BACNET_APPLICATION_TAG_NULL) {
                                fprintf(stdout, "?");
                                if (WriteResultToJsonFile) {
                                  snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "\"?\"");
                                }
                                break;
                            }
                            /* Else, fall through for normal processing. */
                        case PROP_DAYLIGHT_SAVINGS_STATUS:
                        case PROP_LOCAL_TIME:
                        case PROP_LOCAL_DATE: /* Only if !ShowValues */
                        case PROP_PRESENT_VALUE:
                        case PROP_PRIORITY_ARRAY:
                        case PROP_RELIABILITY:
                        case PROP_UTC_OFFSET:
                        case PROP_DATABASE_REVISION:
                            if (!ShowValues) {
                                fprintf(stdout, "?");
                                if (WriteResultToJsonFile) {
                                  snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "\"?\"");
                                }
                                break;
                            }
                            /* Else, fall through and print value: */
                        default:
                            bacapp_print_value(stdout, &object_value);
                            if (WriteResultToJsonFile) {
                                bacapp_snprintf_value2(&buf[strlen(buf)], buf_len - strlen(buf), &object_value);
                            }
                            break;
                    }
                }
                if (value->next != NULL) {
                    /* there's more! */
                    fprintf(stdout, ",");
                    if (WriteResultToJsonFile) {
                        snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ",");
                    }
                } else {
                    if (print_brace) {
                        /* Closing brace for this multi-valued array */
                        fprintf(stdout, " }");
                        if (WriteResultToJsonFile) {
                            snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "]");
                        }
                    }
                    CheckIsWritableProperty(object_type, /* object_instance, */
                        rpm_property, buf, buf_len);
                    fprintf(stdout, "\n");
                }
                break;
        }

        old_value = value;
        value = value->next; /* next or NULL */
        free(old_value);
    } /* End while loop */
}


/** Print out the value(s) for one Property.
 * This function may be called repeatedly for one property if we are walking
 * through a list (Using_Walked_List is True) to show just one value of the
 * array per call. 
 *              
 * @param object_type [in] The BACnet Object type of this object.
 * @param object_instance [in] The ID number for this object.
 * @param rpm_property [in] Points to structure holding the Property,
 *                          Value, and Error information.
 */             
static void PrintReadPropertyData2(BACNET_OBJECT_TYPE object_type,
    uint32_t object_instance,
    BACNET_PROPERTY_REFERENCE *rpm_property, char **buf, int *buf_len)
{
    BACNET_OBJECT_PROPERTY_VALUE object_value; /* for bacapp printing */
    BACNET_APPLICATION_DATA_VALUE *value, *old_value;
    bool print_brace = false;
    KEY object_list_element;
    bool isSequence = false; /* Ie, will need bracketing braces {} */
    int num_prop_data;
    char *buf_ptr;

    if (rpm_property == NULL) {
        fprintf(stdout, "    -- Null Property data \n");
        return;
    }       
    value = rpm_property->value;
    if (value == NULL) {
        /* Then we print the error information */
        fprintf(stdout, "?  -- BACnet Error: %s: %s\n",
            bactext_error_class_name((int)rpm_property->error.error_class),
            bactext_error_code_name((int)rpm_property->error.error_code));
        return;
    }

    num_prop_data = count_prop_data(rpm_property);
    *buf_len = PROP_VALUE_MAX_LEN + (PROP_VALUE_MAX_LEN * num_prop_data);
    *buf = calloc(1, *buf_len);

    buf_ptr = *buf;

    object_value.object_type = object_type;
    object_value.object_instance = object_instance;
    if ((value != NULL) && (value->next != NULL)) {
        /* Then this is an array of values.
         * But are we showing Values?  We (VTS3) want ? instead of {?,?} to show
         * up. */
        switch (rpm_property->propertyIdentifier) {
                /* Screen the Properties that can be arrays or Sequences */
            case PROP_PRESENT_VALUE:
            case PROP_PRIORITY_ARRAY:
                if (!ShowValues) {
                    fprintf(stdout, "? \n");
                    /* We want the Values freed below, but don't want to
                     * print anything for them.  To achieve this, swap
                     * out the Property for a non-existent Property
                     * and catch that below.  */
                    rpm_property->propertyIdentifier =
                        PROP_PROTOCOL_CONFORMANCE_CLASS;
                    break;
                }
                if (object_type == OBJECT_DATETIME_VALUE) {
                    break; /* A special case - no braces for this pair */
                }
                /* Else, fall through to normal processing. */
            default:
                /* Normal array: open brace */
                fprintf(stdout, "{ ");
                if (WriteResultToJsonFile) {
                    snprintf(&buf_ptr[strlen(buf_ptr)], *buf_len - strlen(buf_ptr), "[");
                }
                print_brace = true; /* remember to close it */
                break;
        }
    }

    if (!Using_Walked_List) {
        Walked_List_Index = Walked_List_Length = 0; /* In case we need this. */
    }
    /* value(s) loop until there is no "next" ... */
    while (value != NULL) {
        object_value.object_property = rpm_property->propertyIdentifier;
        object_value.array_index = rpm_property->propertyArrayIndex;
        object_value.value = value;
        switch (rpm_property->propertyIdentifier) {
                /* These are all arrays, so they open and close with braces */
            case PROP_OBJECT_LIST:
            case PROP_STATE_TEXT:
            case PROP_STRUCTURED_OBJECT_LIST:
            case PROP_SUBORDINATE_ANNOTATIONS:
            case PROP_SUBORDINATE_LIST:
                if (Using_Walked_List) {
                    if ((rpm_property->propertyArrayIndex == 0) &&
                        (value->tag == BACNET_APPLICATION_TAG_UNSIGNED_INT)) {
                        /* Grab the value of the Object List length - don't
                         * print it! */
                        Walked_List_Length = value->type.Unsigned_Int;
                        if (rpm_property->propertyIdentifier ==
                            PROP_OBJECT_LIST) {
                            Object_List_Length = value->type.Unsigned_Int;
                        }
                        break;
                    } else {
                        assert(Walked_List_Index ==
                            (uint32_t)rpm_property->propertyArrayIndex);
                    }
                } else {
                    Walked_List_Index++;
                    /* If we got the whole Object List array in one RP call,
                     * keep the Index and List_Length in sync as we cycle
                     * through. */
                    if (rpm_property->propertyIdentifier == PROP_OBJECT_LIST) {
                        Object_List_Length = ++Object_List_Index;
                    }
                }
                if (Walked_List_Index == 1) {
                    /* If the array is empty (nothing for this first entry),
                     * Make it VTS3-friendly and don't show "Null" as a value.
                     */
                    if (value->tag == BACNET_APPLICATION_TAG_NULL) {
                        fprintf(stdout, "?\n        ");
                        break;
                    }

                    /* Open this Array of Objects for the first entry (unless
                     * opening brace has already printed, since this is an array
                     * of values[] ) */
                    if (value->next == NULL) {
                        fprintf(stdout, "{ \n        ");
                        if (WriteResultToJsonFile) {
                            snprintf(&buf_ptr[strlen(buf_ptr)], *buf_len - strlen(buf_ptr), "[");
                        }
                    } else {
                        fprintf(stdout, "\n        ");
                    }
                }

                if (rpm_property->propertyIdentifier == PROP_OBJECT_LIST) {
                    if (value->tag != BACNET_APPLICATION_TAG_OBJECT_ID) {
                        assert(value->tag ==
                            BACNET_APPLICATION_TAG_OBJECT_ID); /* Something
                                                                  not right
                                                                  here */
                        break;
                    }
                    /* Store the object list so we can interrogate
                       each object. */
                    object_list_element = KEY_ENCODE(value->type.Object_Id.type,
                        value->type.Object_Id.instance);
                    /* We don't have anything to put in the data pointer
                     * yet, so just leave it null.  The key is Key here. */
                    Keylist_Data_Add(Object_List, object_list_element, NULL);
                } else if (rpm_property->propertyIdentifier ==
                    PROP_STATE_TEXT) {
                    /* Make sure it fits within 31 chars for original VTS3
                     * limitation. If longer, take first 15 dash, and last 15
                     * chars. */
                    if (value->type.Character_String.length > 31) {
                        int iLast15idx =
                            value->type.Character_String.length - 15;
                        value->type.Character_String.value[15] = '-';
                        memcpy(&value->type.Character_String.value[16],
                            &value->type.Character_String.value[iLast15idx],
                            15);
                        value->type.Character_String.value[31] = 0;
                        value->type.Character_String.length = 31;
                    }
                } else if (rpm_property->propertyIdentifier ==
                    PROP_SUBORDINATE_LIST) {
                    if (value->tag != BACNET_APPLICATION_TAG_OBJECT_ID) {
                        assert(value->tag ==
                            BACNET_APPLICATION_TAG_OBJECT_ID); /* Something
                                                                  not right
                                                                  here */
                        break;
                    }
                    /* TODO: handle Sequence of { Device ObjID, Object ID }, */
                    isSequence = true;
                }

                /* If the object is a Sequence, it needs its own bracketing
                 * braces */
                if (isSequence) {
                    fprintf(stdout, "{");
                    if (WriteResultToJsonFile) {
                        snprintf(&buf_ptr[strlen(buf_ptr)], *buf_len - strlen(buf_ptr), "[");
                    }
                }
                bacapp_print_value(stdout, &object_value);
                if (WriteResultToJsonFile) {
                    bacapp_snprintf_value2(&buf_ptr[strlen(buf_ptr)], *buf_len - strlen(buf_ptr), &object_value);
                }
                if (isSequence) {
                    fprintf(stdout, "}");
                    if (WriteResultToJsonFile) {
                        snprintf(&buf_ptr[strlen(buf_ptr)], *buf_len - strlen(buf_ptr), "]");
                    }
                }

                if ((Walked_List_Index < Walked_List_Length) ||
                    (value->next != NULL)) {
                    /* There are more. */
                    fprintf(stdout, ", ");
                    if (WriteResultToJsonFile) {
                        snprintf(&buf_ptr[strlen(buf_ptr)], *buf_len - strlen(buf_ptr), ",");
                    }
                    if (!(Walked_List_Index % 3)) {
                        fprintf(stdout, "\n        ");
                    }
                } else {
                    fprintf(stdout, " } \n");
                    if (WriteResultToJsonFile) {
                        snprintf(&buf_ptr[strlen(buf_ptr)], *buf_len - strlen(buf_ptr), "]");
                    }
                }
                break;

            case PROP_PROTOCOL_OBJECT_TYPES_SUPPORTED:
            case PROP_PROTOCOL_SERVICES_SUPPORTED:
                PrettyPrintPropertyValue(stdout, &object_value, buf_ptr, *buf_len);
                break;

                /* Our special non-existent case; do nothing further here. */
            case PROP_PROTOCOL_CONFORMANCE_CLASS:
                break;

            default:
                /* First, if this is a date type, it needs a different format
                 * for VTS, so pretty print it. */
                if (ShowValues &&
                    (object_value.value->tag == BACNET_APPLICATION_TAG_DATE)) {
                    /* This would be PROP_LOCAL_DATE, or OBJECT_DATETIME_VALUE,
                     * or OBJECT_DATE_VALUE                     */
                    PrettyPrintPropertyValue(stdout, &object_value, buf_ptr, *buf_len);
                } else {
                    /* Some properties are presented just as '?' in an EPICS;
                     * screen these out here, unless ShowValues is true.  */
                    switch (rpm_property->propertyIdentifier) {
                        case PROP_DEVICE_ADDRESS_BINDING:
                            /* Make it VTS3-friendly and don't show "Null"
                             * as a value. */
                            if (value->tag == BACNET_APPLICATION_TAG_NULL) {
                                fprintf(stdout, "?");
                                if (WriteResultToJsonFile) {
                                  snprintf(&buf_ptr[strlen(buf_ptr)], *buf_len - strlen(buf_ptr), "\"?\"");
                                }
                                break;
                            }
                            /* Else, fall through for normal processing. */
                        case PROP_DAYLIGHT_SAVINGS_STATUS:
                        case PROP_LOCAL_TIME:
                        case PROP_LOCAL_DATE: /* Only if !ShowValues */
                        case PROP_PRESENT_VALUE:
                        case PROP_PRIORITY_ARRAY:
                        case PROP_RELIABILITY:
                        case PROP_UTC_OFFSET:
                        case PROP_DATABASE_REVISION:
                            if (!ShowValues) {
                                fprintf(stdout, "?");
                                if (WriteResultToJsonFile) {
                                  snprintf(&buf_ptr[strlen(buf_ptr)], *buf_len - strlen(buf_ptr), "\"?\"");
                                }
                                break;
                            }
                            /* Else, fall through and print value: */
                        default:
                            bacapp_print_value(stdout, &object_value);
                            if (WriteResultToJsonFile) {
                                bacapp_snprintf_value2(&buf_ptr[strlen(buf_ptr)], *buf_len - strlen(buf_ptr), &object_value);
                            }
                            break;
                    }
                }
                if (value->next != NULL) {
                    /* there's more! */
                    fprintf(stdout, ",");
                    if (WriteResultToJsonFile) {
                        snprintf(&buf_ptr[strlen(buf_ptr)], *buf_len - strlen(buf_ptr), ",");
                    }
                } else {
                    if (print_brace) {
                        /* Closing brace for this multi-valued array */
                        fprintf(stdout, " }");
                        if (WriteResultToJsonFile) {
                            snprintf(&buf_ptr[strlen(buf_ptr)], *buf_len - strlen(buf_ptr), "]");
                        }
                    }
                    CheckIsWritableProperty(object_type, /* object_instance, */
                        rpm_property, buf_ptr, *buf_len);
                    fprintf(stdout, "\n");
                }
                break;
        }

        old_value = value;
        value = value->next; /* next or NULL */
        free(old_value);
    } /* End while loop */
}


/** Process the RPM list, either printing out on success or building a
 *  properties list for later use.
 *  Also need to free the data in the list.
 *  If the present state is GET_HEADING_RESPONSE, store the results
 *  in globals for later use.
 * @param rpm_data [in] The list of RPM data received.
 * @param myState [in] The current state.
 * @return The next state of the EPICS state machine, normally NEXT_OBJECT
 *         if the RPM got good data, or GET_PROPERTY_REQUEST if we have to
 *         singly process the list of Properties.
 */
static EPICS_STATES ProcessRPMData(
    BACNET_READ_ACCESS_DATA *rpm_data, EPICS_STATES myState)
{
    BACNET_READ_ACCESS_DATA *old_rpm_data;
    BACNET_PROPERTY_REFERENCE *rpm_property;
    BACNET_PROPERTY_REFERENCE *old_rpm_property;
    BACNET_APPLICATION_DATA_VALUE *value;
    BACNET_APPLICATION_DATA_VALUE *old_value;
    bool bSuccess = true;
    EPICS_STATES nextState = myState; /* assume no change */
    /* Some flags to keep the output "pretty" -
     * wait and put these object lists at the end */
    bool bHasObjectList = false;
    bool bHasStructuredViewList = false;
    bool first_prop;
    int i = 0;
    int num_prop_data;
    int json_value_buf_len;
    char json_key_buf[128];
    char *json_value_buf;

    memset(json_key_buf, 0, sizeof(json_key_buf));

    while (rpm_data) {
printf(">>> 11 object_type: %d , object_instance: %d\n", rpm_data->object_type,
  rpm_data->object_instance);
        rpm_property = rpm_data->listOfProperties;
        first_prop = true;
        while (rpm_property) {
            if (!is_object_type_supported(rpm_data->object_type) ||
              !is_object_property_supported(rpm_data->object_type, rpm_property->propertyIdentifier)) {
printf(">>> Object (%d) / Property (%d) not supported, Skipping.\n", rpm_data->object_type,
  rpm_property->propertyIdentifier);
              value = rpm_property->value;
              while (value) {
                old_value = value;
                value = value->next;
                free(old_value);
              }

              old_rpm_property = rpm_property;
              rpm_property = rpm_property->next;
              free(old_rpm_property);
              continue;
            }

            num_prop_data = count_prop_data(rpm_property);
printf("1 num_prop_data: %d\n", num_prop_data);
            if (rpm_property->propertyIdentifier == PROP_OBJECT_LIST) {
              printf("++ PROP_OBJECT_LIST , myState: %d\n", myState);
            }
            json_value_buf_len = PROP_VALUE_MAX_LEN + (PROP_VALUE_MAX_LEN * num_prop_data);
            json_value_buf = calloc(1, json_value_buf_len);
            if (json_value_buf == NULL) {
              return(myState);
            }

            /* For the GET_LIST_OF_ALL_RESPONSE case,
             * just keep what property this was */
            if (myState == GET_LIST_OF_ALL_RESPONSE) {
                switch (rpm_property->propertyIdentifier) {
                    case PROP_OBJECT_LIST:
                        bHasObjectList = true; /* Will append below */
                        break;
                    case PROP_STRUCTURED_OBJECT_LIST:
                        bHasStructuredViewList = true;
                        break;
                    default:
                        Property_List[Property_List_Index] =
                            rpm_property->propertyIdentifier;
                        Property_List_Index++;
                        Property_List_Length++;
                        break;
                }       
                /* Free up the value(s) */
                value = rpm_property->value;
                while (value) {
                    old_value = value;
                    value = value->next;
                    free(old_value);
                }
            } else if (myState == GET_HEADING_RESPONSE) {
                Property_Value_List[i++].value = rpm_property->value;
                /* copy this pointer.
                 * On error, the pointer will be null
                 * We won't free these values; they will free at exit */
            } else {
                PPrint_Property_Identifier(rpm_property->propertyIdentifier);
                PrintReadPropertyData(rpm_data->object_type,
                    rpm_data->object_instance, rpm_property, json_value_buf, json_value_buf_len - 1);

                if (WriteResultToJsonFile) {
                    get_property_identifier_name(rpm_property->propertyIdentifier, json_key_buf, sizeof(json_key_buf));
                    StripDoubleQuotes(json_value_buf);
                    // WriteKeyValueToJsonFile(json_key_buf, json_value_buf, false);
                    if (first_prop) {
                      first_prop = false;
                    } else {
                      WriteCommaNLToJsonFile();
                    }
                    WriteKeyValueNoNLToJsonFile(json_key_buf, json_value_buf, false);
                    memset(json_value_buf, 0, json_value_buf_len);
                    memset(json_key_buf, 0, sizeof(json_key_buf));
                }
            }
            old_rpm_property = rpm_property;
            rpm_property = rpm_property->next;
            free(old_rpm_property);

            free(json_value_buf);
        }
        old_rpm_data = rpm_data;
        rpm_data = rpm_data->next;
        free(old_rpm_data);
    }

    /* Now determine the next state */
    if (myState == GET_HEADING_RESPONSE) {
        nextState = PRINT_HEADING;
        /* press ahead with or without the data */
    } else if (bSuccess && (myState == GET_ALL_RESPONSE)) {
        nextState = NEXT_OBJECT;
    } else if (bSuccess) { /* and GET_LIST_OF_ALL_RESPONSE */
        /* Now append the properties we waited on. */
        if (bHasStructuredViewList) {
            Property_List[Property_List_Index] = PROP_STRUCTURED_OBJECT_LIST;
            Property_List_Index++;
            Property_List_Length++;
        }
        if (bHasObjectList) {
            Property_List[Property_List_Index] = PROP_OBJECT_LIST;
            Property_List_Index++;
            Property_List_Length++;
        }
        /* Now insert the -1 list terminator, but don't count it. */
        Property_List[Property_List_Index] = -1;
        assert(Property_List_Length < MAX_PROPS);
        Property_List_Index = 0; /* Will start at top of the list */
        nextState = GET_PROPERTY_REQUEST;
    }
    return nextState;
}


/*
 * Check if the last character in the JSON file is a comma.
 */
static int is_json_file_last_comma(void)
{
  int is_comma = false;
  int n, l;
  char ch;

  n = lseek(JsonFileFd, 0, SEEK_END);
  if (n) {
    while (n--) {
      lseek(JsonFileFd, n, SEEK_SET);
      l = read(JsonFileFd, &ch, 1);
      if (l != 1) {
        break;
      }

      if (isspace(ch)) {
        continue;
      } else if (ch == ',') {
        is_comma = true;
        break;
      } else {
        break;
      }
    }
  }

  lseek(JsonFileFd, 0, SEEK_END);

  return(is_comma);
}


/*
 * Return true if object type is supported for PICS request.
 */
bool is_object_type_supported(int object_type)
{
  switch(object_type) {
    case OBJECT_DEVICE:
    case OBJECT_ANALOG_INPUT:
    case OBJECT_ANALOG_OUTPUT:
    case OBJECT_ANALOG_VALUE:
    case OBJECT_BINARY_INPUT:
    case OBJECT_BINARY_OUTPUT:
    case OBJECT_BINARY_VALUE:
    case OBJECT_MULTI_STATE_INPUT:
    case OBJECT_MULTI_STATE_OUTPUT:
    case OBJECT_MULTI_STATE_VALUE:
      return(true);
  }

  return(false);
}


/*
 * Return true if object property is supports for PICS request.
 */
bool is_object_property_supported(int object_type, int property_id)
{
  switch(object_type) {
    case OBJECT_DEVICE:
      return(true);

    case OBJECT_ANALOG_INPUT:
    case OBJECT_ANALOG_OUTPUT:
    case OBJECT_ANALOG_VALUE:
    case OBJECT_BINARY_INPUT:
    case OBJECT_BINARY_OUTPUT:
    case OBJECT_BINARY_VALUE:
    case OBJECT_MULTI_STATE_INPUT:
    case OBJECT_MULTI_STATE_OUTPUT:
    case OBJECT_MULTI_STATE_VALUE:
      switch(property_id) {
        case PROP_OBJECT_IDENTIFIER:
        case PROP_OBJECT_LIST:
        case PROP_OBJECT_NAME:
        case PROP_OBJECT_TYPE:
        case PROP_DESCRIPTION:
        case PROP_UNITS:
          return(true);
      }

      break;
  }

  return(false);
}


/*
 * Process Bacnet client PICS command.
 */
int process_bacnet_client_pics_command(bacnet_client_cmd_opts *opts)
{
  BACNET_READ_ACCESS_DATA *rpm_object = NULL;
  BACNET_OBJECT_ID myObject;
  BACNET_ADDRESS src;
  uint16_t pdu_len = 0;
  unsigned timeout = 100;
  uint8_t Rx_Buf[MAX_MPDU] = { 0 };
  uint8_t buffer[MAX_PDU] = { 0 };
  time_t elapsed_seconds = 0;
  time_t last_seconds = 0;
  time_t current_seconds = 0;
  time_t timeout_seconds = 0;
  unsigned max_apdu = 0;
  bool found = false;
  int ret;
  int json_file_len;
  int prop_value_buf_len;
  KEY nextKey;
  char json_key_buf[128] = {0};
  char *json_file_buff;
  char *prop_value_buf;

  int pics_ctr = 0;

  address_init();

  Object_List_Length = 0;
  Object_List_Index = 0;
  Object_List = Keylist_Create();

  Walked_List_Length = 0;
  Walked_List_Index = 0;
  Using_Walked_List = false;
  IsLongArray = false;

  Property_List_Length = 0;
  Property_List_Index = 0;
  memset((char*)&Property_List[0], 0, sizeof(Property_List));

  memset(&pics_Target_Address, 0, sizeof(pics_Target_Address));
  pics_Target_Device_Object_Instance = BACNET_MAX_INSTANCE;
  pics_Provided_Targ_MAC = false;
  memset(&src, 0, sizeof(BACNET_ADDRESS));

  Error_Detected = false;
  Last_Error_Class = 0;
  Last_Error_Code = 0;
  Error_Count = 0;
  Has_RPM = true;

  epics_page_number = opts->page_number;
  epics_page_size = (opts->page_size > 0) ? opts->page_size : EPICS_PAGE_SIZE_DEFAULT;
  total_device_objects = 0;

  pics_retry_ctr = 0;

  current_pics_object_type = -1;
  pics_object_json_start = false;

  ret = get_pics_target_address(opts);
  if (ret) {
    if (mqtt_debug) {
      printf("Error: Invalid PICS command options!\n");
    }

    return(1);
  }

  if (!pics_Provided_Targ_MAC) {
    if (mqtt_debug) {
      printf("Error: No target address for PICS command!\n");
    }

    return(1);
  }

  if (epics_pagination) {
    printf("-- page_number: %d\n", opts->page_number);
    printf("-- page_size: %d\n", opts->page_size);
  }

  current_seconds = time(NULL);
  timeout_seconds = (apdu_timeout() / 1000) * apdu_retries();

  found = address_bind_request(
    pics_Target_Device_Object_Instance, &max_apdu, &pics_Target_Address);
  if (!found) {
    if (pics_Target_Address.net > 0) {
      if (mqtt_debug) {
        printf("Sending WHO_IS\n");
      }
      Send_WhoIs_Remote(&pics_Target_Address,
        pics_Target_Device_Object_Instance,
        pics_Target_Device_Object_Instance);

        tsm_timer_milliseconds(2000);
    } else {
      if (max_apdu == 0) {
        max_apdu = MAX_APDU;
      }

      address_add_binding(
        pics_Target_Device_Object_Instance, max_apdu, &pics_Target_Address);
    }
  }

  /* open json output file */
  if (WriteResultToJsonFile && OpenJsonFile()) {
    printf("Error opening JSON output file!\n");
    return(1);
  }

  myObject.type = OBJECT_DEVICE;
  myObject.instance = pics_Target_Device_Object_Instance;
  myState = INITIAL_BINDING;
  do {
    last_seconds = current_seconds;
    current_seconds = time(NULL);
    if (current_seconds != last_seconds) {
      tsm_timer_milliseconds(
        (uint16_t)((current_seconds - last_seconds) * 1000));
    }

    switch (myState) {
      case INITIAL_BINDING:
        pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, timeout);
        if (pdu_len) {
          npdu_handler(&src, &Rx_Buf[0], pdu_len);
        }

        found = address_bind_request(
          pics_Target_Device_Object_Instance, &max_apdu, &pics_Target_Address);
        if (!found) {
          printf("- pics_Target_Address NOT_FOUND\n");
          elapsed_seconds += (current_seconds - last_seconds);
          if (elapsed_seconds > timeout_seconds) {
            printf("- Unable to bind to %u after waiting %ld seconds.\n",
              pics_Target_Device_Object_Instance, (long int)elapsed_seconds);
            break;
          }

          continue;
        } else {
          printf("- pics_Target_Address FOUND!\n");
          rpm_object = calloc(1, sizeof(BACNET_READ_ACCESS_DATA));
          assert(rpm_object);
          myState = GET_HEADING_INFO;
        }

        break;

      case GET_HEADING_INFO:
        last_seconds = current_seconds;
        StartNextObject(rpm_object, &myObject);
        BuildPropRequest(rpm_object);
        pics_Request_Invoke_ID = Send_Read_Property_Multiple_Request(
          buffer, MAX_PDU, pics_Target_Device_Object_Instance, rpm_object);
        if (pics_Request_Invoke_ID > 0) {
          elapsed_seconds = 0;
        } else {
          printf("\r-- Failed to get Heading info \n");
        }
        myState = GET_HEADING_RESPONSE;
        break;

      case PRINT_HEADING:
        PrintHeading();
        Print_Device_Heading();
        myState = GET_ALL_REQUEST;

      case GET_ALL_REQUEST:
      case GET_LIST_OF_ALL_REQUEST:
        /* "list" differs in ArrayIndex only */
        /* Update times; aids single-step debugging */
        last_seconds = current_seconds;
        StartNextObject(rpm_object, &myObject);

        pics_Request_Invoke_ID = Send_Read_Property_Multiple_Request(
          buffer, MAX_PDU, pics_Target_Device_Object_Instance, rpm_object);
        if (pics_Request_Invoke_ID > 0) {
          elapsed_seconds = 0;
          if (myState == GET_LIST_OF_ALL_REQUEST) {
            myState = GET_LIST_OF_ALL_RESPONSE;
          } else {
            myState = GET_ALL_RESPONSE;
          }
        }
        break;

      case GET_HEADING_RESPONSE:
      case GET_ALL_RESPONSE:
      case GET_LIST_OF_ALL_RESPONSE:
        pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, timeout);

        /* process */
        if (pdu_len) {
          npdu_handler(&src, &Rx_Buf[0], pdu_len);
        }

        if ((pics_Read_Property_Multiple_Data.new_data) &&
          (pics_Request_Invoke_ID == pics_Read_Property_Multiple_Data.service_data.invoke_id)) {
          pics_Read_Property_Multiple_Data.new_data = false;
printf(">>> 1 object_type: %d , object_instance: %d\n", pics_Read_Property_Multiple_Data.rpm_data->object_type,
  pics_Read_Property_Multiple_Data.rpm_data->object_instance);
          myState = ProcessRPMData(pics_Read_Property_Multiple_Data.rpm_data, myState);
          if (tsm_invoke_id_free(pics_Request_Invoke_ID)) {
            pics_Request_Invoke_ID = 0;
          } else {
            assert(false); /* How can this be? */
            pics_Request_Invoke_ID = 0;
          }
          elapsed_seconds = 0;
          pics_retry_ctr = 0;
        } else  if (tsm_invoke_id_free(pics_Request_Invoke_ID)) {
          elapsed_seconds = 0;
          pics_Request_Invoke_ID = 0;
          if (myState == GET_HEADING_RESPONSE) {
            myState = PRINT_HEADING;
          /* just press ahead without the data */
          } else if (Error_Detected) {
            if (Last_Error_Code ==
              ERROR_CODE_REJECT_UNRECOGNIZED_SERVICE) {
              /* The normal case for Device Object */
              /* Was it because the Device can't do RPM? */
              Has_RPM = false;
              myState = GET_PROPERTY_REQUEST;
            } else if (Last_Error_Code ==
                            ERROR_CODE_ABORT_SEGMENTATION_NOT_SUPPORTED) {
              myState = GET_PROPERTY_REQUEST;
              StartNextObject(rpm_object, &myObject);
            } else if (myState == GET_ALL_RESPONSE) {
              /* Try again, just to get a list of properties. */
              myState = GET_LIST_OF_ALL_REQUEST;
            } else {
              /* Else drop back to RP. */
              myState = GET_PROPERTY_REQUEST;
              StartNextObject(rpm_object, &myObject);
            }
          } else if (Has_RPM) {
            myState = GET_ALL_REQUEST; /* Let's try again */
            pics_retry_ctr++;
            if (pics_retry_ctr > pics_retry_max) {
              myObject.type = MAX_BACNET_OBJECT_TYPE;
              if (FirstJsonItem) {
                printf("  } \n");
                if (WriteResultToJsonFile) {
                  WriteKeyValueToJsonFile("}", NULL, false);
                }
              }
              break;
            }
          } else {
            myState = GET_PROPERTY_REQUEST;
            pics_retry_ctr = 0;
          }
        } else if (tsm_invoke_id_failed(pics_Request_Invoke_ID)) {
          fprintf(stderr, "\rError: TSM Timeout!\n");
          tsm_free_invoke_id(pics_Request_Invoke_ID);
          pics_Request_Invoke_ID = 0;
          elapsed_seconds = 0;
          if (myState == GET_HEADING_RESPONSE) {
            myState = PRINT_HEADING;
            pics_retry_ctr = 0;
            /* just press ahead without the data */
          } else {
            myState = GET_ALL_REQUEST; /* Let's try again */
            pics_retry_ctr++;
            if (pics_retry_ctr > pics_retry_max) {
              myObject.type = MAX_BACNET_OBJECT_TYPE;
              if (FirstJsonItem) {
                printf("  } \n");
                if (WriteResultToJsonFile) {
                  WriteKeyValueToJsonFile("}", NULL, false);
                }
              }
              break;
            }
          }
        } else if (Error_Detected) {
          /* Don't think we'll ever actually reach this point. */
          elapsed_seconds = 0;
          pics_Request_Invoke_ID = 0;
          if (myState == GET_HEADING_RESPONSE) {
            myState = PRINT_HEADING;
            /* just press ahead without the data */
          } else {
            myState = NEXT_OBJECT;
          }/* Give up and move on to the
                                                  next. */
          Error_Count++;
        } else {
          printf(">>> NOT SUPPOSED TO BE HERE!\n");
        }

        break;

      case GET_PROPERTY_REQUEST:
        Error_Detected = false;
        /* Update times; aids single-step debugging */
        last_seconds = current_seconds;
        elapsed_seconds = 0;
        pics_Request_Invoke_ID =
          Read_Properties(pics_Target_Device_Object_Instance, &myObject);
        if (pics_Request_Invoke_ID == 0) {
          /* Reached the end of the list. */
          myState = NEXT_OBJECT; /* Move on to the next. */
        } else {
          myState = GET_PROPERTY_RESPONSE;
        }
        break;

      case GET_PROPERTY_RESPONSE:
        /* returns 0 bytes on timeout */
        pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, timeout);

        /* process */
        if (pdu_len) {
          npdu_handler(&src, &Rx_Buf[0], pdu_len);
        }

        if ((pics_Read_Property_Multiple_Data.new_data) &&
          (pics_Request_Invoke_ID == pics_Read_Property_Multiple_Data.service_data.invoke_id)) {
          pics_Read_Property_Multiple_Data.new_data = false;
          prop_value_buf = NULL;
          prop_value_buf_len = 0;
printf(">>> 2 object_type: %d , object_instance: %d\n", pics_Read_Property_Multiple_Data.rpm_data->object_type,
  pics_Read_Property_Multiple_Data.rpm_data->object_instance);

          if (!is_object_type_supported(pics_Read_Property_Multiple_Data.rpm_data->object_type) ||
            !is_object_property_supported(pics_Read_Property_Multiple_Data.rpm_data->object_type,
            pics_Read_Property_Multiple_Data.rpm_data->listOfProperties->propertyIdentifier)) {
printf(">>> Object (%d) / Property (%d) not supported. Skipping.\n", pics_Read_Property_Multiple_Data.rpm_data->object_type,
  pics_Read_Property_Multiple_Data.rpm_data->listOfProperties->propertyIdentifier);
            BACNET_APPLICATION_DATA_VALUE *x_value, *x_old_value;
            x_value = pics_Read_Property_Multiple_Data.rpm_data->listOfProperties->value;
            while (x_value != NULL) {
              x_old_value = x_value;
              x_value = x_value->next;
              free(x_old_value);
            }
          } else {

          PrintReadPropertyData2(
            pics_Read_Property_Multiple_Data.rpm_data->object_type,
            pics_Read_Property_Multiple_Data.rpm_data->object_instance,
            pics_Read_Property_Multiple_Data.rpm_data->listOfProperties,
            &prop_value_buf, &prop_value_buf_len);

          if (WriteResultToJsonFile) {
            get_property_identifier_name(pics_Read_Property_Multiple_Data.rpm_data->listOfProperties->propertyIdentifier,
              json_key_buf, sizeof(json_key_buf));
            StripDoubleQuotes(prop_value_buf);
            if (FirstJsonItem) {
              FirstJsonItem = false;
            } else {
              if (!is_json_file_last_comma()) {
                WriteKeyValueNoNLToJsonFile(",\n", NULL, false);
              }
            }

            if (!strcmp(json_key_buf, "object-list")) {
              if (strlen(prop_value_buf) > 0) {
                if ((prop_value_buf[0] == '[') && (prop_value_buf[1] == '[')) {
                  WriteKeyValueNoNLToJsonFile(json_key_buf, prop_value_buf, false);
                } else {
                  WriteKeyValueNoNLToJsonFile(NULL, prop_value_buf, false);
                }
              }
            } else {
              WriteKeyValueNoNLToJsonFile(json_key_buf, prop_value_buf, false);
              if (!strcmp(json_key_buf, "object-identifier")) {
                printf("++ OBJECT-IDENTIFIER\n");
              }
            }
          }
          }

          if (tsm_invoke_id_free(pics_Request_Invoke_ID)) {
            pics_Request_Invoke_ID = 0;
          } else {
            assert(false); /* How can this be? */
            pics_Request_Invoke_ID = 0;
          }
          elapsed_seconds = 0;
          /* Advance the property (or Array List) index */
          if (Using_Walked_List) {
            Walked_List_Index++;
            if (Walked_List_Index > Walked_List_Length) {
              /* go on to next property */
              Property_List_Index++;
              Using_Walked_List = false;
            }
          } else {
            Property_List_Index++;
          }
          myState = GET_PROPERTY_REQUEST; /* Go fetch next Property */
          if (prop_value_buf) {
            free(prop_value_buf);
          }
        } else if (tsm_invoke_id_free(pics_Request_Invoke_ID)) {
          pics_Request_Invoke_ID = 0;
          elapsed_seconds = 0;
          myState = GET_PROPERTY_REQUEST;
          if (Error_Detected) {
            if ((Last_Error_Class != ERROR_CLASS_PROPERTY) &&
              (Last_Error_Code != ERROR_CODE_UNKNOWN_PROPERTY)) {
              if (IsLongArray) {
                /* Change to using a Walked List and retry this
                 * property */
                Using_Walked_List = true;
                Walked_List_Index = Walked_List_Length = 0;
              } else {
                /* OK, skip this one and try the next property.
                 */
                fprintf(stdout, "    -- Failed to get ");
                PPrint_Property_Identifier(
                  Property_List[Property_List_Index]);
                fprintf(stdout, " \n");
                Error_Count++;
                Property_List_Index++;
                if (Property_List_Index >= Property_List_Length) {
                  /* Give up and move on to the next. */
                  myState = NEXT_OBJECT;
                }
              }
            } else {
              fprintf(stdout, "    -- unknown property\n");
              Error_Count++;
              Property_List_Index++;
              if (Property_List_Index >= Property_List_Length) {
                /* Give up and move on to the next. */
                myState = NEXT_OBJECT;
              }
            }
          }
        } else if (tsm_invoke_id_failed(pics_Request_Invoke_ID)) {
          fprintf(stderr, "\rError: TSM Timeout!\n");
          tsm_free_invoke_id(pics_Request_Invoke_ID);
          elapsed_seconds = 0;
          pics_Request_Invoke_ID = 0;
          myState = 3; /* Let's try again, same Property */
        } else if (Error_Detected) {
          /* Don't think we'll ever actually reach this point. */
          elapsed_seconds = 0;
          pics_Request_Invoke_ID = 0;
          myState = NEXT_OBJECT; /* Give up and move on to the next. */
          Error_Count++;
        }
        break;

      case NEXT_OBJECT:
        if (myObject.type == OBJECT_DEVICE) {
          total_device_objects = Keylist_Count(Object_List);
          printf("  -- Found %d Objects \n", Keylist_Count(Object_List));
          Object_List_Index = -1; /* start over (will be incr to 0) */
          if (ShowDeviceObjectOnly) {
            /* Closing brace for the Device Object */
            printf("  }, \n");
            if (WriteResultToJsonFile) {
              WriteKeyValueToJsonFile("}", NULL, true);
            }
            /* done with all Objects, signal end of this while loop
             */
            myObject.type = MAX_BACNET_OBJECT_TYPE;
            break;
          }
        }
        /* Advance to the next object, as long as it's not the Device
         * object */
        do {
          Object_List_Index++;
          if (Object_List_Index < Keylist_Count(Object_List)) {
            nextKey = Keylist_Key(Object_List, Object_List_Index);
            myObject.type = KEY_DECODE_TYPE(nextKey);
            myObject.instance = KEY_DECODE_ID(nextKey);
            /* Don't re-list the Device Object among its objects */
            if (myObject.type == OBJECT_DEVICE) {
              continue;
            }
            /* Closing brace for the previous Object */
            printf("  }, <<<\n");
            if (WriteResultToJsonFile) {
              WriteKeyValueToJsonFile("}", NULL, true);
            }
            /* Opening brace for the new Object */
            printf("  { <<<\n");
            if (WriteResultToJsonFile) {
              WriteKeyValueToJsonFile("{", NULL, false);
              FirstJsonItem = true;
            }
            /* Test code:
              if ( myObject.type == OBJECT_STRUCTURED_VIEW )
                printf( "    -- Structured View %d \n",
                  myObject.instance );
             */
          } else {
            /* Closing brace for the last Object */
            printf("  } <<<\n");
            if (WriteResultToJsonFile) {
              WriteKeyValueToJsonFile("}", NULL, false);
            }
            /* done with all Objects, signal end of this while loop
             */
            myObject.type = MAX_BACNET_OBJECT_TYPE;
          }
          if (Has_RPM) {
            myState = GET_ALL_REQUEST;
          } else {
            myState = GET_PROPERTY_REQUEST;
            StartNextObject(rpm_object, &myObject);
          }
        } while (myObject.type == OBJECT_DEVICE);
        /* Else, don't re-do the Device Object; move to the next object.
         */
        break;

      default:
        assert(false);
        break;
    }

    if (!found || (pics_Request_Invoke_ID > 0)) {
      /* increment timer - exit if timed out */
      elapsed_seconds += (current_seconds - last_seconds);
      if (elapsed_seconds > timeout_seconds) {
        fprintf(stderr, "\rError: APDU Timeout! (%lds)\n",
          (long int)elapsed_seconds);
        break;
      }
    }

    pics_ctr++;
  } while (myObject.type < MAX_BACNET_OBJECT_TYPE);

  if (Error_Count > 0) {
    fprintf(stdout, "\r-- Found %d Errors\n", Error_Count);
  }

  /* Closing brace for all Objects, if we got any, and closing footer  */
  if (myState != INITIAL_BINDING) {
    printf("} \n");
    if (WriteResultToJsonFile) {
      WriteKeyValueToJsonFile("]}", NULL, false);
    }   
    printf("End of BACnet Protocol Implementation Conformance Statement\n");
    printf("\n");
  } else {
    if (WriteResultToJsonFile) {
      WriteKeyValueToJsonFile("}", NULL, false);
    }
  }

  if (epics_pagination) {
    printf("- Total Device Objects: %d\n", total_device_objects);
    printf("- Object_List_Index: %d\n", Object_List_Index);
  }

  /* read the contents of the result and publish */
  if (WriteResultToJsonFile) {
    json_file_len = lseek(JsonFileFd, 0, SEEK_END);
    printf("json_file_len: %d\n", json_file_len);
    json_file_buff = calloc(1, json_file_len + 1);
    if (json_file_buff == NULL) {
      fprintf(stderr, "Error: Failed to allocate memory for PICs reply!\n");
      pics_request_locked = false;
      return(1);
    }

    lseek(JsonFileFd, 0, SEEK_SET);
    ret = read(JsonFileFd, json_file_buff, json_file_len);

    publish_bacnet_client_pics_result(opts, json_file_buff);
    free(json_file_buff);
  }

  /* Close json output file */
  if (WriteResultToJsonFile) {
    CloseJsonFile();
  }

  pics_request_locked = false;

  return(0);
}


/*
 * Add bacnet client point discovery.
 */
int add_bacnet_client_point_disc(uint32_t request_id, BACNET_ADDRESS *dest, bacnet_client_cmd_opts *opts)
{
  llist_point_disc_cb *ptr;

  ptr = malloc(sizeof(llist_point_disc_cb));
  if (!ptr) {
    printf("Error allocating memory for llist_point_disc_cb!\n");
    return(1);
  }

  memset(ptr, 0, sizeof(llist_point_disc_cb));
  ptr->request_id = request_id;
  ptr->request_object_id = bc_point_disc_request_ctr++;
  ptr->state = POINT_DISC_STATE_GET_SIZE_SENT;
  ptr->timestamp = time(NULL);
  ptr->data.timeout = opts->timeout;
  memcpy(&ptr->data.dest, dest, sizeof(BACNET_ADDRESS));
  ptr->data.device_instance = opts->device_instance;
  ptr->data.dnet = opts->dnet;
  if (strlen(opts->mac)) {
    strcpy(ptr->data.mac, opts->mac);
  }

  if (strlen(opts->dadr)) {
    strcpy(ptr->data.dadr, opts->dadr);
  }

  if (opts->req_tokens_len > 0) {
    ptr->data.req_tokens_len = opts->req_tokens_len;
    memcpy((char *)&ptr->data.req_tokens[0], (char *)&opts->req_tokens[0],
      sizeof(request_token_cb) * opts->req_tokens_len);
  }

  ptr->next = NULL;

  if (!bc_point_disc_head) {
    bc_point_disc_head = bc_point_disc_tail = ptr;
  } else {
    bc_point_disc_tail->next = ptr;
    bc_point_disc_tail = ptr;
  }

  printf("- Added point discovery request: %d\n", ptr->request_id);

  return(0);
}


/*
 * Process Bacnet client point discovery command.
 */
int process_bacnet_client_point_discovery_command(bacnet_client_cmd_opts *opts)
{
  uint8_t Rx_Buf[MAX_MPDU] = { 0 };
  BACNET_ADDRESS src = { 0 };
  BACNET_MAC_ADDRESS mac = { 0 };
  BACNET_MAC_ADDRESS dadr = { 0 };
  BACNET_ADDRESS dest = { 0 };
  int dnet = -1;
  bool specific_address = false;
  unsigned timeout = 100;
  uint16_t pdu_len = 0;
  uint32_t request_invoke_id;

  if (strlen(opts->mac)) {
    if (address_mac_from_ascii(&mac, opts->mac)) {
      specific_address = true;
    }
  }

  if (strlen(opts->dadr)) {
    if (address_mac_from_ascii(&dadr, opts->dadr)) {
      specific_address = true;
    }
  }

  if (opts->dnet >= 0) {
    dnet = opts->dnet;
    if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
      specific_address = true;
    }
  }

  if (specific_address) {
    if (dadr.len && mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      memcpy(&dest.adr[0], &dadr.adr[0], dadr.len);
      dest.len = dadr.len;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = BACNET_BROADCAST_NETWORK;
      }
    } else if (mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      dest.len = 0;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = 0;
      }
    } else {
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = BACNET_BROADCAST_NETWORK;
      }
      dest.mac_len = 0;
      dest.len = 0;
    }

    address_add(opts->device_instance, MAX_APDU, &dest);
  }

  /* Get number of points */
  request_invoke_id = Send_Read_Property_Request(opts->device_instance,
    OBJECT_DEVICE, opts->device_instance, PROP_OBJECT_LIST, 0);
  printf("point discovery request_invoke_id: %d\n", request_invoke_id);

  if (opts->timeout > 0) {
    timeout = opts->timeout;
  } else {
    timeout = opts->timeout = BACNET_CLIENT_POINT_DISC_TIMEOUT;
  }

  add_bacnet_client_point_disc(request_invoke_id, &dest, opts);

  pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, timeout);
  if (pdu_len) {
    npdu_handler(&src, &Rx_Buf[0], pdu_len);
  }

  return(0);
}


/*
 * Get point desc points.
 */
int get_bacnet_client_point_disc_points(llist_point_disc_cb *pd_ptr)
{
  uint8_t Rx_Buf[MAX_MPDU] = { 0 };
  BACNET_ADDRESS src = { 0 };
  uint16_t pdu_len;
  uint32_t request_invoke_id;

  request_invoke_id = Send_Read_Property_Request(pd_ptr->data.device_instance,
    OBJECT_DEVICE, pd_ptr->data.device_instance, PROP_OBJECT_LIST, BACNET_ARRAY_ALL);
  printf("point discovery request_invoke_id: %d\n", request_invoke_id);
  pd_ptr->request_id = request_invoke_id;
  pd_ptr->state = POINT_DISC_STATE_GET_POINTS_SENT;

  pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, pd_ptr->data.timeout);
  if (pdu_len) {
    npdu_handler(&src, &Rx_Buf[0], pdu_len);
  }

  return(0);
}


/*
 * Add bacnet client points info.
 */
llist_points_info_cb *add_bacnet_client_points_info(BACNET_ADDRESS *dest, bacnet_client_cmd_opts *opts)
{
  llist_points_info_cb *ptr;

  ptr = malloc(sizeof(llist_points_info_cb));
  if (!ptr) {
    printf("Error allocating memory for llist_points_info_cb!\n");
    return(NULL);
  }

  memset(ptr, 0, sizeof(llist_points_info_cb));
  ptr->request_object_id = bc_points_info_request_ctr++;
  ptr->timestamp = time(NULL);
  ptr->data.timeout = opts->timeout;
  memcpy(&ptr->data.dest, dest, sizeof(BACNET_ADDRESS));
  ptr->data.device_instance = opts->device_instance;
  ptr->data.dnet = opts->dnet;
  if (strlen(opts->mac)) {
    strcpy(ptr->data.mac, opts->mac);
  }

  if (strlen(opts->dadr)) {
    strcpy(ptr->data.dadr, opts->dadr);
  }

  if (opts->req_tokens_len > 0) {
    ptr->data.req_tokens_len = opts->req_tokens_len;     
    memcpy((char *)&ptr->data.req_tokens[0], (char *)&opts->req_tokens[0],
      sizeof(request_token_cb) * opts->req_tokens_len);
  }

  if (opts->points_len > 0) {
    ptr->data.points_len = opts->points_len;     
    ptr->data.points = malloc(sizeof(point_entry_cb) * ptr->data.points_len);
    memcpy((char *)&ptr->data.points[0], (char *)&opts->points[0],
      sizeof(point_entry_cb) * opts->points_len);
  }

  if (!bc_points_info_head) {
    bc_points_info_head = bc_points_info_tail = ptr;
  } else {
    bc_points_info_tail->next = ptr;
    bc_points_info_tail = ptr;
  }

  ptr->next = NULL;

  printf("- Added points info request: %d\n", ptr->request_object_id);

  return(ptr);
}


/*
 * Process Bacnet client points info command.
 */
int process_bacnet_client_points_info_command(bacnet_client_cmd_opts *opts)
{
  uint8_t Rx_Buf[MAX_MPDU] = { 0 };
  BACNET_ADDRESS src = { 0 };
  BACNET_MAC_ADDRESS mac = { 0 };
  BACNET_MAC_ADDRESS dadr = { 0 };
  BACNET_ADDRESS dest = { 0 };
  llist_points_info_cb *lpi_ptr;
  point_entry_cb *point_cb;
  int i, rc, dnet = -1;
  bool specific_address = false;
  unsigned timeout = 100;
  uint16_t pdu_len = 0;
  uint32_t request_invoke_id;
  char *ptr, *s_ptr, obj_name[MAX_JSON_VALUE_LENGTH] = {0};

  if (strlen(opts->mac)) {
    if (address_mac_from_ascii(&mac, opts->mac)) {
      specific_address = true;
    }
  }

  if (strlen(opts->dadr)) {
    if (address_mac_from_ascii(&dadr, opts->dadr)) {
      specific_address = true;
    }
  }

  if (opts->dnet >= 0) {
    dnet = opts->dnet;
    if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
      specific_address = true;
    }
  }

  if (specific_address) {
    if (dadr.len && mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      memcpy(&dest.adr[0], &dadr.adr[0], dadr.len);
      dest.len = dadr.len;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = BACNET_BROADCAST_NETWORK;
      }
    } else if (mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      dest.len = 0;
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = 0;
      }
    } else {
      if ((dnet >= 0) && (dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = dnet;
      } else {
        dest.net = BACNET_BROADCAST_NETWORK;
      }
      dest.mac_len = 0;
      dest.len = 0;
    }

    address_add(opts->device_instance, MAX_APDU, &dest);
  }

  for (i = 0; i < opts->points_len; i++) {
    point_cb = &opts->points[i];
    strcpy(obj_name, point_cb->obj_name);
    if (mqtt_debug) {
      printf("- [%d] Get point name of %s\n", i, obj_name);
    }
    s_ptr = &obj_name[0];
    ptr = strsep(&s_ptr, "-");
    point_cb->instance = atoi(s_ptr);
    if (!get_object_type_id(ptr, &point_cb->object_type)) {
      if (mqtt_debug) {
        printf("-- Unsupported object name: [%s]\n", ptr);
      }

      point_cb->error = true;
      strcpy(point_cb->err_msg, "Unsupported object type");
      continue;
    }

    if (point_cb->instance < 1) {
      if (mqtt_debug) {
        printf("-- Invalid object instance: [%d]\n", point_cb->instance);
      }

      point_cb->error = true;
      strcpy(point_cb->err_msg, "Invalid object instance");
      continue;
    }
  }

  if (!opts->timeout) {
    opts->timeout = BACNET_CLIENT_POINT_DISC_TIMEOUT;
  }

  lpi_ptr = add_bacnet_client_points_info(&dest, opts);

  for (i = 0; i < lpi_ptr->data.points_len; i++) {
    point_cb = &lpi_ptr->data.points[i];
    if (point_cb->error) {
      continue;
    }

    if (mqtt_debug) {
      printf("- Sending points info read property object name request for device: %d , object_type: %d , object_instance: %d\n",
       opts->device_instance, point_cb->object_type, point_cb->instance);
    }

    request_invoke_id = Send_Read_Property_Request(opts->device_instance,
      point_cb->object_type, point_cb->instance, PROP_OBJECT_NAME, BACNET_ARRAY_ALL);
    if (mqtt_debug) {
      printf("- points info request_invoke_id: %d\n", request_invoke_id);
    }

    point_cb->request_id = request_invoke_id;
    point_cb->sent = true;
  }

  return(0);
}


/* Lock MQTT mutex */
int mqtt_lock(void)
{
  int rc;

  rc = pthread_mutex_lock(&mqtt_mutex);

  return(rc);
}


/* Unlock MQTT mutex */
int mqtt_unlock(void)
{
  int rc;

  rc = pthread_mutex_unlock(&mqtt_mutex);

  return(rc);
}


/* Lock MQTT message queue mutex */
int mqtt_msg_queue_lock(void)
{
  int rc;

  rc = pthread_mutex_lock(&mqtt_msg_queue_mutex);

  return(rc);
}


/* Unlock MQTT message queue mutex */
int mqtt_msg_queue_unlock(void)
{
  int rc;

  rc = pthread_mutex_unlock(&mqtt_msg_queue_mutex);

  return(rc);
}


/*
 * Pop and process MQTT message.
 */
int mqtt_msg_pop_and_process(void)
{
  mqtt_msg_cb *mqtt_msg = NULL;
  json_object *json_root = NULL;
  json_object *json_field;
  char prop_value[MAX_JSON_MULTI_VALUE_LENGTH] = {0};
  char uuid_value[MAX_JSON_VALUE_LENGTH] = {0};
  char *ptr;
  int address;

  mqtt_msg = mqtt_msg_pop();
  if (!mqtt_msg) {
    goto EXIT;
  }

  printf("\n** Processing MQTT Message\n");
  if (mqtt_debug) {
    dump_topic_tokens(mqtt_msg->topic_tokens_length, mqtt_msg->topic_tokens);
  }

  if (!strcmp(mqtt_msg->topic_tokens[1], "cmd")) {
    process_bacnet_client_command(mqtt_msg->topic_tokens, mqtt_msg->topic_tokens_length, &mqtt_msg->cmd_opts);
  } else {
    address = strtol(mqtt_msg->topic_tokens[2], &ptr, 10);
    if (*ptr != '\0' || address < 0) {
      if (mqtt_debug) {
        printf("MQTT Invalid Topic Address: [%s]\n", mqtt_msg->topic_tokens[2]);
      }

      goto EXIT;
    }

    json_root = json_tokener_parse(mqtt_msg->topic_value);
    if (json_root == NULL) {
      if (mqtt_debug) {
        printf("Failed to parse topic value. Must be a JSON formatted string!\n");
      }

      goto EXIT;
    }

    json_field = json_object_object_get(json_root, "value");
    if (json_field == NULL) {
      if (mqtt_debug) {
        printf("Value not found in JSON string!\n");
      }

      goto EXIT;
    }

    strncpy(prop_value, json_object_get_string(json_field), sizeof(prop_value) - 1);

    if (!yaml_config_mqtt_disable_persistence() && (!strcasecmp(mqtt_msg->topic_tokens[3], "name") ||
      !strcasecmp(mqtt_msg->topic_tokens[3], "pv") ||
      !strcasecmp(mqtt_msg->topic_tokens[3], "pri"))) {
      if (!strcasecmp(mqtt_msg->topic_tokens[1], "ai")) {
        set_ai_property_persistent_value(address, mqtt_msg->topic_tokens, prop_value);
      } else if (!strcasecmp(mqtt_msg->topic_tokens[1], "ao")) {
        set_ao_property_persistent_value(address, mqtt_msg->topic_tokens, prop_value, json_field);
      } else if (!strcasecmp(mqtt_msg->topic_tokens[1], "av")) {
        set_av_property_persistent_value(address, mqtt_msg->topic_tokens, prop_value, json_field);
      } else if (!strcasecmp(mqtt_msg->topic_tokens[1], "bi")) {
        set_bi_property_persistent_value(address, mqtt_msg->topic_tokens, prop_value);
      } else if (!strcasecmp(mqtt_msg->topic_tokens[1], "bo")) {
        set_bo_property_persistent_value(address, mqtt_msg->topic_tokens, prop_value, json_field);
      } else if (!strcasecmp(mqtt_msg->topic_tokens[1], "bv")) {
        set_bv_property_persistent_value(address, mqtt_msg->topic_tokens, prop_value, json_field);
      } else {
        if (mqtt_debug) {
          printf("MQTT Unknown persistent Topic Object: [%s]\n", mqtt_msg->topic_tokens[1]);
        }
      }

      goto EXIT;
    }

    if (strcasecmp(mqtt_msg->topic_tokens[3], "write")) {
      if (mqtt_debug) {
        printf("MQTT Invalid Topic Command: [%s]\n", mqtt_msg->topic_tokens[3]);
      }

      goto EXIT;
    }

    json_field = json_object_object_get(json_root, "uuid");
    if (json_field == NULL) {
      if (mqtt_debug) {
        printf("UUID not found in JSON string!\n");
      }

      goto EXIT;
    }

    strncpy(uuid_value, json_object_get_string(json_field), sizeof(uuid_value) - 1);
    if (mqtt_debug) {
      printf("Topic Value/Properties:\n");
      printf("- value: [%s]\n", prop_value);
      printf("- uuid : [%s]\n", uuid_value);
    }

    if (!strcasecmp(mqtt_msg->topic_tokens[1], "ai")) {
      process_ai_write(address, mqtt_msg->topic_tokens, prop_value, uuid_value);
    } else if (!strcasecmp(mqtt_msg->topic_tokens[1], "ao")) {
      process_ao_write(address, mqtt_msg->topic_tokens, prop_value, uuid_value);
    } else if (!strcasecmp(mqtt_msg->topic_tokens[1], "av")) {
      process_av_write(address, mqtt_msg->topic_tokens, prop_value, uuid_value);
    } else if (!strcasecmp(mqtt_msg->topic_tokens[1], "bi")) {
      process_bi_write(address, mqtt_msg->topic_tokens, prop_value, uuid_value);
    } else if (!strcasecmp(mqtt_msg->topic_tokens[1], "bo")) {
      process_bo_write(address, mqtt_msg->topic_tokens, prop_value, uuid_value);
    } else if (!strcasecmp(mqtt_msg->topic_tokens[1], "bv")) {
      process_bv_write(address, mqtt_msg->topic_tokens, prop_value, uuid_value);
    } else {
      if (mqtt_debug) {
        printf("MQTT Unknown Topic Object: [%s]\n", mqtt_msg->topic_tokens[1]);
      }

      goto EXIT;
    }
  }

  EXIT:

  if (json_root) {
    json_object_put(json_root);
  }

  if (mqtt_msg) {
    if (mqtt_msg->cmd_opts.rpm_objects) {
      free(mqtt_msg->cmd_opts.rpm_objects);
    }

    if (mqtt_msg->cmd_opts.points) {
      free(mqtt_msg->cmd_opts.points);
    }

    free(mqtt_msg);
  }

  return(0);
}


/*
 * Restore persistent values.
 */
int restore_persistent_values(void* context)
{
  MQTTAsync client = (MQTTAsync)context;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  int rc;
  const char *topics[] = {
    "bacnet/ai/+/name",
    "bacnet/ao/+/name",
    "bacnet/av/+/name",
    "bacnet/bi/+/name",
    "bacnet/bo/+/name",
    "bacnet/bv/+/name",
    "bacnet/ai/+/pv",
    "bacnet/ao/+/pv",
    "bacnet/av/+/pv",
    "bacnet/bi/+/pv",
    "bacnet/bo/+/pv",
    "bacnet/bv/+/pv",
    "bacnet/ai/+/pri",
    "bacnet/ao/+/pri",
    "bacnet/av/+/pri",
    "bacnet/bi/+/pri",
    "bacnet/bo/+/pri",
    "bacnet/bv/+/pri",
    NULL
  };

  if (mqtt_debug) {
    printf("Initializing object property values from persistent store.\n");
  }

  /* subscribe topics */
  for (int i = 0; topics[i] != NULL; i++) {
    if (mqtt_debug) {
      printf("- Subscribing persistent topic[%d] = [%s]\n", i, topics[i]);
    }

    opts.onSuccess = mqtt_on_subscribe;
    opts.onFailure = mqtt_on_subscribe_failure;
    opts.context = client;
    rc = MQTTAsync_subscribe(mqtt_client, topics[i], 0, &opts);
    if (rc != MQTTASYNC_SUCCESS) {
      if (mqtt_debug) {
        printf("- WARNING: Failed to subscribe: %s\n", MQTTAsync_strerror(rc));
      }
    }
  }

  /* fetch last values asynchronously */

  /* unsubscribe topics */
  for (int i = 0; topics[i] != NULL; i++) {
    if (mqtt_debug) {
      printf("- Unsubscribing persistent topic[%d] = [%s]\n", i, topics[i]);
    }

    rc = MQTTAsync_unsubscribe(mqtt_client, topics[i], NULL);
    if (rc != MQTTASYNC_SUCCESS) {
      if (mqtt_debug) {
        printf("- WARNING: Failed to unsubscribe: %s\n", MQTTAsync_strerror(rc));
      }
    }
  }

  restore_persistent_done = true;

  return(0);
}

/*
 * Check if restore persistent values is done.
 */
int is_restore_persistent_done(void)
{
  return(restore_persistent_done);
}

