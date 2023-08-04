/*
 * MQTT client module
 */
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <ctype.h>

#include "bacnet/indtext.h"
#include "bacnet/bacenum.h"
#include "bacnet/basic/object/device.h"
#include "bacnet/basic/binding/address.h"
#include "bacnet/npdu.h"
#include "bacnet/apdu.h"
#include "bacnet/datalink/datalink.h"
#include "bacnet/bactext.h"

#include "bacnet/basic/services.h"

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
  uint32_t object_instance, float value, unsigned priority, char *uuid);
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
int process_ao_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int process_av_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int process_bi_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int process_bo_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int process_bv_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
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
int mqtt_subscribe_to_bacnet_client_topics(void *context);
int extract_json_fields_to_cmd_opts(json_object *json_root, bacnet_client_cmd_opts *cmd_opts);
int process_bacnet_client_whois_command(bacnet_client_cmd_opts *opts);
int mqtt_publish_command_result(int object_type, int object_instance, int property_id, int priority,
  int vtype, void *vptr, int topic_id, json_key_value_pair *kv_pairs, int kvps_length);
bool bbmd_address_match_self(BACNET_IP_ADDRESS *addr);
int is_address_us(bacnet_client_cmd_opts *opts);
int is_command_for_us(bacnet_client_cmd_opts *opts);
char *get_object_type_str(int object_type);
char *get_object_property_str(int object_property);
int encode_read_value_result(BACNET_READ_PROPERTY_DATA *data, llist_obj_data *obj_data, char *buf, int buf_len);
int encode_write_value_result(llist_obj_data *data, char *buf, int buf_len);
int publish_bacnet_client_read_value_result(BACNET_READ_PROPERTY_DATA *data, llist_obj_data *obj_data);
int publish_bacnet_client_write_value_result(llist_obj_data *data);
int publish_bacnet_client_whois_result(llist_whois_cb *cb);
int publish_whois_device(char *topic, char *topic_value);
int publish_bacnet_client_whois_result_per_device(llist_whois_cb *cb);
void set_kvps_for_reply(json_key_value_pair *kvps, int kvps_len, int *kvps_outlen, bacnet_client_cmd_opts *opts);
int process_local_read_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_read_value_command(bacnet_client_cmd_opts *opts);
void init_bacnet_client_service_handlers(void);
int process_local_write_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_write_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_command(char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], int n_topic_tokens, bacnet_client_cmd_opts *opts);

void encode_array_value_result(BACNET_READ_PROPERTY_DATA *data, llist_obj_data *obj_data, char *buf, int buf_len);
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

void mqtt_on_send_success(void* context, MQTTAsync_successData* response);
void mqtt_on_send_failure(void* context, MQTTAsync_failureData* response);
void mqtt_on_connect(void* context, MQTTAsync_successData* response);
void mqtt_on_connect_failure(void* context, MQTTAsync_failureData* response);
void mqtt_on_disconnect(void* context, MQTTAsync_successData* response);
void mqtt_on_disconnect_failure(void* context, MQTTAsync_failureData* response);
void mqtt_on_subscribe(void* context, MQTTAsync_successData* response);
void mqtt_on_subscribe_failure(void* context, MQTTAsync_failureData* response);
int mqtt_publish_command_error(char *err_msg, bacnet_client_cmd_opts *opts, int topic_id);

int iam_decode_service_request(uint8_t *apdu,
  uint32_t *pDevice_id,
  unsigned *pMax_apdu,
  int *pSegmentation,
  uint16_t *pVendor_id);
bool bacnet_address_same(BACNET_ADDRESS *dest, BACNET_ADDRESS *src);

int is_reqquest_token_present(char *token);
void init_request_tokens(void);

int extract_char_string(char *buf, int buf_len, BACNET_APPLICATION_DATA_VALUE *value);


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

static int req_tokens_len = 0;
static char req_tokens[MAX_JSON_KEY_VALUE_PAIR][MAX_JSON_KEY_LENGTH] = {0};

/* crude request list locking mechanism */
static int request_list_locked = false;


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
          Analog_Output_Priority_Array_Set(index, f, i, uuid);
        }
      }
    } else {
      Analog_Output_Priority_Array_Set(index, f, pri_idx, uuid);
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
 * Process Binary Value (AV) write.
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

    case OBJECT_DEVICE:
      str = "device";
      break;

    case OBJECT_MULTI_STATE_INPUT:
      str = "msi";
      break;

    case OBJECT_MULTI_STATE_OUTPUT:
      str = "mso";
      break;

    case OBJECT_MULTI_STATE_VALUE:
      str = "msv";
      break;
  }

  return(str);
}


char *get_object_property_str(int object_property)
{
  char *str = NULL;

  switch(object_property) {
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
        case PROP_PRESENT_VALUE:
          sprintf(tmp, "%.6f", *((float*)&value.type));
          break;

        case PROP_OBJECT_NAME:
          object_value.object_type = data->object_type;
          object_value.object_instance = data->object_instance;
          object_value.object_property = data->object_property;
          object_value.array_index = data->array_index;
          object_value.value = &value;
          bacapp_snprintf_value(tmp, sizeof(tmp) - 1, &object_value);
          break;

        case PROP_PRIORITY_ARRAY:
          encode_array_value_result(data, obj_data, tmp, sizeof(tmp) - 1);
          break;

        default:
          printf("Unsupported object_type: %d\n", data->object_property);
          return(1);
      }

      break;

    case OBJECT_BINARY_INPUT:
    case OBJECT_BINARY_OUTPUT:
    case OBJECT_BINARY_VALUE:
      switch(data->object_property) {
        case PROP_PRESENT_VALUE:
          sprintf(tmp, "%d", *((int*)&value.type));
          break;

        case PROP_OBJECT_NAME:
          object_value.object_type = data->object_type;
          object_value.object_instance = data->object_instance;
          object_value.object_property = data->object_property;
          object_value.array_index = data->array_index;
          object_value.value = &value;
          bacapp_snprintf_value(tmp, sizeof(tmp) - 1, &object_value);
          break;

        case PROP_PRIORITY_ARRAY:
          encode_array_value_result(data, obj_data, tmp, sizeof(tmp) - 1);
          break;

        default:
          return(1);
      }

      break;

    case OBJECT_DEVICE:
      switch(data->object_property) {
        case PROP_OBJECT_NAME:
          object_value.object_type = data->object_type;
          object_value.object_instance = data->object_instance;
          object_value.object_property = data->object_property;
          object_value.array_index = data->array_index;
          object_value.value = &value;
          bacapp_snprintf_value(tmp, sizeof(tmp) - 1, &object_value);
          break;

        case PROP_OBJECT_IDENTIFIER:
          sprintf(tmp, "%lu", (unsigned long)value.type.Object_Id.instance);
          break;

        case PROP_SYSTEM_STATUS:
          device_status_to_str(*((unsigned int*)&value.type), tmp, sizeof(tmp) - 1);
          break;

        case PROP_VENDOR_NAME:
          object_value.object_type = data->object_type;
          object_value.object_instance = data->object_instance;
          object_value.object_property = data->object_property;
          object_value.array_index = data->array_index;
          object_value.value = &value;
          bacapp_snprintf_value(tmp, sizeof(tmp) - 1, &object_value);
          break;

        case PROP_VENDOR_IDENTIFIER:
          sprintf(tmp, "%d", *((int*)&value.type));
          break;

        case PROP_OBJECT_LIST:
          encode_object_list_value_result(data, obj_data, tmp, sizeof(tmp) - 1);
          break;

        default:
          return(1);
      }

      break;

    case OBJECT_MULTI_STATE_INPUT:
    case OBJECT_MULTI_STATE_OUTPUT:
    case OBJECT_MULTI_STATE_VALUE:
      switch(data->object_property) {
        case PROP_PRESENT_VALUE:
          sprintf(tmp, "%d", *((unsigned int*)&value.type));
          break;

        case PROP_OBJECT_NAME:
          object_value.object_type = data->object_type;
          object_value.object_instance = data->object_instance;
          object_value.object_property = data->object_property;
          object_value.array_index = data->array_index;
          object_value.value = &value;
          bacapp_snprintf_value(tmp, sizeof(tmp) - 1, &object_value);
          break;

        default:
          printf("Unsupported object_type: %d\n", data->object_property);
          return(1);
      }

      break;

    default:
      return(1);
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
    printf("- read value result topic value: %s\n", topic_value);
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
 * Bacnet client read value handler.
 */
static void bacnet_client_read_value_handler(uint8_t *service_request,
  uint16_t service_len,
  BACNET_ADDRESS *src,
  BACNET_CONFIRMED_SERVICE_ACK_DATA *service_data)
{
  BACNET_READ_PROPERTY_DATA data;
  llist_obj_data obj_data = {0};
  int len;

  printf("- bacnet_client_read_value_handler() => %d\n", getpid());
  printf("-- service_data->invoke_id: %d\n", service_data->invoke_id);

  if (get_bacnet_client_request_obj_data(service_data->invoke_id, &obj_data)) {
    printf("-- Request with pending reply found!\n");
    del_bacnet_client_request(service_data->invoke_id);
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

  publish_bacnet_client_write_value_result(&obj_data);
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
    }
  } else {
    fprintf(stderr, ", but unable to decode it.\n");
  }
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


/*
 * Initialize bacnet client service handlers.
 */
void init_bacnet_client_service_handlers(void)
{
  apdu_set_confirmed_ack_handler(
    SERVICE_CONFIRMED_READ_PROPERTY, bacnet_client_read_value_handler);
  apdu_set_confirmed_simple_ack_handler(
    SERVICE_CONFIRMED_WRITE_PROPERTY, bacnet_client_write_value_handler);
  apdu_set_unconfirmed_handler(SERVICE_UNCONFIRMED_I_AM, bacnet_client_whois_handler);
  apdu_set_error_handler(SERVICE_CONFIRMED_READ_PROPERTY, bacnet_client_error_handler);
  apdu_set_error_handler(SERVICE_CONFIRMED_WRITE_PROPERTY, bacnet_client_error_handler);
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

  if (!mqtt_create_topic(opts->object_type, opts->object_instance, opts->property, topic, sizeof(topic),
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

  switch (opts->object_type) {
    case OBJECT_ANALOG_INPUT:
    case OBJECT_ANALOG_OUTPUT:
    case OBJECT_ANALOG_VALUE:
    case OBJECT_BINARY_INPUT:
    case OBJECT_BINARY_OUTPUT:
    case OBJECT_BINARY_VALUE:
      if (opts->property != PROP_OBJECT_NAME && opts->property != PROP_PRESENT_VALUE &&
        opts->property != PROP_PRIORITY_ARRAY) {
        sprintf(err_msg, "Unsupported property: %d of object_type %d", opts->property, opts->object_type);
        if (mqtt_debug) {
          printf("%s\n", err_msg);
        }

        mqtt_publish_command_error(err_msg, opts, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
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

        mqtt_publish_command_error(err_msg, opts, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
        return(1);
      }

      break;

    case OBJECT_DEVICE:
      if (opts->property != PROP_OBJECT_NAME && opts->property != PROP_OBJECT_IDENTIFIER &&
        opts->property != PROP_SYSTEM_STATUS && opts->property != PROP_VENDOR_NAME &&
        opts->property != PROP_VENDOR_IDENTIFIER && opts->property != PROP_OBJECT_LIST) {
        sprintf(err_msg, "Unsupported property: %d of object_type %d", opts->property, opts->object_type);
        if (mqtt_debug) {
          printf("%s\n", err_msg);
        }

        mqtt_publish_command_error(err_msg, opts, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
        return(1);
      }

      break;

    default:
      sprintf(err_msg, "Unknown read value object_type: %d", opts->object_type);
      if (mqtt_debug) {
        printf("%s\n", err_msg);
      }

      mqtt_publish_command_error(err_msg, opts, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
      return(1);
  }

  if (is_command_for_us(opts)) {
    printf("*** Command for us\n");
    process_local_read_value_command(opts);
  } else {
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
          bacapp_parse_application_data(4, str, value);
          break;

        case PROP_OBJECT_NAME:
          bacapp_parse_application_data(7, str, value);
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
          bacapp_parse_application_data(9, str, value);
          break;

        case PROP_OBJECT_NAME:
          bacapp_parse_application_data(7, str, value);
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
          bacapp_parse_application_data(2, str, value);
          break;

        case PROP_OBJECT_NAME:
          bacapp_parse_application_data(7, str, value);
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

  printf("write request_invoke_id: %d\n", request_invoke_id);

  if (send_reply) {
    printf("- queuing request_invoke_id %d for reply\n", request_invoke_id);
    add_bacnet_client_request(request_invoke_id, obj_data);

    if (opts->timeout > 0) {
      timeout = opts->timeout;
    }

    pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, timeout);
    if (pdu_len) {
      npdu_handler(&src, &Rx_Buf[0], pdu_len);
    }

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
    case OBJECT_BINARY_OUTPUT:
    case OBJECT_ANALOG_VALUE:
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

  if (is_command_for_us(opts)) {
    printf("command for us\n");
    process_local_write_value_command(opts);
  } else {
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

        obj_data.device_instance = opts->device_instance;
        obj_data.object_type = opts->object_type;
        obj_data.object_instance = opts->object_instance;
        obj_data.object_property = PROP_PRIORITY_ARRAY;
        obj_data.priority = 0;
        obj_data.index = opts->prio_array[i].index;

        if ((i + 1) == opts->prio_array_len) {
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

          send_reply = true;
        }

        send_write_request(&obj_data, &value, opts, send_reply);
      }
    } else {
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
  }

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
  char *key;
  char value[MAX_CMD_STR_OPT_VALUE_LENGTH];
  int i;
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
      struct json_object_iterator it;
      struct json_object_iterator itEnd;
      struct json_object* obj;

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
  bacnet_client_cmd_opts cmd_opts = init_bacnet_client_cmd_opts;
  json_object *json_root = NULL;
  json_object *json_field;
  char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH];
  char topic_value[MAX_TOPIC_VALUE_LENGTH] = {0};
  char prop_value[MAX_JSON_VALUE_LENGTH] = {0};
  char uuid_value[MAX_JSON_VALUE_LENGTH] = {0};
  char *ptr;
  int n_topic_tokens;
  int address;

  memset(&cmd_opts.prio_array[0], 0, sizeof(cmd_opts.prio_array));
  memset(topic_value, 0, sizeof(topic_value));
  strncpy(topic_value, (char*)message->payload,
    (message->payloadlen > sizeof(topic_value)) ? sizeof(topic_value) -1 : message->payloadlen);

  if (mqtt_debug) {
     printf("MQTT message arrived: => %d\n", getpid());
     printf("- version: [%d]\n", message->struct_version);
     printf("- payloadLen: [%d]\n", message->payloadlen);
     printf("- topic  : [%s]\n", topic);
     printf("- value  : [%s]\n", topic_value);
  }

  memset(topic_tokens, 0, sizeof(topic_tokens));
  n_topic_tokens = tokenize_topic(topic, topic_tokens);
  if (n_topic_tokens == 0) {
    goto EXIT;
  }

  if (mqtt_debug) {
    dump_topic_tokens(n_topic_tokens, topic_tokens);
  }

  json_root = json_tokener_parse(topic_value);
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
    &cmd_opts)) {
    if (mqtt_debug) {
      printf("Error extracting command options!\n");
    }

    goto EXIT;
  }

  if (!strcmp(topic_tokens[1], "cmd")) {
    process_bacnet_client_command(topic_tokens, n_topic_tokens, &cmd_opts);
  } else {
    address = strtol(topic_tokens[2], &ptr, 10);
    if (*ptr != '\0' || address < 0) {
      if (mqtt_debug) {
        printf("MQTT Invalid Topic Address: [%s]\n", topic_tokens[2]);
      }

      goto EXIT;
    }

    if (strcasecmp(topic_tokens[3], "write")) {
      if (mqtt_debug) {
        printf("MQTT Invalid Topic Command: [%s]\n", topic_tokens[3]);
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

    if (!strcasecmp(topic_tokens[1], "ai")) {
      process_ai_write(address, topic_tokens, prop_value, uuid_value);
    } else if (!strcasecmp(topic_tokens[1], "ao")) {
      process_ao_write(address, topic_tokens, prop_value, uuid_value);
    } else if (!strcasecmp(topic_tokens[1], "av")) {
      process_av_write(address, topic_tokens, prop_value, uuid_value);
    } else if (!strcasecmp(topic_tokens[1], "bi")) {
      process_bi_write(address, topic_tokens, prop_value, uuid_value);
    } else if (!strcasecmp(topic_tokens[1], "bo")) {
      process_bo_write(address, topic_tokens, prop_value, uuid_value);
    } else if (!strcasecmp(topic_tokens[1], "bv")) {
      process_bv_write(address, topic_tokens, prop_value, uuid_value);
    } else {
      if (mqtt_debug) {
        printf("MQTT Unknown Topic Object: [%s]\n", topic_tokens[1]);
      }

      goto EXIT;
    }
  }
 
  EXIT:

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

  conn_opts.keepAliveInterval = 30;
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
  
  while (ptr != NULL) {
    if (ptr->data.invoke_id == invoke_id) {
      memcpy(buf, &ptr->data.obj_data, sizeof(llist_obj_data));
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

      free(tmp);
      continue;
    }

    prev = ptr;
    ptr = ptr->next;
  }
}


/*
 * Initialize mqtt client module.
 */
int mqtt_client_init(void)
{
  MQTTAsync_createOptions createOpts = MQTTAsync_createOptions_initializer;
  char mqtt_broker_endpoint[100];
  char buf[100];
  char *pEnv;
  int rc;

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

  if (mqtt_connect_to_broker()) {
    return(1);
  }

  init_bacnet_client_request_list();
  init_bacnet_client_whois_list();
  init_request_tokens();

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
 * Publish topic.
 */
int mqtt_publish_topic(int object_type, int object_instance, int property_id, int vtype, void *vptr, char *uuid_value)
{
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
  char topic[512];
  char topic_value[30000];
  char buf[2048];
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
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" , \"uuid\" : \"%s\" }", (char*)vptr,
        (uuid_value == NULL || !strlen(uuid_value)) ? "" : uuid_value);
      pubmsg.payload = topic_value;
      pubmsg.payloadlen = strlen(topic_value);
      break;

    case MQTT_TOPIC_VALUE_BACNET_STRING:
      characterstring_ansi_copy(buf, sizeof(buf), vptr);
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" , \"uuid\" : \"%s\" }", buf,
        (uuid_value == NULL || !strlen(uuid_value)) ? "" : uuid_value);
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
    }
  }
  
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

