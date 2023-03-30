/*
 * MQTT client module
 */
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "bacnet/indtext.h"
#include "bacnet/bacenum.h"
#include "bacnet/basic/object/device.h"
#include "bacnet/basic/binding/address.h"
#include "bacnet/npdu.h"
#include "bacnet/apdu.h"
#include "bacnet/datalink/datalink.h"

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
void set_kvps_for_reply(json_key_value_pair *kvps, int kvps_len, int *kvps_outlen, bacnet_client_cmd_opts *opts);
int process_local_read_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_read_value_command(bacnet_client_cmd_opts *opts);
void init_bacnet_client_service_handlers(void);
int process_local_write_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_write_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_command(char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], int n_topic_tokens, bacnet_client_cmd_opts *opts);

void init_bacnet_client_request_list(void);
void shutdown_bacnet_client_request_list(void);
int is_bacnet_client_request_present(uint8_t invoke_id);
llist_obj_data *get_bacnet_client_request_obj_data(uint8_t invoke_id, llist_obj_data *buf);
int add_bacnet_client_request(uint8_t invoke_id, llist_obj_data *obj_data);
int del_bacnet_client_request(uint8_t invoke_id);

void mqtt_on_send_success(void* context, MQTTAsync_successData* response);
void mqtt_on_send_failure(void* context, MQTTAsync_failureData* response);
void mqtt_on_connect(void* context, MQTTAsync_successData* response);
void mqtt_on_connect_failure(void* context, MQTTAsync_failureData* response);
void mqtt_on_disconnect(void* context, MQTTAsync_successData* response);
void mqtt_on_disconnect_failure(void* context, MQTTAsync_failureData* response);
void mqtt_on_subscribe(void* context, MQTTAsync_successData* response);
void mqtt_on_subscribe_failure(void* context, MQTTAsync_failureData* response);

/* globals */
static int mqtt_debug = false;
static char mqtt_broker_ip[51] = {0};
static int mqtt_broker_port = DEFAULT_MQTT_BROKER_PORT;
static char mqtt_client_id[124] = {0};
static MQTTAsync mqtt_client = NULL;
static int mqtt_client_connected = false;
static bacnet_client_cmd_opts init_bacnet_client_cmd_opts = {-1, BACNET_MAX_INSTANCE, -1, BACNET_ARRAY_ALL, 0, BACNET_MAX_INSTANCE, -1, {0}, {0}, {0}, -1, {0}, -1, -1};

static llist_cb *bc_request_list_head = NULL;
static llist_cb *bc_request_list_tail = NULL;

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
  uint16_t pdu_len;
  uint8_t Rx_Buf[MAX_MPDU] = { 0 };
  long dnet = -1;
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

  if (strlen(opts->daddr)) {
    if (address_mac_from_ascii(&adr, opts->daddr)) {
      global_broadcast = false;
    }
  }

  if (global_broadcast) {
    datalink_get_broadcast_address(&dest);
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

printf("- Sending whois to network ...\n");

  Send_WhoIs_To_Network(
    &dest, opts->device_instance_min, opts->device_instance_max);

  pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU,
    delay_milliseconds);
  /* process */
  if (pdu_len) {
    npdu_handler(&src, &Rx_Buf[0], pdu_len);
        
  }
printf("- Request sent!\n");
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
  char topic[512];
  char topic_value[1024];
  char buf[1024];
  char *first = "";
  BACNET_BINARY_PV b_val;
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
  char array_val[25][32];
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
    bacapp_snprintf_value(array_val[array_val_len], 25, &object_value);
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
 * Encode value payload for bacnet client read command.
 */
int encode_read_value_result(BACNET_READ_PROPERTY_DATA *data, llist_obj_data *obj_data, char *buf, int buf_len)
{
  BACNET_APPLICATION_DATA_VALUE value;
  char tmp[512] = {0};

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
          characterstring_ansi_copy(tmp, sizeof(tmp), (BACNET_CHARACTER_STRING*)&value.type);
          break;

        case PROP_PRIORITY_ARRAY:
          encode_array_value_result(data, obj_data, tmp, sizeof(tmp));
          break;

        default:
          printf("Unsupported object_type: %d\n", data->object_property);
          return(1);
      }

      if (data->object_property == PROP_PRIORITY_ARRAY && obj_data->index == BACNET_ARRAY_ALL) {
        snprintf(buf, buf_len, "{ \"value\" : %s , \"deviceInstance\" : \"%d\" ",
          tmp, obj_data->device_instance);
      } else {
        snprintf(buf, buf_len, "{ \"value\" : \"%s\" , \"deviceInstance\" : \"%d\" ",
          tmp, obj_data->device_instance);
      }

      if (obj_data->index >= 0) {
        snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"index\" : \"%d\" ",
        obj_data->index);
      }

      snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "}");
      break;

    case OBJECT_BINARY_INPUT:
    case OBJECT_BINARY_OUTPUT:
    case OBJECT_BINARY_VALUE:
      switch(data->object_property) {
        case PROP_PRESENT_VALUE:
          sprintf(tmp, "%d", *((int*)&value.type));
          break;

        case PROP_OBJECT_NAME:
          characterstring_ansi_copy(tmp, sizeof(tmp), (BACNET_CHARACTER_STRING*)&value.type);
          break;

        case PROP_PRIORITY_ARRAY:
          encode_array_value_result(data, obj_data, tmp, sizeof(tmp));
          break;

        default:
          return(1);
      }

      if (data->object_property == PROP_PRIORITY_ARRAY && obj_data->index == BACNET_ARRAY_ALL) {
        snprintf(buf, buf_len, "{ \"value\" : %s , \"deviceInstance\" : \"%d\" ",
          tmp, obj_data->device_instance);
      } else {
        snprintf(buf, buf_len, "{ \"value\" : \"%s\" , \"deviceInstance\" : \"%d\" ",
          tmp, obj_data->device_instance);
      }

      if (obj_data->index >= 0) {
        snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"index\" : \"%d\" ",
        obj_data->index);
      }

      snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "}");
      break;

    default:
      return(1);
  }

  return(0);
}


/*
 * Encode value payload for bacnet client write command.
 */
int encode_write_value_result(llist_obj_data *data, char *buf, int buf_len)
{
  switch(data->object_type) {
    case OBJECT_ANALOG_INPUT:
    case OBJECT_ANALOG_OUTPUT:
    case OBJECT_ANALOG_VALUE:
    case OBJECT_BINARY_INPUT:
    case OBJECT_BINARY_OUTPUT:
    case OBJECT_BINARY_VALUE:
      snprintf(buf, buf_len, "{ \"value\" : \"%s\" , \"deviceInstance\" : \"%d\" ",
        data->value,
        data->device_instance);
      if (data->priority) {
        snprintf(&buf[strlen(buf)], buf_len - strlen(buf), ", \"priority\" : \"%d\" ", data->priority);
      }
      snprintf(&buf[strlen(buf)], buf_len - strlen(buf), "}");
      break;

    default:
      return(1);
  }

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
  char topic_value[1024] = {0};
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

  encode_read_value_result(data, obj_data, topic_value, sizeof(topic_value));

  printf("- read value result topic: %s\n", topic);
  printf("- read value result topic value: %s\n", topic_value);

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
  char topic_value[1024] = {0};
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
 * Bacnet client read value handler.
 */
static void bacnet_client_read_value_handler(uint8_t *service_request,
  uint16_t service_len,
  BACNET_ADDRESS *src,
  BACNET_CONFIRMED_SERVICE_ACK_DATA *service_data)
{
  BACNET_READ_PROPERTY_DATA data;
  llist_obj_data obj_data;
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
    rp_ack_print_data(&data);
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
    // address_table_add(device_id, max_apdu, src);
  } else {
    fprintf(stderr, ", but unable to decode it.\n");
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
}


/*
 * Set Key-Value pairs for mqtty reply payload.
 */
void set_kvps_for_reply(json_key_value_pair *kvps, int kvps_len, int *kvps_outlen, bacnet_client_cmd_opts *opts)
{
  int i = 0;

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
 * Process local bacnet client read value command.
 */
int process_local_read_value_command(bacnet_client_cmd_opts *opts)
{
  BACNET_CHARACTER_STRING bs_val;
  BACNET_BINARY_PV b_array[BACNET_MAX_PRIORITY] = {0};
  json_key_value_pair kvps[MAX_JSON_KEY_VALUE_PAIR] = {0};
  float f_array[BACNET_MAX_PRIORITY] = {0};
  float f_val;
  int i_val;
  int kvps_length = 0;

  switch (opts->object_type) {
    case OBJECT_ANALOG_INPUT:
      if (Analog_Input_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          f_val = Analog_Input_Present_Value(opts->object_instance);
          mqtt_publish_command_result(OBJECT_ANALOG_INPUT, opts->object_instance, PROP_PRESENT_VALUE, -1,
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        case PROP_OBJECT_NAME:
          Analog_Input_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_ANALOG_INPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
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
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        case PROP_OBJECT_NAME:
          Analog_Output_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
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
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        case PROP_OBJECT_NAME:
          Analog_Value_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
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
            MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        case PROP_OBJECT_NAME:
          Binary_Input_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_BINARY_INPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
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
            MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        case PROP_OBJECT_NAME:
          Binary_Output_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
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
            MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        case PROP_OBJECT_NAME:
          Binary_Value_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC, NULL, 0);
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

    default:
      if (mqtt_debug) {
        printf("Unknown read value object_type: [%d]\n", opts->object_type);
      }
      return(1);
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
  BACNET_ADDRESS dest = { 0 };
  llist_obj_data obj_data = { 0 };
  bool specific_address = false;
  unsigned timeout = 100;
  uint16_t pdu_len = 0;
  int request_invoke_id;

  if (opts->device_instance < 0 || opts->object_type < 0 || opts->object_instance < 0 ||
    opts->property < 0) {
    if (mqtt_debug) {
      printf("Empty read value device_instance/object_type/object_instance/property: [%d/%d/%d/%d]\n",
        opts->device_instance, opts->object_type, opts->object_instance, opts->property);
    }

    return(1);
  }

  if (strlen(opts->mac)) {
    if (address_mac_from_ascii(&mac, opts->mac)) {
      specific_address = true;
    }
  }

  if (specific_address) {
    if (mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      dest.len = 0;
      if ((opts->dnet >= 0) && (opts->dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = opts->dnet;
      } else {
        dest.net = 0;
      }
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
        if (mqtt_debug) {
          printf("Unsupported object_type %d property: %d\n", opts->object_type, opts->property);
        }

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
        memcpy(&obj_data.value, opts->value, sizeof(opts->value));

        request_invoke_id = Send_Read_Property_Request(opts->device_instance,
          opts->object_type, opts->object_instance, opts->property, opts->index);
        printf("read request_invoke_id: %d\n", request_invoke_id);

        add_bacnet_client_request(request_invoke_id, &obj_data);

        if (opts->tag_flags & CMD_TAG_FLAG_SLOW_TEST) {
          usleep(1000000);
        }

        pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, timeout);
        if (pdu_len) {
          npdu_handler(&src, &Rx_Buf[0], pdu_len);
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
 * Process local bacnet client write value command.
 */
int process_local_write_value_command(bacnet_client_cmd_opts *opts)
{
  BACNET_CHARACTER_STRING bacnet_string;
  BACNET_BINARY_PV pv;
  int priority;
  int i_val;
  float f_val;
  char *endptr;

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
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;
        
        case PROP_OBJECT_NAME:
          characterstring_init_ansi(&bacnet_string, opts->value);
          Analog_Input_Set_Object_Name(opts->object_instance, &bacnet_string, NULL, true);
          mqtt_publish_command_result(OBJECT_ANALOG_INPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bacnet_string, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        default:
          return(1);
      }

      break;

    case OBJECT_ANALOG_OUTPUT:
      if (Analog_Output_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      priority = (opts->priority) ? opts->priority : BACNET_MAX_PRIORITY;
      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          f_val = strtof(opts->value, &endptr);
          Analog_Output_Present_Value_Set(opts->object_instance, f_val, priority, NULL, true);
          f_val = Analog_Output_Present_Value(opts->object_instance);
          mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_PRESENT_VALUE, opts->priority,
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        case PROP_OBJECT_NAME:
          characterstring_init_ansi(&bacnet_string, opts->value);
          Analog_Output_Set_Object_Name(opts->object_instance, &bacnet_string, NULL, true);
          mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bacnet_string, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        default:
          return(1);
      }

      break;

    case OBJECT_ANALOG_VALUE:
      if (Analog_Value_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      priority = (opts->priority) ? opts->priority : BACNET_MAX_PRIORITY;
      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          f_val = strtof(opts->value, &endptr);
          Analog_Value_Present_Value_Set(opts->object_instance, f_val, priority, NULL, true);
          f_val = Analog_Value_Present_Value(opts->object_instance);
          mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_PRESENT_VALUE, opts->priority,
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        case PROP_OBJECT_NAME:
          characterstring_init_ansi(&bacnet_string, opts->value);
          Analog_Value_Set_Object_Name(opts->object_instance, &bacnet_string, NULL, true);
          mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bacnet_string, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, NULL, 0);
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
          i_val = atoi(opts->value);
          pv = (i_val == 0) ? 0 : 1;
          Binary_Input_Present_Value_Set(opts->object_instance, pv, NULL, true);
          mqtt_publish_command_result(OBJECT_BINARY_INPUT, opts->object_instance, PROP_PRESENT_VALUE, -1,
            MQTT_TOPIC_VALUE_INTEGER, &pv, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        case PROP_OBJECT_NAME:
          characterstring_init_ansi(&bacnet_string, opts->value);
          Binary_Input_Set_Object_Name(opts->object_instance, &bacnet_string, NULL, true);
          mqtt_publish_command_result(OBJECT_BINARY_INPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bacnet_string, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        default:
          return(1);
      }

      break;

    case OBJECT_BINARY_OUTPUT:
      if (Binary_Output_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      priority = (opts->priority) ? opts->priority : BACNET_MAX_PRIORITY;
      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          i_val = atoi(opts->value);
          pv = (i_val == 0) ? 0 : 1;
          Binary_Output_Present_Value_Set(opts->object_instance, pv, priority, NULL, true);
          mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_PRESENT_VALUE, opts->priority,
            MQTT_TOPIC_VALUE_INTEGER, &pv, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        case PROP_OBJECT_NAME:
          characterstring_init_ansi(&bacnet_string, opts->value);
          Binary_Output_Set_Object_Name(opts->object_instance, &bacnet_string, NULL, true);
          mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bacnet_string, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        default:
          return(1);
      }

      break;

    case OBJECT_BINARY_VALUE:
      if (Binary_Value_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      priority = (opts->priority) ? opts->priority : BACNET_MAX_PRIORITY;
      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          i_val = atoi(opts->value);
          pv = (i_val == 0) ? 0 : 1;
          Binary_Value_Present_Value_Set(opts->object_instance, pv, priority, NULL, true);
          mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_PRESENT_VALUE, opts->priority,
            MQTT_TOPIC_VALUE_INTEGER, &pv, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, NULL, 0);
          break;

        case PROP_OBJECT_NAME:
          characterstring_init_ansi(&bacnet_string, opts->value);
          Binary_Value_Set_Object_Name(opts->object_instance, &bacnet_string, NULL, true);
          mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_OBJECT_NAME, -1,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bacnet_string, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC, NULL, 0);
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
 * Process Bacnet client write value command.
 */
int process_bacnet_client_write_value_command(bacnet_client_cmd_opts *opts)
{
  uint8_t Rx_Buf[MAX_MPDU] = { 0 };
  BACNET_ADDRESS src = { 0 };
  BACNET_APPLICATION_DATA_VALUE value = { 0 };
  BACNET_MAC_ADDRESS mac = { 0 };
  BACNET_ADDRESS dest = { 0 };
  llist_obj_data obj_data = { 0 };
  bool specific_address = false;
  unsigned timeout = 100;
  uint16_t pdu_len = 0;
  int request_invoke_id;

  if (opts->device_instance < 0 || opts->object_type < 0 || opts->object_instance < 0 ||
    opts->property < 0) {
    if (mqtt_debug) {
      printf("Empty write value device_instance/object_type/object_instance/property: [%d/%d/%d/%d]\n",
        opts->device_instance, opts->object_type, opts->object_instance, opts->property);
    }

    return(1);
  }

  if (strlen(opts->mac)) {
    if (address_mac_from_ascii(&mac, opts->mac)) {
      specific_address = true;
    }
  }

  if (specific_address) {
    if (mac.len) {
      memcpy(&dest.mac[0], &mac.adr[0], mac.len);
      dest.mac_len = mac.len;
      dest.len = 0;
      if ((opts->dnet >= 0) && (opts->dnet <= BACNET_BROADCAST_NETWORK)) {
        dest.net = opts->dnet;
      } else {
        dest.net = 0;
      }
    }

    address_add(opts->device_instance, MAX_APDU, &dest);
  }

  if (!strlen(opts->value)) {
    if (mqtt_debug) {
      printf("Invalid/empty write value object value\n");
    }

    return(1);
  }

  value.context_specific = false;

  switch (opts->object_type) {
    case OBJECT_ANALOG_INPUT:
    case OBJECT_ANALOG_OUTPUT:
    case OBJECT_ANALOG_VALUE:
      bacapp_parse_application_data(4, opts->value, &value);
      break;

    case OBJECT_BINARY_INPUT:
    case OBJECT_BINARY_OUTPUT:
    case OBJECT_BINARY_VALUE:
      bacapp_parse_application_data(9, opts->value, &value);
      break;

    default: 
      if (mqtt_debug) {
        printf("Unknown object type: %d\n", opts->object_type);
      }
      return(1);
  }

  switch (opts->object_type) {
    case OBJECT_ANALOG_INPUT:
    case OBJECT_ANALOG_OUTPUT:
    case OBJECT_ANALOG_VALUE:
    case OBJECT_BINARY_INPUT:
    case OBJECT_BINARY_OUTPUT:
    case OBJECT_BINARY_VALUE:
      if (opts->property != PROP_OBJECT_NAME && opts->property != PROP_PRESENT_VALUE) {
        if (mqtt_debug) {
          printf("Unsupported object_type %d property: %d\n", opts->object_type, opts->property);
        }

        return(1);
      }

      if (is_command_for_us(opts)) {
        printf("command for us\n");
        process_local_write_value_command(opts);
      } else {
        obj_data.device_instance = opts->device_instance;
        obj_data.object_type = opts->object_type;
        obj_data.object_instance = opts->object_instance;
        obj_data.object_property = opts->property;
        obj_data.priority = opts->priority;
        memcpy(&obj_data.value, opts->value, sizeof(opts->value));

        request_invoke_id = Send_Write_Property_Request(
          opts->device_instance, opts->object_type,
          opts->object_instance, opts->property,
          &value,
          obj_data.priority,
          opts->index);
        printf("write request_invoke_id: %d\n", request_invoke_id);

        add_bacnet_client_request(request_invoke_id, &obj_data);

        pdu_len = datalink_receive(&src, &Rx_Buf[0], MAX_MPDU, timeout);
        if (pdu_len) {
          npdu_handler(&src, &Rx_Buf[0], pdu_len);
        }
      }

      break;

    default:
      if (mqtt_debug) {
        printf("Unknown write value object_type: [%d]\n", opts->object_type);
      }
      return(1);
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
    "daddr",
    "tag",
    "value",
    "uuid",
    "tags",
    NULL
  };

  for (i = 0; keys[i] ;i++) {
    key = keys[i];
    json_field = json_object_object_get(json_root, key);
    if (!json_field) {
      continue;
    }

    strncpy(value, json_object_get_string(json_field), sizeof(value) - 1);
    if (!strcmp(key, "objectType")) {
      cmd_opts->object_type = (unsigned)strtol(value, NULL, 0);
    } else if (!strcmp(key, "objectInstance")) {
      cmd_opts->object_instance = strtol(value, NULL, 0);
    } else if (!strcmp(key, "property")) {
      cmd_opts->property = (unsigned)strtol(value, NULL, 0);
    } else if (!strcmp(key, "index")) {
      cmd_opts->index = atoi(value);
    } else if (!strcmp(key, "priority")) {
      cmd_opts->priority = atoi(value);
    } else if (!strcmp(key, "deviceInstance")) {
      cmd_opts->device_instance = strtol(value, NULL, 0);
    } else if (!strcmp(key, "dnet")) {
      cmd_opts->dnet = atoi(value);
    } else if (!strcmp(key, "mac")) {
      strncpy(cmd_opts->mac, value, sizeof(cmd_opts->mac) - 1);
    } else if (!strcmp(key, "daddr")) {
      strncpy(cmd_opts->daddr, value, sizeof(cmd_opts->daddr) - 1);
    } else if (!strcmp(key, "value")) {
      strncpy(cmd_opts->value, value, sizeof(cmd_opts->value) - 1);
    } else if (!strcmp(key, "uuid")) {
      strncpy(cmd_opts->uuid, value, sizeof(cmd_opts->uuid) - 1);
    } else if (!strcmp(key, "tags")) {
      if (!strcmp(value, "slow_test")) {
        cmd_opts->tag_flags |= CMD_TAG_FLAG_SLOW_TEST;
      }
    } else {
      if (mqtt_debug) {
        printf("Unsupported command option key: %s. Skipping.\n", key);
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
  char topic_value[MAX_TOPIC_VALUE_LENGTH];
  char prop_value[MAX_TOPIC_VALUE_LENGTH] = {0};
  char uuid_value[MAX_TOPIC_VALUE_LENGTH] = {0};
  char *ptr;
  int n_topic_tokens;
  int address;

  memset(topic_value, 0, sizeof(topic_value));
  strncpy(topic_value, (char*)message->payload,
    (message->payloadlen > sizeof(topic_value)) ? sizeof(topic_value) -1 : message->payloadlen);

  if (mqtt_debug) {
     printf("MQTT message arrived: => %d\n", getpid());
     printf("- version: [%d]\n", message->struct_version);
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
 * Sweep aged bacnet client requests.
 */
void sweep_bacnet_client_aged_requests(void)
{
  llist_cb *tmp, *prev = NULL;
  llist_cb *ptr = bc_request_list_head;
  time_t cur_tt = time(NULL);

  printf("- sweep_bacnet_client_aged_requests()\n");

  while (ptr != NULL) {
    if ((cur_tt - ptr->timestamp) >= BACNET_CLIENT_REQUEST_TTL) {
      printf("- Aged request found: %d\n", ptr->data.invoke_id);

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
      return(NULL);
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

    default:
      return(NULL);
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
  char topic_value[1024];
  char buf[1024];
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

