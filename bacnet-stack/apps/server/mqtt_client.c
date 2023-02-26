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

// #include "MQTTClient.h"
#include "MQTTAsync.h"
#include "json-c/json.h"
#include "mqtt_client.h"
#if defined(YAML_CONFIG)
#include "yaml_config.h"
#endif /* defined(YAML_CONFIG) */


/* forward decls */
extern bool Analog_Input_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid);
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
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid);
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

extern bool Analog_Value_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid);
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

extern bool Binary_Input_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid);
extern bool Binary_Input_Present_Value_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, char *uuid, int bacnet_client);
extern BACNET_BINARY_PV Binary_Input_Present_Value(uint32_t object_instance);
extern unsigned Binary_Input_Instance_To_Index(uint32_t object_instance);
extern bool Binary_Input_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name);

extern bool Binary_Output_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid);
extern bool Binary_Output_Present_Value_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid);
extern bool Binary_Output_Priority_Array_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid);
extern bool Binary_Output_Priority_Array_Set2(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority);
extern BACNET_BINARY_PV Binary_Output_Present_Value(uint32_t object_instance);
extern unsigned Binary_Output_Instance_To_Index(uint32_t object_instance);
extern bool Binary_Output_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name);

extern bool Binary_Value_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid);
extern bool Binary_Value_Present_Value_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid);
extern bool Binary_Value_Priority_Array_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid);
extern bool Binary_Value_Priority_Array_Set2(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority);
extern BACNET_BINARY_PV Binary_Value_Present_Value(uint32_t object_instance);
extern unsigned Binary_Value_Instance_To_Index(uint32_t object_instance);
extern bool Binary_Value_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name);

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
int process_bacnet_client_whois_command(void);
int mqtt_publish_command_result(int object_type, int object_instance, int property_id, int vtype, void *vptr, int topic_id);
bool bbmd_address_match_self(BACNET_IP_ADDRESS *addr);
int is_address_us(bacnet_client_cmd_opts *opts);
int is_command_for_us(bacnet_client_cmd_opts *opts);
char *get_object_type_str(int object_type);
char *get_object_property_str(int object_property);
int encode_read_value_result(BACNET_READ_PROPERTY_DATA *data, char *buf, int buf_len);
int publish_bacnet_client_read_value_result(BACNET_READ_PROPERTY_DATA *data);
void bacnet_client_read_value_handler(uint8_t *service_request,
  uint16_t service_len,
  BACNET_ADDRESS *src,
  BACNET_CONFIRMED_SERVICE_ACK_DATA *service_data);
int process_local_read_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_read_value_command(bacnet_client_cmd_opts *opts);
void init_bacnet_client_service_handlers(void);
int process_local_write_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_write_value_command(bacnet_client_cmd_opts *opts);
int process_bacnet_client_command(char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], int n_topic_tokens, bacnet_client_cmd_opts *opts);

void init_bacnet_client_request_list(void);
void shutdown_bacnet_client_request_list(void);
int is_bacnet_client_request_present(uint8_t invoke_id);
int add_bacnet_client_request(uint8_t invoke_id);
int del_bacnet_client_request(uint8_t invoke_id);

/* globals */
static int mqtt_debug = false;
static char mqtt_broker_ip[51] = {0};
static int mqtt_broker_port = DEFAULT_MQTT_BROKER_PORT;
static char mqtt_client_id[124] = {0};
static MQTTAsync mqtt_client = NULL;
static int mqtt_client_connected = false;
static bacnet_client_cmd_opts init_bacnet_client_cmd_opts = { -1, BACNET_MAX_INSTANCE, -1, BACNET_ARRAY_ALL, 0, BACNET_MAX_INSTANCE, 0, {0}, {0}, {0} };

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
    Analog_Input_Set_Object_Name(index, &bacnet_string, uuid);
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
    Analog_Output_Set_Object_Name(index, &bacnet_string, uuid);
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
    Analog_Value_Set_Object_Name(index, &bacnet_string, uuid);
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
    Binary_Input_Set_Object_Name(index, &bacnet_string, uuid);
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
    Binary_Output_Set_Object_Name(index, &bacnet_string, uuid);
  } else if (!strcasecmp(prop_name, "pv")) {
    pv = (!strcasecmp(value, "null")) ? 255 : (i_val == 0) ? 0 : 1;
    Binary_Output_Present_Value_Set(index, pv, BACNET_MAX_PRIORITY, uuid);
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
    Binary_Value_Set_Object_Name(index, &bacnet_string, uuid);
  } else if (!strcasecmp(prop_name, "pv")) {
    pv = (!strcasecmp(value, "null")) ? 255 : (i_val == 0) ? 0 : 1;
    Binary_Value_Present_Value_Set(index, pv, BACNET_MAX_PRIORITY, uuid);
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
int process_bacnet_client_whois_command(void)
{
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
int mqtt_publish_command_result(int object_type, int object_instance, int property_id, int vtype, void *vptr, int topic_id)
{
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
  char topic[512];
  char topic_value[1024];
  char buf[1024];
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
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" }", (char*)vptr);
      pubmsg.payload = topic_value;
      pubmsg.payloadlen = strlen(topic_value);
      break;

    case MQTT_TOPIC_VALUE_BACNET_STRING:
      characterstring_ansi_copy(buf, sizeof(buf), vptr);
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" }", buf);
      pubmsg.payload = topic_value;
      pubmsg.payloadlen = strlen(topic_value);
      break;

    case MQTT_TOPIC_VALUE_INTEGER:
      sprintf(buf, "%d", *((int*)vptr));
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" }", buf);
      pubmsg.payload = topic_value;
      pubmsg.payloadlen = strlen(topic_value);
      break;

    case MQTT_TOPIC_VALUE_FLOAT:
      sprintf(buf, "%.6f", *((float*)vptr));
      snprintf(topic_value, sizeof(topic_value), "{ \"value\" : \"%s\" }", buf);
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
  }

  return(str);
}


int encode_read_value_result(BACNET_READ_PROPERTY_DATA *data, char *buf, int buf_len)
{
  BACNET_APPLICATION_DATA_VALUE value;
  char tmp[100];

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

        default:
          return(1);
      }

      snprintf(buf, buf_len, "{ \"value\" : \"%s\" }", tmp);
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

        default:
          return(1);
      }

      snprintf(buf, buf_len, "{ \"value\" : \"%s\" }", tmp);
      break;

    default:
      return(1);
  }

  return(0);
}


/*
 * Publish bacnet client read value result.
 */
int publish_bacnet_client_read_value_result(BACNET_READ_PROPERTY_DATA *data)
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

  encode_read_value_result(data, topic_value, sizeof(topic_value));

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
 * Bacnet client read value handler.
 */
void bacnet_client_read_value_handler(uint8_t *service_request,
  uint16_t service_len,
  BACNET_ADDRESS *src,
  BACNET_CONFIRMED_SERVICE_ACK_DATA *service_data)
{
  BACNET_READ_PROPERTY_DATA data;
  int len;

  printf("- bacnet_client_read_value_handler() => %d\n", getpid());
  printf("-- service_data->invoke_id: %d\n", service_data->invoke_id);

  if (is_bacnet_client_request_present(service_data->invoke_id)) {
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
    publish_bacnet_client_read_value_result(&data);
  }
}


/*
 * Initialize bacnet client service handlers.
 */
void init_bacnet_client_service_handlers(void)
{
  apdu_set_confirmed_ack_handler(
    SERVICE_CONFIRMED_READ_PROPERTY, bacnet_client_read_value_handler);
}


/*
 * Process local bacnet client read value command.
 */
int process_local_read_value_command(bacnet_client_cmd_opts *opts)
{
  BACNET_CHARACTER_STRING bs_val;
  float f_val;
  int i_val;

  switch (opts->object_type) {
    case OBJECT_ANALOG_INPUT:
      if (Analog_Input_Instance_To_Index(opts->object_instance) < 1) {
        return(1);
      }

      switch(opts->property) {
        case PROP_PRESENT_VALUE:
          f_val = Analog_Input_Present_Value(opts->object_instance);
          mqtt_publish_command_result(OBJECT_ANALOG_INPUT, opts->object_instance, PROP_PRESENT_VALUE,
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
          break;

        case PROP_OBJECT_NAME:
          Analog_Input_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_ANALOG_INPUT, opts->object_instance, PROP_OBJECT_NAME,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
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
          mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_PRESENT_VALUE,
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
          break;

        case PROP_OBJECT_NAME:
          Analog_Output_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, opts->object_instance, PROP_OBJECT_NAME,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
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
          mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_PRESENT_VALUE,
            MQTT_TOPIC_VALUE_FLOAT, &f_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
          break;

        case PROP_OBJECT_NAME:
          Analog_Value_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_ANALOG_VALUE, opts->object_instance, PROP_OBJECT_NAME,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
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
          mqtt_publish_command_result(OBJECT_BINARY_INPUT, opts->object_instance, PROP_PRESENT_VALUE,
            MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
          break;

        case PROP_OBJECT_NAME:
          Binary_Input_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_BINARY_INPUT, opts->object_instance, PROP_OBJECT_NAME,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
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
          mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_PRESENT_VALUE,
            MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
          break;

        case PROP_OBJECT_NAME:
          Binary_Output_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_BINARY_OUTPUT, opts->object_instance, PROP_OBJECT_NAME,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
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
          mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_PRESENT_VALUE,
            MQTT_TOPIC_VALUE_INTEGER, &i_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
          break;

        case PROP_OBJECT_NAME:
          Binary_Value_Object_Name(opts->object_instance, &bs_val);
          mqtt_publish_command_result(OBJECT_BINARY_VALUE, opts->object_instance, PROP_OBJECT_NAME,
            MQTT_TOPIC_VALUE_BACNET_STRING, &bs_val, MQTT_READ_VALUE_CMD_RESULT_TOPIC);
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
      if (opts->property != PROP_OBJECT_NAME && opts->property != PROP_PRESENT_VALUE) {
        if (mqtt_debug) {
          printf("Unsupported object_type %d property: %d\n", opts->object_type, opts->property);
        }

        return(1);
      }

      if (is_command_for_us(opts)) {
        printf("command for us\n");
        process_local_read_value_command(opts);
      } else {
        if (opts->tag_flags & CMD_TAG_FLAG_SLOW_TEST) {
          printf("** Slow test enabled!\n");
          usleep(1000000);
        }

        request_invoke_id = Send_Read_Property_Request(opts->device_instance,
          opts->object_type, opts->object_instance, opts->property, opts->index);
        printf("read request_invoke_id: %d\n", request_invoke_id);

        add_bacnet_client_request(request_invoke_id);

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
  return(0);
}


/*
 * Process Bacnet client write value command.
 */
int process_bacnet_client_write_value_command(bacnet_client_cmd_opts *opts)
{
  BACNET_APPLICATION_DATA_VALUE value = { 0 };
  BACNET_MAC_ADDRESS mac = { 0 };
  BACNET_ADDRESS dest = { 0 };
  bool specific_address = false;
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
        request_invoke_id = Send_Write_Property_Request(
          opts->device_instance, opts->object_type,
          opts->object_instance, opts->property,
          &value,
          opts->priority,
          opts->index);
        printf("read request_invoke_id: %d\n", request_invoke_id);
      }

      break;

    default:
      if (mqtt_debug) {
        printf("Unknown write value object_type: [%d]\n", opts->object_type);
      }
      return(1);
  }

  /* if (strcmp(property_id, "name") && strcmp(property_id, "pv")) {
    if (mqtt_debug) {
      printf("Unknown write value property_id: [%s]\n", property_id);
    }

    return(1);
  }

  if (!strcmp(object_type, "ai")) {
    index = Analog_Input_Instance_To_Index(object_instance);
    if (!index) {
      if (mqtt_debug) {
        printf("Unknown AI object index: [%s]\n", object_index);
      }

      return(1);
    }

    if (!strcmp(property_id, "pv")) {
      f = strtof(value, &endptr);
      if (endptr != NULL && strlen(endptr)) {
        if (mqtt_debug) {
          printf("Invalid write value AI value: [%s]\n", value);
        }

        return(1);
      }

      Analog_Input_Present_Value_Set(object_instance, f, NULL, true);
      f = Analog_Input_Present_Value(object_instance);
      mqtt_publish_command_result(OBJECT_ANALOG_INPUT, object_instance, PROP_PRESENT_VALUE,
        MQTT_TOPIC_VALUE_FLOAT, &f, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
    }
  } else if (!strcmp(object_type, "ao")) {
    index = Analog_Output_Instance_To_Index(object_instance);
    if (!index) {
      if (mqtt_debug) {
        printf("Unknown AO object index: [%s]\n", object_index);
      }
        
      return(1);
    }   
            
    if (!strcmp(property_id, "pv")) {
      f = strtof(value, &endptr);
      if (endptr != NULL && strlen(endptr)) {
        if (mqtt_debug) {
          printf("Invalid write value AO value: [%s]\n", value);
        }   
        
        return(1);
      }

      Analog_Output_Present_Value_Set(object_instance, f, BACNET_MAX_PRIORITY, NULL, true);
      f = Analog_Output_Present_Value(object_instance);
      mqtt_publish_command_result(OBJECT_ANALOG_OUTPUT, object_instance, PROP_PRESENT_VALUE,
        MQTT_TOPIC_VALUE_FLOAT, &f, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
    }
  } else if (!strcmp(object_type, "av")) {
    index = Analog_Value_Instance_To_Index(object_instance);
    if (!index) {
      if (mqtt_debug) {
        printf("Unknown AV object index: [%s]\n", object_index);
      }

      return(1);
    }

    if (!strcmp(property_id, "pv")) {
      f = strtof(value, &endptr);
      if (endptr != NULL && strlen(endptr)) {
        if (mqtt_debug) {
          printf("Invalid write value AV value: [%s]\n", value);
        }

        return(1);
      }

      Analog_Value_Present_Value_Set(object_instance, f, BACNET_MAX_PRIORITY, NULL, true);
      f = Analog_Value_Present_Value(object_instance);
      mqtt_publish_command_result(OBJECT_ANALOG_VALUE, object_instance, PROP_PRESENT_VALUE,
        MQTT_TOPIC_VALUE_FLOAT, &f, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
    }
  } else if (!strcmp(object_type, "bi")) {
    index = Binary_Input_Instance_To_Index(object_instance);
    if (!index) {
      if (mqtt_debug) {
        printf("Unknown BI object index: [%s]\n", object_index);
      }

      return(1);
    }

    if (!strcmp(property_id, "pv")) {
      i_val = atoi(value);
      pv = (i_val == 0) ? 0 : 1;

      Binary_Input_Present_Value_Set(object_instance, pv, NULL, true);
      f = Binary_Input_Present_Value(object_instance);
      mqtt_publish_command_result(OBJECT_BINARY_INPUT, object_instance, PROP_PRESENT_VALUE,
        MQTT_TOPIC_VALUE_INTEGER, &pv, MQTT_WRITE_VALUE_CMD_RESULT_TOPIC);
    }
  } else {
    if (mqtt_debug) {
      printf("Unknown write value object_type: [%s]\n", object_type);
    }

    return(1);
  } */

  return(0);
}


/*
 * Process Bacnet client commands.
 */
int process_bacnet_client_command(char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], int n_topic_tokens,
  bacnet_client_cmd_opts *opts)
{
  if (!strcasecmp(topic_tokens[2], "whois")) {
    process_bacnet_client_whois_command();
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
 * Add bacnet client request.
 */
int add_bacnet_client_request(uint8_t invoke_id)
{
  llist_cb *ptr;

  ptr = malloc(sizeof(llist_cb));
  if (!ptr) {
    printf("Error allocating memory for llist_cb!\n");
    return(1);
  }

  ptr->data.invoke_id = invoke_id;
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
  const char **topics;

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

