/*
 * MQTT client module
 */
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "MQTTClient.h"
#include "json-c/json.h"
#include "mqtt_client.h"
#if defined(YAML_CONFIG)
#include "yaml_config.h"
#endif /* defined(YAML_CONFIG) */

#include "bacnet/basic/object/device.h"

/* forward decls */
extern bool Analog_Input_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid);
extern void Analog_Input_Present_Value_Set(
  uint32_t object_instance,
  float value,
  char *uuid);

extern bool Analog_Output_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid);
extern bool Analog_Output_Present_Value_Set(
  uint32_t object_instance, float value, unsigned priority, char *uuid);
extern bool Analog_Output_Priority_Array_Set(
  uint32_t object_instance, float value, unsigned priority, char *uuid);
extern bool Analog_Output_Priority_Array_Set2(
  uint32_t object_instance, float value, unsigned priority);

extern bool Analog_Value_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid);
extern bool Analog_Value_Present_Value_Set(
  uint32_t object_instance, float value, uint8_t priority, char *uuid);
extern bool Analog_Value_Priority_Array_Set(
  uint32_t object_instance, float value, uint8_t priority, char *uuid);
extern bool Analog_Value_Priority_Array_Set2(
  uint32_t object_instance, float value, uint8_t priority);

extern bool Binary_Input_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid);
extern bool Binary_Input_Present_Value_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, char *uuid);

extern bool Binary_Output_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid);
extern bool Binary_Output_Present_Value_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid);
extern bool Binary_Output_Priority_Array_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid);
extern bool Binary_Output_Priority_Array_Set2(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority);

extern bool Binary_Value_Set_Object_Name(
  uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid);
extern bool Binary_Value_Present_Value_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid);
extern bool Binary_Value_Priority_Array_Set(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid);
extern bool Binary_Value_Priority_Array_Set2(
  uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority);

int tokenize_topic(char *topic, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH]);
void dump_topic_tokens(int n_topic_tokens, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH]);
int process_ai_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int process_ao_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int process_av_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int process_bi_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int process_bo_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
int process_bv_write(int index, char topic_tokens[MAX_TOPIC_TOKENS][MAX_TOPIC_TOKEN_LENGTH], char *value, char *uuid);
void mqtt_connection_lost(void *context, char *cause);
int mqtt_msg_arrived(void *context, char *topic, int topic_len, MQTTClient_message *message);
void mqtt_msg_delivered(void *context, MQTTClient_deliveryToken dt);
int subscribe_write_prop_name(void);
int subscribe_write_prop_present_value(void);
int subscribe_write_prop_priority_array(void);
int mqtt_subscribe_to_topics(void);

/* globals */
static int mqtt_debug = false;
static char mqtt_broker_ip[51] = {0};
static int mqtt_broker_port = DEFAULT_MQTT_BROKER_PORT;
static char mqtt_client_id[124] = {0};
static MQTTClient mqtt_client;


/*
 * MQTT subscribe connection lost callback.
 */
void mqtt_connection_lost(void *context, char *cause)
{ 
  if (mqtt_debug) {
     printf("MQTT connection lost: %s\n", cause);
  }
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
    Analog_Input_Present_Value_Set(index, f, uuid);
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
    Analog_Output_Present_Value_Set(index, f, BACNET_MAX_PRIORITY, uuid);
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
    Analog_Value_Present_Value_Set(index, f, BACNET_MAX_PRIORITY, uuid);
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
    Binary_Input_Present_Value_Set(index, pv, uuid);
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
 * MQTT message arrived callback.
 */
int mqtt_msg_arrived(void *context, char *topic, int topic_len, MQTTClient_message *message)
{
  json_object *json_root, *json_field;
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
     printf("MQTT message arrived:\n");
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
 
  EXIT:

  MQTTClient_freeMessage(&message);
  MQTTClient_free(topic);

  return(1);
}


/*
 * MQTT message delivered callback.
 */
void mqtt_msg_delivered(void *context, MQTTClient_deliveryToken dt)
{
  if (mqtt_debug) {
     printf("MQTT message delivered\n");
  }
}


/*
 * Initialize mqtt client module.
 */
int mqtt_client_init(void)
{
  MQTTClient_createOptions createOpts = MQTTClient_createOptions_initializer;
  MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer5;
  MQTTProperties props = MQTTProperties_initializer;
  MQTTProperties willProps = MQTTProperties_initializer;
  MQTTResponse response = MQTTResponse_initializer;
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

  createOpts.MQTTVersion = MQTTVERSION_5;
  rc = MQTTClient_createWithOptions(&mqtt_client, mqtt_broker_endpoint, mqtt_client_id, MQTTCLIENT_PERSISTENCE_NONE, NULL, &createOpts);
  if (rc != MQTTCLIENT_SUCCESS) {
    printf("MQTT error creating client instance: %s\n", mqtt_broker_endpoint);
    return(1);
  }

  rc = MQTTClient_setCallbacks(mqtt_client, NULL, mqtt_connection_lost, mqtt_msg_arrived, mqtt_msg_delivered);
  if (rc != MQTTCLIENT_SUCCESS) {
    printf("MQTT error setting up callbacks\n");
    return(1);
  }

  conn_opts.keepAliveInterval = 30;
  conn_opts.MQTTVersion = MQTTVERSION_5;
  conn_opts.cleanstart = 1;
  response = MQTTClient_connect5(mqtt_client, &conn_opts, &props, &willProps);
  rc = response.reasonCode;
  MQTTResponse_free(response);
  if (rc != MQTTCLIENT_SUCCESS)
  {
    printf("MQTT failed to connect to server: %s\n", MQTTClient_strerror(rc));
    return(1);
  }

  if (yaml_config_mqtt_write_via_subscribe()) {
    printf("MQTT write via subscribe enabled\n");
    rc = mqtt_subscribe_to_topics();
    if (rc) {
      printf("- Failed to subscribe to one of the topics\n");
      return(1);
    }
  }

  return(0);
}


/*
 * Subscribe to write property name topics.
 */
int subscribe_write_prop_name(void)
{
  MQTTResponse response;
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

    response = MQTTClient_subscribe5(mqtt_client, topics[i], 0, NULL, NULL);
    rc = response.reasonCode;
    MQTTResponse_free(response);
    if (rc != MQTTCLIENT_SUCCESS) {
      if (mqtt_debug) {
        printf("- Failed to subscribe: %s\n", MQTTClient_strerror(rc));
      }

      return(1);
    } else {
      if (mqtt_debug) {
        printf("- Subscribed\n");
      }
    }
  }

  return(0);
}


/*
 * Subscribe to write property present value topics.
 */
int subscribe_write_prop_present_value(void)
{
  MQTTResponse response;
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

    response = MQTTClient_subscribe5(mqtt_client, topics[i], 0, NULL, NULL);
    rc = response.reasonCode;
    MQTTResponse_free(response);
    if (rc != MQTTCLIENT_SUCCESS) {
      if (mqtt_debug) {
        printf("- Failed to subscribe: %s\n", MQTTClient_strerror(rc));
      }
      
      return(1);
    } else {
      if (mqtt_debug) {
        printf("- Subscribed\n");
      }
    }
  }

  return(0);
}


/*
 * Subscribe to write property name topics.
 */
int subscribe_write_prop_priority_array(void)
{
  MQTTResponse response;
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

    response = MQTTClient_subscribe5(mqtt_client, topics[i], 0, NULL, NULL);
    rc = response.reasonCode;
    MQTTResponse_free(response);
    if (rc != MQTTCLIENT_SUCCESS) {
      if (mqtt_debug) {
        printf("- Failed to subscribe: %s\n", MQTTClient_strerror(rc));
      }

      return(1);
    } else {
      if (mqtt_debug) {
        printf("- Subscribed\n");
      }
    }
  }

  return(0);
}


/*
 * Subscribe to topics.
 */
int mqtt_subscribe_to_topics(void)
{
  int i, n_topics = 0;
  const char **topics;

  topics = yaml_config_properties(&n_topics);
  if ((topics == NULL) || (n_topics == 0)) {
    return(0);
  }

  for (i = 0; i < n_topics; i++) {
    if (!strncmp(topics[i], "name", 4)) {
      if (subscribe_write_prop_name()) {
        return(1);
      }
    } else if (!strncmp(topics[i], "pv", 2)) {
      if (subscribe_write_prop_present_value()) {
        return(1);
      }
    } else if (!strncmp(topics[i], "pri", 3)) {
      if (subscribe_write_prop_priority_array()) {
        return(1);
      }
    }
  }

  return(0);
}


/*
 * Shutdown mqtt client module.
 */
void mqtt_client_shutdown(void)
{
  MQTTClient_disconnect(mqtt_client, 10000);
  MQTTClient_destroy(&mqtt_client);
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
char *mqtt_create_topic(int object_type, int object_instance, int property_id, char *buf, int buf_len)
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

  snprintf(buf, buf_len - 1, "bacnet/%s/%d/%s", object_type_str,
    object_instance, property_id_str);

  return(buf); 
}


/*
 * Publish topic.
 */
int mqtt_publish_topic(int object_type, int object_instance, int property_id, int vtype, void *vptr, char *uuid_value)
{
  MQTTResponse response;
  MQTTClient_message pubmsg = MQTTClient_message_initializer;
  MQTTClient_deliveryToken token;
  char topic[512];
  char topic_value[1024];
  char buf[1024];
  int rc;

  if (!mqtt_create_topic(object_type, object_instance, property_id, topic, sizeof(topic))) {
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

  pubmsg.qos = DEFAULT_PUB_QOS;
  pubmsg.retained = 0;
  response = MQTTClient_publishMessage5(mqtt_client, topic, &pubmsg, &token);
  rc = response.reasonCode;
  MQTTResponse_free(response);
  if (mqtt_debug) {
    if (rc != MQTTCLIENT_SUCCESS) {
      printf("MQTT failed to publish topic: \"%s\" with token %d\n", topic, token);
    } else {
      printf("MQTT published topic: \"%s\" with token %d\n", topic, token);
      rc = MQTTClient_waitForCompletion(mqtt_client, token, DEFAULT_PUB_TIMEOUT);
    }
  }

  return(0);
}

