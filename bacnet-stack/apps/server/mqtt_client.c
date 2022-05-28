/*
 * MQTT client module
 */
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mqtt_client.h"
#include "MQTTClient.h"

#include "bacnet/basic/object/device.h"

/* globals */
static int mqtt_debug = false;
static char mqtt_broker_ip[51] = {0};
static int mqtt_broker_port = DEFAULT_MQTT_BROKER_PORT;
static char mqtt_client_id[51] = {0};
static MQTTClient mqtt_client;

/* topic and object/property mappings */
static topic_mapping_t topic_mappings[] = {
  { DEVICE_OBJECT_NAME_TOPIC_ID,
    "device_object_name" },
  { BINARY_OUTPUT_OBJECT_NAME_TOPIC_ID,
    "binary_output_object_name" },
  { TOPIC_ID_MAX, NULL }
};


/*
 * Initialize mqtt client module.
 */
int mqtt_client_init(void)
{
  char mqtt_broker_endpoint[100];
  char buf[100];
  char *pEnv;

  pEnv = getenv("MQTT_DEBUG");
  if (pEnv) {
    mqtt_debug = true;
  }

  pEnv = getenv("MQTT_BROKER_IP");
  if (pEnv) {
    strncpy(mqtt_broker_ip, pEnv, sizeof(mqtt_broker_ip) -1);
  } else {
    strcpy(mqtt_broker_ip, DEFAULT_MQTT_BROKER_IP);
  }

  pEnv = getenv("MQTT_BROKER_PORT");
  if (pEnv) {
    mqtt_broker_port = atoi(pEnv);
  }

  sprintf(mqtt_broker_endpoint, "%s:%d", mqtt_broker_ip, mqtt_broker_port);

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

  MQTTClient_create(&mqtt_client, mqtt_broker_endpoint, mqtt_client_id, MQTTCLIENT_PERSISTENCE_NONE, NULL);

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
char *mqtt_create_topic(int topic_id, char *buf, int buf_len)
{
  topic_mapping_t *ptm;

  if (topic_id >= TOPIC_ID_MAX) {
    return(NULL);
  }

  for (ptm = &topic_mappings[0]; ptm->topic_id != TOPIC_ID_MAX; ptm++) {
    if (ptm->topic_id == topic_id) {
      snprintf(buf, buf_len - 1, "/%d/%s", Device_Object_Instance_Number(), ptm->topic_mid_name);
      return(buf);
    }
  }

  return(NULL); 
}


/*
 * Publish topic.
 */
int mqtt_publish_topic(int topic_id, int vtype, void *vptr)
{
  MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
  MQTTClient_message pubmsg = MQTTClient_message_initializer;
  MQTTClient_deliveryToken token;
  char topic[512];
  char buf[1024];
  int rc;

  if (!mqtt_create_topic(topic_id, topic, sizeof(topic))) {
    printf("Failed to create MQTT topic: %d\n", topic_id);
    return(1);
  }

  if (mqtt_debug) {
    printf("MQTT topic: %s\n", topic);
  }

  switch(vtype) {
    case MQTT_TOPIC_VALUE_STRING:
      pubmsg.payload = vptr;
      pubmsg.payloadlen = strlen(vptr);
      break;

    case MQTT_TOPIC_VALUE_BACNET_STRING:
      characterstring_ansi_copy(buf, sizeof(buf), vptr);
      pubmsg.payload = buf;
      pubmsg.payloadlen = strlen(buf);
      break;

    default:
      printf("MQTT unsupported topic value: %d\n", vtype);
      return(1);
  }

  conn_opts.keepAliveInterval = 20;
  conn_opts.cleansession = 1;
  if ((rc = MQTTClient_connect(mqtt_client, &conn_opts)) != MQTTCLIENT_SUCCESS) {
    printf("Failed to connect to MQTT broker, return code %d\n", rc);
    return(1);
  }

  pubmsg.qos = DEFAULT_PUB_QOS;
  pubmsg.retained = 1;
  MQTTClient_publishMessage(mqtt_client, topic, &pubmsg, &token);
  rc = MQTTClient_waitForCompletion(mqtt_client, token, DEFAULT_PUB_TIMEOUT);
  if (mqtt_debug) {
    if (rc) {
      printf("MQTT failed to publish topic: \"%s\" with token %d\n", topic, token);
    } else {
      printf("MQTT published topic: \"%s\" with token %d\n", topic, token);
    }
  }

  MQTTClient_disconnect(mqtt_client, 10000);

  return(0);
}

