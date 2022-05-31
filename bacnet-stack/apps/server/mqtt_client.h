#ifndef MQTT_CLIENT_H
#define MQTT_CLIENT_H

#ifndef true
#define true                           1
#endif

#ifndef false
#define false                          0
#endif

/* default values */
#define DEFAULT_MQTT_BROKER_IP         "127.0.0.1"
#define DEFAULT_MQTT_BROKER_PORT       1883
#define DEFAULT_PUB_QOS                1
#define DEFAULT_PUB_TIMEOUT            10000L

/* topic value types */
#define MQTT_TOPIC_VALUE_STRING          1
#define MQTT_TOPIC_VALUE_BACNET_STRING   2
#define MQTT_TOPIC_VALUE_INTEGER         3

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

  int mqtt_client_init(void);
  void mqtt_client_shutdown(void);
  char *mqtt_form_publish_topic(char *device_id, char *object_name);
  char *mqtt_create_topic(int object_type, int object_instance, int property_id, char *buf, int buf_len);
  int mqtt_publish_topic(int object_type, int object_instance, int property_id, int vtype, void *vptr);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif
