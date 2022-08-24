/*
 * YAML config module
 */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cyaml/cyaml.h>
#include "yaml_config.h"

#include "bacnet/basic/object/device.h"

#define DEFAULT_YAML_CONFIG   "/data/bacnet-server-driver/config/config.yml"

/* mqtt struct */
struct _mqtt {
  const char *broker_ip;
  uint16_t broker_port;
  const char *debug;
  const char *enable;
};

/* top level struct for storing the configuration */
struct _bacnet_config {
  const char *server_name;
  const char *device_id;
  const char *iface;
  uint16_t port;
  uint8_t bi_max;
  uint8_t bo_max;
  uint8_t bv_max;
  uint8_t ai_max;
  uint8_t ao_max;
  uint8_t av_max;
  struct _mqtt *mqtt;
};

/* globals */
static struct _bacnet_config *bacnet_config = NULL;
static int yaml_config_debug = false;
static int yaml_config_mqtt_disable = false;

static const cyaml_schema_field_t mqtt_fields_schema[] = {
  CYAML_FIELD_STRING_PTR(
    "broker_ip", CYAML_FLAG_POINTER,
    struct _mqtt, broker_ip, 0, CYAML_UNLIMITED),

  CYAML_FIELD_UINT(
    "broker_port", CYAML_FLAG_DEFAULT,
    struct _mqtt, broker_port),

  CYAML_FIELD_STRING_PTR(
    "debug", CYAML_FLAG_POINTER,
    struct _mqtt, debug, 0, CYAML_UNLIMITED),

  CYAML_FIELD_STRING_PTR(
    "enable", CYAML_FLAG_POINTER,
    struct _mqtt, enable, 0, CYAML_UNLIMITED),

  CYAML_FIELD_END
};

static const cyaml_schema_field_t config_fields_schema[] = {
  CYAML_FIELD_STRING_PTR(
    "server_name", CYAML_FLAG_POINTER,
    struct _bacnet_config, server_name, 0, CYAML_UNLIMITED),

  CYAML_FIELD_STRING_PTR(
    "device_id", CYAML_FLAG_POINTER,
    struct _bacnet_config, device_id, 0, CYAML_UNLIMITED),

  CYAML_FIELD_STRING_PTR(
    "iface", CYAML_FLAG_POINTER,
    struct _bacnet_config, iface, 0, CYAML_UNLIMITED),

  CYAML_FIELD_UINT(
    "port", CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, port),

  CYAML_FIELD_UINT(
    "bi_max", CYAML_FLAG_DEFAULT,
    struct _bacnet_config, bi_max),

  CYAML_FIELD_UINT(
    "bo_max", CYAML_FLAG_DEFAULT,
    struct _bacnet_config, bo_max),

  CYAML_FIELD_UINT(
    "bv_max", CYAML_FLAG_DEFAULT,
    struct _bacnet_config, bv_max),

  CYAML_FIELD_UINT(
    "ai_max", CYAML_FLAG_DEFAULT,
    struct _bacnet_config, ai_max),

  CYAML_FIELD_UINT(
    "ao_max", CYAML_FLAG_DEFAULT,
    struct _bacnet_config, ao_max),

  CYAML_FIELD_UINT(
    "av_max", CYAML_FLAG_DEFAULT,
    struct _bacnet_config, av_max),

  CYAML_FIELD_MAPPING_PTR(
    "mqtt", CYAML_FLAG_POINTER,
    struct _bacnet_config, mqtt, mqtt_fields_schema),

  CYAML_FIELD_END
};

static const cyaml_schema_value_t config_schema = {
  CYAML_VALUE_MAPPING(CYAML_FLAG_POINTER,
    struct _bacnet_config, config_fields_schema),
};

static const cyaml_config_t yaml_config = {
  .log_fn = cyaml_log,            /* Use the default logging function. */
  .mem_fn = cyaml_mem,            /* Use the default memory allocator. */
  .log_level = CYAML_LOG_WARNING, /* Logging errors and warnings only. */
};

/*
 * Initialize yaml config module.
 */
int yaml_config_init(void)
{
  struct stat statbuf;
  char *pEnv;
  char config_file[255];
  cyaml_err_t err;

  pEnv = getenv("YAML_CONFIG_DEBUG");
  if (pEnv) {
    yaml_config_debug = true;
  }

  pEnv = getenv("MQTT_DISABLE");
  if (pEnv) {
    yaml_config_mqtt_disable = true;
  }

  pEnv = getenv("g");
  if (pEnv) {
    sprintf(config_file, "%s/config/", pEnv);
  } else {
    printf("Global configuration directory not specified. Exiting!\n");
    exit(0);
  }

  pEnv = getenv("s");
  if (pEnv) {
    sprintf(&config_file[strlen(config_file)], "%s", pEnv);
  } else {
    printf("Configuration file not specified. Exiting!\n");
    exit(0);
  }

  if (yaml_config_debug) {
    printf("YAML Config: configuration file: %s\n", config_file);
  }

  if (stat(config_file, &statbuf) < 0) {
    printf("Configuration file (%s) not found. Exiting!\n", config_file);
    exit(0);
  }

  err = cyaml_load_file(config_file, &yaml_config,
    &config_schema, (void **)&bacnet_config, NULL);
  if (err != CYAML_OK) {
    fprintf(stderr, "ERROR: %s\n", cyaml_strerror(err));
    exit(1);
  }

  if (yaml_config_debug) {
    yaml_config_dump();
  }

  return(0);
}


/*
 * Dump yaml configs.
 */
void yaml_config_dump(void)
{
  printf("YAML Config: server_name: %s\n", (bacnet_config->server_name) ?
    bacnet_config->server_name : "null");
  printf("YAML Config: device_id: %s\n", (bacnet_config->device_id) ?
    bacnet_config->device_id : "null");
  printf("YAML Config: iface: %s\n", (bacnet_config->iface) ?
    bacnet_config->iface : "null");
  printf("YAML Config: port: %d\n", bacnet_config->port);
  printf("YAML Config: bi_max: %d\n", bacnet_config->bi_max);
  printf("YAML Config: bo_max: %d\n", bacnet_config->bo_max);
  printf("YAML Config: bv_max: %d\n", bacnet_config->bv_max);
  printf("YAML Config: ai_max: %d\n", bacnet_config->ai_max);
  printf("YAML Config: ao_max: %d\n", bacnet_config->ao_max);
  printf("YAML Config: av_max: %d\n", bacnet_config->av_max);
  if (bacnet_config->mqtt) {
    printf("YAML Config: mqtt->broker_ip: %s\n", (bacnet_config->mqtt->broker_ip) ?
      bacnet_config->mqtt->broker_ip : "null");
    printf("YAML Config: mqtt->broker_port: %d\n", bacnet_config->mqtt->broker_port);
    printf("YAML Config: mqtt->debug: %s\n", (bacnet_config->mqtt->debug) ?
      bacnet_config->mqtt->debug: "null");
    printf("YAML Config: mqtt->enable: %s\n", (bacnet_config->mqtt->enable) ?
      bacnet_config->mqtt->enable: "null");
  }
}


/*
 * Get server name.
 */
const char *yaml_config_server_name(void)
{
  return(bacnet_config->server_name);
}


/*
 * Get device ID.
 */
const char *yaml_config_service_id(void)
{
  return(bacnet_config->device_id);
}


/*
 * Get interface name.
 */
const char *yaml_config_iface(void)
{
  return(bacnet_config->iface);
}


/*
 * Get port.
 */
int yaml_config_port(void)
{
  return(bacnet_config->port);
}


/*
 * Get BI max.
 */
int yaml_config_bi_max(void)
{
  return(bacnet_config->bi_max);
}


/*
 * Get BO max.
 */
int yaml_config_bo_max(void)
{
  return(bacnet_config->bo_max);
}


/*
 * Get BV max.
 */
int yaml_config_bv_max(void)
{
  return(bacnet_config->bv_max);
}


/*
 * Get AI max.
 */
int yaml_config_ai_max(void)
{
  return(bacnet_config->ai_max);
}


/*
 * Get AO max.
 */
int yaml_config_ao_max(void)
{
  return(bacnet_config->ao_max);
}


/*
 * Get AV max.
 */
int yaml_config_av_max(void)
{
  return(bacnet_config->av_max);
}


/*
 * Get MQTT Broker IP.
 */
const char *yaml_config_mqtt_broker_ip(void)
{
  return(bacnet_config->mqtt->broker_ip);
}


/*
 * Get MQTT Broker Port.
 */
int yaml_config_mqtt_broker_port(void)
{
  return(bacnet_config->mqtt->broker_port);
}


/*
 * Get MQTT debug flag.
 */
int yaml_config_mqtt_debug(void)
{
  return ((bacnet_config->mqtt && bacnet_config->mqtt->debug &&
    !strcmp(bacnet_config->mqtt->debug, "true")) ? true : false);
}


/*
 * Get MQTT enable flag.
 */
int yaml_config_mqtt_enable(void)
{
  if (yaml_config_mqtt_disable) {
    return(false);
  }

  return ((bacnet_config->mqtt && bacnet_config->mqtt->enable &&
    !strcmp(bacnet_config->mqtt->enable, "true")) ? true : false);
}


/*
 * Cleanup yaml config.
 */
int yaml_config_cleanup(void)
{
  if (bacnet_config) {
    cyaml_free(&yaml_config, &config_schema, bacnet_config, 0);
  }

  return(0);
}

