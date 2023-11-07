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

#define DEFAULT_YAML_CONFIG         "/data/bacnet-server-driver/config/config.yml"
#define MAX_YAML_STR_VALUE_LENGTH   51

/* mqtt struct */
struct _mqtt {
  char *broker_ip;
  uint16_t broker_port;
  char *debug;
  char *enable;
  char *write_via_subscribe;
  char *retry_enable;
  unsigned int retry_limit;
  unsigned int retry_interval;
};

/* bacnet-client */
struct _bacnet_client {
  char *debug;
  char *enable;
  char **commands;
  unsigned int n_commands;
  char *whois_program;
  char *read_program;
  char *write_program;
  char **tokens;
  unsigned int n_tokens;
  char *filter_objects;
};

/* top level struct for storing the configuration */
struct _bacnet_config {
  char *server_name;
  char *device_id;
  char *iface;
  uint16_t port;
  unsigned int bi_max;
  unsigned int bo_max;
  unsigned int bv_max;
  unsigned int ai_max;
  unsigned int ao_max;
  unsigned int av_max;
  unsigned int msi_max;
  unsigned int mso_max;
  unsigned int msv_max;
  struct _mqtt *mqtt;
  char **objects;
  unsigned int n_objects;
  char **properties;
  unsigned n_properties;
  struct _bacnet_client *bacnet_client;
};

/* forward decls */
void load_default_settings(void);

/* globals */
static struct _bacnet_config *bacnet_config = NULL;
static int yaml_config_debug = false;
static int yaml_config_mqtt_disable = false;
static int yaml_config_bacnet_client_disable = false;

/* Schema for string pointer values (used in sequences of strings). */
static const cyaml_schema_value_t string_ptr_schema = {
  CYAML_VALUE_STRING(CYAML_FLAG_POINTER, char, 0, CYAML_UNLIMITED),
};

static const cyaml_schema_field_t mqtt_fields_schema[] = {
  CYAML_FIELD_STRING_PTR(
    "broker_ip", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _mqtt, broker_ip, 0, CYAML_UNLIMITED),

  CYAML_FIELD_UINT(
    "broker_port", CYAML_FLAG_DEFAULT | CYAML_FLAG_OPTIONAL,
    struct _mqtt, broker_port),

  CYAML_FIELD_STRING_PTR(
    "debug", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _mqtt, debug, 0, CYAML_UNLIMITED),

  CYAML_FIELD_STRING_PTR(
    "enable", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _mqtt, enable, 0, CYAML_UNLIMITED),

  CYAML_FIELD_STRING_PTR(
    "write_via_subscribe", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _mqtt, write_via_subscribe, 0, CYAML_UNLIMITED),

  CYAML_FIELD_STRING_PTR(
    "retry_enable", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _mqtt, retry_enable, 0, CYAML_UNLIMITED),

  CYAML_FIELD_UINT(
    "retry_limit", CYAML_FLAG_OPTIONAL,
    struct _mqtt, retry_limit),

  CYAML_FIELD_UINT(
    "retry_interval", CYAML_FLAG_OPTIONAL,
    struct _mqtt, retry_interval),

  CYAML_FIELD_END
};

static const cyaml_schema_field_t bacnet_client_fields_schema[] = {
  CYAML_FIELD_STRING_PTR(
    "debug", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _bacnet_client, debug, 0, CYAML_UNLIMITED),

  CYAML_FIELD_STRING_PTR(
    "enable", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _bacnet_client, enable, 0, CYAML_UNLIMITED),

  CYAML_FIELD_SEQUENCE_COUNT(
    "commands", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _bacnet_client, commands, n_commands,
    &string_ptr_schema, 0, CYAML_UNLIMITED),

  CYAML_FIELD_SEQUENCE_COUNT(
    "tokens", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _bacnet_client, tokens, n_tokens,
    &string_ptr_schema, 0, CYAML_UNLIMITED),

  CYAML_FIELD_STRING_PTR(
    "filter_objects", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _bacnet_client, filter_objects, 0, CYAML_UNLIMITED),

  CYAML_FIELD_END
};

static const cyaml_schema_field_t config_fields_schema[] = {
  CYAML_FIELD_STRING_PTR(
    "server_name", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, server_name, 0, CYAML_UNLIMITED),

  CYAML_FIELD_STRING_PTR(
    "device_id", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, device_id, 0, CYAML_UNLIMITED),

  CYAML_FIELD_STRING_PTR(
    "iface", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, iface, 0, CYAML_UNLIMITED),

  CYAML_FIELD_UINT(
    "port", CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, port),

  CYAML_FIELD_UINT(
    "bi_max", CYAML_FLAG_DEFAULT | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, bi_max),

  CYAML_FIELD_UINT(
    "bo_max", CYAML_FLAG_DEFAULT | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, bo_max),

  CYAML_FIELD_UINT(
    "bv_max", CYAML_FLAG_DEFAULT | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, bv_max),

  CYAML_FIELD_UINT(
    "ai_max", CYAML_FLAG_DEFAULT | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, ai_max),

  CYAML_FIELD_UINT(
    "ao_max", CYAML_FLAG_DEFAULT | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, ao_max),

  CYAML_FIELD_UINT(
    "av_max", CYAML_FLAG_DEFAULT | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, av_max),

  CYAML_FIELD_UINT(
    "msi_max", CYAML_FLAG_DEFAULT | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, msi_max),

  CYAML_FIELD_UINT(
    "mso_max", CYAML_FLAG_DEFAULT | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, mso_max),

  CYAML_FIELD_UINT(
    "msv_max", CYAML_FLAG_DEFAULT | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, msv_max),

  CYAML_FIELD_MAPPING_PTR(
    "mqtt", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, mqtt, mqtt_fields_schema),

  CYAML_FIELD_SEQUENCE_COUNT(
    "objects", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, objects, n_objects,
    &string_ptr_schema, 0, CYAML_UNLIMITED),

  CYAML_FIELD_SEQUENCE_COUNT(
    "properties", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, properties, n_properties,
    &string_ptr_schema, 0, CYAML_UNLIMITED),

  CYAML_FIELD_MAPPING_PTR(
    "bacnet_client", CYAML_FLAG_POINTER | CYAML_FLAG_OPTIONAL,
    struct _bacnet_config, bacnet_client, bacnet_client_fields_schema),

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
  .flags = CYAML_CFG_IGNORE_UNKNOWN_KEYS
};


/*
 * Load default configs.
 */
void load_default_settings(void)
{
  if (bacnet_config) {
    if (!bacnet_config->server_name) {
      bacnet_config->server_name = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
      strcpy(bacnet_config->server_name, "My Bacnet Server");
    }

    if (!bacnet_config->device_id) {
      bacnet_config->device_id = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
      strcpy(bacnet_config->device_id, "1234");
    }

    if (!bacnet_config->port) {
      bacnet_config->port = 47808;
    }

    if (!bacnet_config->bi_max) {
      bacnet_config->bi_max = 10;
    }

    if (!bacnet_config->bo_max) {
      bacnet_config->bo_max = 10;
    }

    if (!bacnet_config->bv_max) {
      bacnet_config->bv_max = 20;
    }

    if (!bacnet_config->ai_max) {
      bacnet_config->ai_max = 10;
    }

    if (!bacnet_config->ao_max) {
      bacnet_config->ao_max = 10;
    }

    if (!bacnet_config->av_max) {
      bacnet_config->av_max = 20;
    }

    if (!bacnet_config->msi_max) {
      bacnet_config->msi_max = 20;
    }

    if (!bacnet_config->mso_max) {
      bacnet_config->mso_max = 20;
    }

    if (!bacnet_config->msv_max) {
      bacnet_config->msv_max = 20;
    }

    if (!bacnet_config->mqtt) {
      bacnet_config->mqtt = malloc(sizeof(struct _mqtt));
      memset(bacnet_config->mqtt, 0, sizeof(struct _mqtt));
      bacnet_config->mqtt->retry_limit = 3;
      bacnet_config->mqtt->retry_interval = 5;
    }

    if (!bacnet_config->mqtt->broker_ip) {
      bacnet_config->mqtt->broker_ip = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
      strcpy(bacnet_config->mqtt->broker_ip, "127.0.0.1");
    }

    if (!bacnet_config->mqtt->broker_port) {
      bacnet_config->mqtt->broker_port = 1883;
    }

    if (!bacnet_config->mqtt->debug) {
      bacnet_config->mqtt->debug = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
      strcpy(bacnet_config->mqtt->debug, "false");
    }

    if (!bacnet_config->mqtt->enable) {
      bacnet_config->mqtt->enable = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
      strcpy(bacnet_config->mqtt->enable, "true");
    }

    if (!bacnet_config->mqtt->write_via_subscribe) {
      bacnet_config->mqtt->write_via_subscribe = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
      strcpy(bacnet_config->mqtt->write_via_subscribe, "true");
    }

    if (!bacnet_config->mqtt->retry_enable) {
      bacnet_config->mqtt->retry_enable = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
      strcpy(bacnet_config->mqtt->retry_enable, "true");
    }

    if (!bacnet_config->objects) {
      char *obj_names[10] = { "device", "ai", "ao", "av", "bi", "bo", "bv", "msi", "mso", "msv" };
      bacnet_config->n_objects = 10;
      bacnet_config->objects = malloc(sizeof(char*) * bacnet_config->n_objects);
      for (int i = 0; i < bacnet_config->n_objects; i++) {
        bacnet_config->objects[i] = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
        strcpy(bacnet_config->objects[i], obj_names[i]);
      }
    }

    if (!bacnet_config->properties) {
      char *prop_names[3] = { "name", "pv", "pri" };
      bacnet_config->n_properties = 3;
      bacnet_config->properties = malloc(sizeof(char*) * bacnet_config->n_properties);
      for (int i = 0; i < bacnet_config->n_properties; i++) {
        bacnet_config->properties[i] = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
        strcpy(bacnet_config->properties[i], prop_names[i]);
      }
    }

    if (!bacnet_config->bacnet_client) {
      bacnet_config->bacnet_client= malloc(sizeof(struct _bacnet_client));
      memset(bacnet_config->bacnet_client, 0, sizeof(struct _bacnet_client));
    }

    if (!bacnet_config->bacnet_client->debug) {
      bacnet_config->bacnet_client->debug = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
      strcpy(bacnet_config->bacnet_client->debug, "false");
    }

    if (!bacnet_config->bacnet_client->enable) {
      bacnet_config->bacnet_client->enable = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
      strcpy(bacnet_config->bacnet_client->enable, "true");
    }

    if (!bacnet_config->bacnet_client->commands) {
      char *cmd_names[3] = { "whois", "read_value", "write_value" };
      bacnet_config->bacnet_client->n_commands = 3;
      bacnet_config->bacnet_client->commands = malloc(sizeof(char*) * bacnet_config->bacnet_client->n_commands);
      for (int i = 0; i < bacnet_config->bacnet_client->n_commands; i++) {
        bacnet_config->bacnet_client->commands[i] = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
        strcpy(bacnet_config->bacnet_client->commands[i], cmd_names[i]);
      }
    }

    if (!bacnet_config->bacnet_client->filter_objects) {
      bacnet_config->bacnet_client->filter_objects = malloc(sizeof(char) * MAX_YAML_STR_VALUE_LENGTH);
      strcpy(bacnet_config->bacnet_client->filter_objects, "true");
    }
  }
}


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

  pEnv = getenv("BACNET_CLIENT_DISABLE");
  if (pEnv) {
    yaml_config_bacnet_client_disable = true;
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

  load_default_settings();

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
  int i;

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
  printf("YAML Config: msi_max: %d\n", bacnet_config->msi_max);
  printf("YAML Config: mso_max: %d\n", bacnet_config->mso_max);
  printf("YAML Config: msv_max: %d\n", bacnet_config->msv_max);
  if (bacnet_config->mqtt) {
    printf("YAML Config: mqtt->broker_ip: %s\n", (bacnet_config->mqtt->broker_ip) ?
      bacnet_config->mqtt->broker_ip : "null");
    printf("YAML Config: mqtt->broker_port: %d\n", bacnet_config->mqtt->broker_port);
    printf("YAML Config: mqtt->debug: %s\n", (bacnet_config->mqtt->debug) ?
      bacnet_config->mqtt->debug: "null");
    printf("YAML Config: mqtt->enable: %s\n", (bacnet_config->mqtt->enable) ?
      bacnet_config->mqtt->enable: "null");
    printf("YAML Config: mqtt->write_via_subscribe: %s\n", (bacnet_config->mqtt->write_via_subscribe) ?
      bacnet_config->mqtt->write_via_subscribe: "null");
    printf("YAML Config: mqtt->retry_enable: %s\n", (bacnet_config->mqtt->retry_enable) ?
      bacnet_config->mqtt->retry_enable: "null");
    printf("YAML Config: mqtt->retry_limit: %d\n", bacnet_config->mqtt->retry_limit);
    printf("YAML Config: mqtt->retry_interval: %d\n", bacnet_config->mqtt->retry_interval);
  }

  if (bacnet_config->n_objects > 0) {
    printf("YAML Config: Objects\n");
  
    for(i = 0; i < bacnet_config->n_objects; i++) {
      printf("[%d]: %s\n", i, bacnet_config->objects[i]);
    }
  }

  if (bacnet_config->n_properties> 0) {
    printf("YAML Config: Properties\n");

    for(i = 0; i < bacnet_config->n_properties; i++) {
      printf("[%d]: %s\n", i, bacnet_config->properties[i]);
    }
  }

  if (bacnet_config->bacnet_client) {
    printf("YAML Config: bacnet_client->debug: %s\n", (bacnet_config->bacnet_client->debug) ?
      bacnet_config->bacnet_client->debug: "null");
    printf("YAML Config: bacnet_client->enable: %s\n", (bacnet_config->bacnet_client->enable) ?
      bacnet_config->bacnet_client->enable: "null");

    if (bacnet_config->bacnet_client->n_commands > 0) {
      printf("YAML Config: Bacnet Client Commands\n");

      for(i = 0; i < bacnet_config->bacnet_client->n_commands; i++) {
        printf("[%d]: %s\n", i, bacnet_config->bacnet_client->commands[i]);
      }
    }

    if (bacnet_config->bacnet_client->n_tokens > 0) {
      printf("YAML Config: Bacnet Client Tokens\n");

      for(i = 0; i < bacnet_config->bacnet_client->n_tokens; i++) {
        printf("[%d]: %s\n", i, bacnet_config->bacnet_client->tokens[i]);
      }
    }

    printf("YAML Config: bacnet_client->filter_objects: %s\n", (bacnet_config->bacnet_client->filter_objects) ?
      bacnet_config->bacnet_client->filter_objects: "null");
  }
}


/*
 * Get server name.
 */
char *yaml_config_server_name(void)
{
  return(bacnet_config->server_name);
}


/*
 * Get device ID.
 */
char *yaml_config_service_id(void)
{
  return(bacnet_config->device_id);
}


/*
 * Get interface name.
 */
char *yaml_config_iface(void)
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
 * Get MSI max.
 */
int yaml_config_msi_max(void)
{
  return(bacnet_config->msi_max);
}


/*
 * Get MSO max.
 */
int yaml_config_mso_max(void)
{
  return(bacnet_config->mso_max);
}


/*
 * Get MSV max.
 */
int yaml_config_msv_max(void)
{
  return(bacnet_config->msv_max);
}


/*
 * Get MQTT Broker IP.
 */
char *yaml_config_mqtt_broker_ip(void)
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
 * Get MQTT write via subscribe flag.
 */
int yaml_config_mqtt_write_via_subscribe(void)
{
  return ((bacnet_config->mqtt && bacnet_config->mqtt->write_via_subscribe &&
    !strcmp(bacnet_config->mqtt->write_via_subscribe, "true")) ? true : false);
}


/*
 * Get MQTT connect retry enable flag.
 */
int yaml_config_mqtt_connect_retry(void)
{
  return ((bacnet_config->mqtt && bacnet_config->mqtt->retry_enable &&
    !strcmp(bacnet_config->mqtt->retry_enable, "true")) ? true : false);
}


/*
 * Get MQTT retry limit.
 */
int yaml_config_mqtt_connect_retry_limit(void)
{
  return(bacnet_config->mqtt->retry_limit);
}


/*
 * Get MQTT retry interval.
 */
int yaml_config_mqtt_connect_retry_interval(void)
{
  return(bacnet_config->mqtt->retry_interval);
}


/*
 * Get objects.
 */
char **yaml_config_objects(int *length)
{
  *length = bacnet_config->n_objects;
  return(bacnet_config->objects);
}


/*
 * Check if object is supported.
 */
int is_object_supported(char *obj)
{
  for (int i = 0; i < bacnet_config->n_objects; i++) {
    if (!strcmp(obj, bacnet_config->objects[i])) {
      return(true);
    }
  }

  return(false);
}


/*
 * Get properties.
 */
char **yaml_config_properties(int *length)
{
  *length = bacnet_config->n_properties;
  return(bacnet_config->properties);
}


/*
 * Get bacnet client debug flag.
 */
int yaml_config_bacnet_client_debug(void)
{ 
  return ((bacnet_config->bacnet_client && bacnet_config->bacnet_client->debug &&
    !strcmp(bacnet_config->bacnet_client->debug, "true")) ? true : false);
}


/*
 * Get bacnet client enable flag.
 */
int yaml_config_bacnet_client_enable(void)
{ 
  if (yaml_config_bacnet_client_disable) {
    return(false);
  }
  
  return ((bacnet_config->bacnet_client && bacnet_config->bacnet_client->enable &&
    !strcmp(bacnet_config->bacnet_client->enable, "true")) ? true : false);
}


/*
 * Get bacnet client commands.
 */
char **yaml_config_bacnet_client_commands(int *length)
{
  if (bacnet_config->bacnet_client == NULL) {
    *length = 0;
    return(NULL);
  }

  *length = bacnet_config->bacnet_client->n_commands;
  return(bacnet_config->bacnet_client->commands);
}


/*
 * Get bacnet client tokens.
 */
char **yaml_config_bacnet_client_tokens(int *length)
{
  if (bacnet_config->bacnet_client == NULL) {
    *length = 0;
    return(NULL);
  }

  *length = bacnet_config->bacnet_client->n_tokens;
  return(bacnet_config->bacnet_client->tokens);
}


/*
 * Get bacnet client whois program.
 */
char *yaml_config_bacnet_client_whois_program(void)
{
  if (yaml_config_bacnet_client_disable) {
    return(NULL);
  }

  return ((bacnet_config->bacnet_client) ? bacnet_config->bacnet_client->whois_program : NULL);
}

/*
 * Get bacnet client read program.
 */
char *yaml_config_bacnet_client_read_program(void)
{ 
  if (yaml_config_bacnet_client_disable) {
    return(NULL);
  }
  
  return ((bacnet_config->bacnet_client) ? bacnet_config->bacnet_client->read_program : NULL);
}


/*
 * Get bacnet client write program.
 */
char *yaml_config_bacnet_client_write_program(void)
{ 
  if (yaml_config_bacnet_client_disable) {
    return(NULL);
  }
  
  return ((bacnet_config->bacnet_client) ? bacnet_config->bacnet_client->write_program : NULL);
}


/*
 * Get bacnet client filter objects flag.
 */
int yaml_config_bacnet_client_filter_objects(void)
{
  return ((bacnet_config->bacnet_client && bacnet_config->bacnet_client->filter_objects &&
    !strcmp(bacnet_config->bacnet_client->filter_objects, "true")) ? true : false);
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

