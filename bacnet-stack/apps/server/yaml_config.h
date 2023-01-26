#ifndef YAML_CONFIG_H
#define YAML_CONFIG_H

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

  int yaml_config_init(void);

  void yaml_config_dump(void);

  const char *yaml_config_server_name(void);

  const char *yaml_config_service_id(void);

  const char *yaml_config_iface(void);

  int yaml_config_port(void);

  int yaml_config_bi_max(void);

  int yaml_config_bo_max(void);

  int yaml_config_bv_max(void);

  int yaml_config_ai_max(void);

  int yaml_config_ao_max(void);

  int yaml_config_av_max(void);

  const char *yaml_config_mqtt_broker_ip(void);

  int yaml_config_mqtt_broker_port(void);

  int yaml_config_mqtt_debug(void);

  int yaml_config_mqtt_enable(void);

  int yaml_config_mqtt_write_via_subscribe(void);

  int yaml_config_mqtt_connect_retry(void);

  int yaml_config_mqtt_connect_retry_limit(void);

  int yaml_config_mqtt_connect_retry_interval(void);

  const char **yaml_config_objects(int *length);

  const char **yaml_config_properties(int *length);

  int yaml_config_bacnet_client_debug(void);

  int yaml_config_bacnet_client_enable(void);

  const char **yaml_config_bacnet_client_commands(int *length);

  const char *yaml_config_bacnet_client_whois_program(void);

  const char *yaml_config_bacnet_client_read_program(void);

  const char *yaml_config_bacnet_client_write_program(void);

  int yaml_config_cleanup(void);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif
