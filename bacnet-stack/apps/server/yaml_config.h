#ifndef YAML_CONFIG_H
#define YAML_CONFIG_H

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

  int yaml_config_init(void);

  void yaml_config_dump(void);

  char *yaml_config_server_name(void);

  char *yaml_config_service_id(void);

  char *yaml_config_iface(void);

  int yaml_config_port(void);

  int yaml_config_bi_max(void);

  int yaml_config_bo_max(void);

  int yaml_config_bv_max(void);

  int yaml_config_ai_max(void);

  int yaml_config_ao_max(void);

  int yaml_config_av_max(void);

  int yaml_config_msi_max(void);

  int yaml_config_mso_max(void);

  int yaml_config_msv_max(void);

  unsigned int yaml_config_main_proc_delay(void);

  char *yaml_config_mqtt_broker_ip(void);

  int yaml_config_mqtt_broker_port(void);

  int yaml_config_mqtt_debug(void);

  int yaml_config_mqtt_enable(void);

  int yaml_config_mqtt_write_via_subscribe(void);

  int yaml_config_mqtt_connect_retry(void);

  int yaml_config_mqtt_connect_retry_limit(void);

  int yaml_config_mqtt_connect_retry_interval(void);

  int yaml_config_mqtt_disable_persistence(void);

  int yaml_config_mqtt_old_pri_array_format(void);

  char **yaml_config_objects(int *length);

  int is_object_supported(char *obj);

  char **yaml_config_properties(int *length);

  int yaml_config_bacnet_client_debug(void);

  int yaml_config_bacnet_client_enable(void);

  char **yaml_config_bacnet_client_commands(int *length);

  char **yaml_config_bacnet_client_tokens(int *length);

  char *yaml_config_bacnet_client_whois_program(void);

  char *yaml_config_bacnet_client_read_program(void);

  char *yaml_config_bacnet_client_write_program(void);

  int yaml_config_bacnet_client_filter_objects(void);

  int yaml_config_cleanup(void);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif
