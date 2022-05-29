# bacnet-server-c
This is a fork of [Bacnet](https://github.com/bacnet-stack/bacnet-stack). This implementation includes an MQTT client sub-system that can used by the Bacnet Server to publish an MQTT message to an MQTT broker for the following conditions:
1. A write/set operation is performed on the server's object properties.
2. An new event is triggered or state changes.

MQTT Library
--
The MQTT C libary is the Eclipse Paho C Client Library for MQTT protocol. The source can be downloaded from https://github.com/eclipse/paho.mqtt.c. Please follow the install instructions to install the library.

Integrating the MQTT Client Library
--
In the server Makefile, change or include the following.
```
--
MQTT_SRC = mqtt_client.c
--
OBJS += ${SRC:.c=.o} ${MQTT_SRC:.c=.o}

MQTT_CFLAGS = -DMQTT -DMQTT_PUBLISH -I/usr/local/include -I${BACNET_SRC_DIR}/../apps/server
MQTT_LFLAGS = -L/usr/local/lib -lpaho-mqtt3c
--
${TARGET_BIN}: ${OBJS} Makefile ${BACNET_LIB_TARGET}
       ${CC} ${PFLAGS} ${OBJS} ${LFLAGS} ${MQTT_LFLAGS} -o $@
--
.c.o:
       ${CC} -c ${CFLAGS} ${MQTT_CFLAGS} $*.c -o $@
--
```

MQTT Client Sub-System
--
The source files are currently under the Bacnet server module (app/server). Include mqtt_client.h to use the MQTT client sub-system.
```
#if defined(MQTT)
#include "mqtt_client.h"
#endif /* defined(MQTT) */
```

To initialize the MQTT client sub-system, call mqtt_client_init().
```
#if defined(MQTT)
    mqtt_client_init();
#endif /* defined(MQTT) */
```

To shutdown/clean-up the MQTT client sub-system, call mqtt_client_shutdown().
```
#if defined(MQTT)
    mqtt_client_shutdown();
#endif /* defined(MQTT) */
```

To publish an MQTT message, call mqtt_publish_topic() with the pre-defined topic ID (see topic and object/property-name mapping section) and value (either a C-string or a Bacnet string).
```
#if defined(MQTT)
                    mqtt_publish_topic(DEVICE_OBJECT_NAME_TOPIC_ID,
                      MQTT_TOPIC_VALUE_BACNET_STRING, &value.type.Character_String);
#endif /* defined(MQTT) */
```

Topic and Object/Property-Name Mapping
--
Topic and Object/Property-Name mappings are pre-defined. Each topic has an ID and defined in apps/server/mqtt_client.h.
```
#define DEVICE_OBJECT_NAME_TOPIC_ID            0
#define BINARY_OUTPUT_OBJECT_NAME_TOPIC_ID     1
#define TOPIC_ID_MAX                           2
```

The textual names of the Object/Property-Name and their Topic ID mappings are defined in apps/server/mqtt_client.c.
```
/* topic and object/property mappings */
static topic_mapping_t topic_mappings[] = {
  { DEVICE_OBJECT_NAME_TOPIC_ID,
    "device_object_name" },
  { BINARY_OUTPUT_OBJECT_NAME_TOPIC_ID,
    "binary_output_object_name" },
  { TOPIC_ID_MAX, NULL }
};
```

New ones much be added before the mapping with the topic ID TOPIC_ID_MAX.

Topic Format
---
The format of the MQTT topic published to MQTT broker is **/<device_id>/object-property-name**.
For example the topic of an MQTT publish message sent when updating the Bacnet device name of a Bacnet server with a device ID 1234 would be:
```
/1234/device_object_name
```
The topic for Bacnet Binary Output Propery Name would be:
```
/1234/binary_output_object_name
```

MQTT Broker IP/Port
--
The default MQTT broker IP and port is 127.0.0.1 and 1883 respectively.
To set the MQTT broker IP to a different IP, set the IP in the environment variable DEFAULT_MQTT_BROKER_IP.
To set the MQTT broker port number to a different port, set the port in the environment variable MQTT_BROKER_PORT.

For example:
```
MQTT_BROKER_IP=10.20.30.123 MQTT_BROKER_PORT=11883 ./bin/bacserv
```

Compiling The Bacnet Server
---
Download or clone the repo.
```
git clone git@github.com:NubeIO/bacnet-server-c.git
cd bacnet-server-c/bacnet-stack
make clean all
```

Running The Bacnet Server
---
On the bacnet-stack directory, run bin/bacserv without any option. The server will connect to the local MQTT broker with the IP address and port number 127.0.0.1 and 1883.
```
./bin/bacserv
```

To use an external MQTT broker, set the environment variable MQTT_BROKER_IP for the IP address and MQTT_BROKER_PORT for the port number.
```
MQTT_BROKER_IP=104.219.42.42 MQTT_BROKER_PORT=11883 ./bin/bacserv 1234
```

Sample Get/Set Commands
---
Get and set the device propery name. The client and server are on the same machine here. If the client is on a different machine, you can omit the "mac" option.

Get the device object property name.
```
./bin/bacrp 1234 8 1234 77 --mac 10.104.0.11:47808
"SimpleServer"
```

To change the device object property name to "My Simple Server".
```
./bin/bacwp 1234 8 1234 77 0 -1 7 "My Simple Server" D 10.104.0.11:47808
WriteProperty Acknowledged!
```

Confirm the change was made.
```
./bin/bacrp 1234 8 1234 77 --mac 10.104.0.11:47808
"My Simple Server"
```

Get and set the binary output object propery name.
Get the binary output object propery name.
```
./bin/bacrp 1234 4 1 77 --mac 10.104.0.11:47808
"BINARY OUTPUT"
```

To change the binary output object property name to "MY BINARY OUTPUT".
```
./bin/bacwp 1234 4 1 77 0 -1 7 "MY BINARY OUTPUT" D 10.104.0.11:47808
WriteProperty Acknowledged!
```

Confirm the change was made.
```
./bin/bacrp 1234 4 1 77 --mac 10.104.0.11:47808
"MY BINARY OUTPUT"
```

Testing the MQTT Publish/Subscribe Message
---
You can use any MQTT protocol compliant client. I used the [mqtt-cli](https://hivemq.github.io/mqtt-cli).
To subscribe to changes to the device object and binary output object property name of the bacnet instance with device ID 1234.
```
mqtt sub -h <mqtt broker ip address> -t "/1234/binary_output_object_name" -t "/1234/device_object_name" -T
```

The client will receive a publish message when a change is made to the device object and binary output object propery name with the new value in the message.
```
mqtt sub -h <mqtt broker ip address> -t "/1234/binary_output_object_name" -t "/1234/device_object_name" -T

/1234/device_object_name: My Simple Server
/1234/binary_output_object_name: MY BINARY OUTPUT
```

