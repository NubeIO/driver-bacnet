# bacnet-server-c
This is a fork of [Bacnet](https://github.com/bacnet-stack/bacnet-stack). This implementation includes an MQTT client sub-system that can used by the Bacnet Server to publish an MQTT message to an MQTT broker for the following conditions:
1. A write/set operation is performed on the server's object properties.
2. An new event is triggered or state changes.


# driver-bacnet local build

# install devel tools
apt update -qq && apt install build-essential net-tools git libssl-dev cmake -y

# create work dir
mkdir -p <work_dir> && cd <work_dir>

# install mqtt lib
git clone https://github.com/eclipse/paho.mqtt.c paho.mqtt.c && cd paho.mqtt.c && cmake -DPAHO_BUILD_STATIC=TRUE && make install

# install json lib
cd <work_dir>
git clone https://github.com/json-c/json-c json-c && cd json-c && mkdir json-c-build && cmake ../json-c && make && make install

# install yaml-dev and cyaml lib
apt-get install libyaml-dev -y
cd <work_dir>
git clone https://github.com/tlsa/libcyaml.git && cd libcyaml && make install VARIANT=release
find / -name libyaml.a -print -exec cp -fR {} /usr/lib

# compile driver-bacnet
cd <work_dir>
git clone git@github.com:NubeIO/driver-bacnet.git && cd driver-bacnet/bacnet-stack && make clean all

# run driver-bacnet , specify the folder of the config file in "g" env variable
g=/data/bacnet-server-driver s=config.yml ./bin/bacserv


Running BACnet Server
--
Configure [config](config.example.yml) on location: `<g>/config/<s>` 

Start Command:
```
g=<path for global_directory> s=config.yml ./app
```

For example:
```
g=/data/bacnet-server-c s=config.yml ./app 
```
on your pc
```
g=/home/user/code/bacnet s=config.yml ./app-amd64 
```

Environment Variables
--
1. MQTT_DEBUG - Set to true to emit MQTT requests and responses debugging lines
2. YAML_CONFIG_DEBUG - Set to true to emit YAML config debugging lines
3. BACNET_IP_DEBUG - Set to true to emit IP address/port and BACnet message headers debugging lines
4. BACNET_IFACE - Set the interface the bacnet server will bind into. Override the setting in the configuration file.
5. BACNET_IP_PORT - Set the listening port of the bacnet server. Override the default value (47808) and the setting in the configuration file.
6. MQTT_BROKER_IP - Set the MQTT broker IP address. Override the setting in the configuration file.
7. MQTT_CLIENT_ID - Set MQTT client ID to a specific value. The default value is "\<hostname\>-mqtt-cid". Where <i>hostname</i> is the hostname of the device.

For example:
```
MQTT_DEBUG=true YAML_CONFIG_DEBUG=true g=/data/bacnet-server-c s=config.yml ./app
```


Updating BACnet Object Property Values Via MQTT
--
The values of the following object properties can be updated via MQTT publish command:
1. Object Name (name) - applies to analog input (ai), analog output (ao), analog value (av), binary input (bi), binary output (bo) and binary value (bv) objects
2. Present Value (pv) - applies to analog input (ai), analog output (ao), analog value (av), binary input (bi), binary output (bo) and binary value (bv) objects
3. Priority Array (pri) - applies to analog output (ao), analog value (av), binary output (bo) and binary value (bv) objects

The value of the publish message must be in JSON using the following format:
```
{"value" : "value here", "uuid" : "uuid here"}
```

Object Name topic format:
```
bacnet/object/address/write/name
```

Write "ao_name1" into analog object (ao) name at instance (address) 1
```
topic: bacnet/ao/1/write/name
json payload: {"value" : "ao_name1", "uuid" : "123456"}
```

Present Value topic format:
```
bacnet/object/address/write/pv
```

Write 10.50 to into analog object (ao) present value at instance (address) 1
```
topic: bacnet/ao/1/write/pv
json payload: {"value" : "10.50", "uuid" : "123456"}
```

Priority Array topic formats:
```
bacnet/object/address/write/pri/priority_index
bacnet/object/address/write/pri/priority_index/all
```

Write 50.20 into analog object (ao) at instance (address) 1 at priority index 10
```
topic: bacnet/ao/1/write/pri/10
json payload: {"value" : "10.50", "uuid" : "123456"}
```

Write 99.99 into analog object (ao) at instance (address) 1 to all priority slots
```
topic: bacnet/ao/1/write/pri/16/all
json payload: {"value" : "99.99", "uuid" : "123456"}
```

Reset priority slots #10 of analog object (ao) at instance (address) 1. Reset applies only to analog ouput, analog value, binary output and binary value objects.
```
topic: bacnet/ao/1/write/pri/10
json payload: {"value" : "null", "uuid" : "123456"}
```

Reset all priority slots of analog object (ao) at instance (address) 1 
```
topic: bacnet/ao/1/write/pri/16/all
json payload: {"value" : "null", "uuid" : "123456"}
```


Bacnet Master
--
The driver bacnet when acting as a master will receive MQTT commands for another device and sends results over MQTT.

The target device is specified using the <i>mac</i> attribute in the MQTT request payload. The value must be an IP address and port pair like below.
```
{"objectType":"1","objectInstance":"1","property":"87","deviceInstance":"5678","mac":"10.104.0.11:47900"}
```

The driver bacnet supports the following 5 commands:
1. read_value
2. write_value
3. whois
4. point_discovery
5. point_list


Read Value Command
--
The <i>read_value</i> command is used to retrieve the value of an object property.

<b>Request</b><br/>
Example 1: Fetch the point value of analog-output object @1 of device 1234
```
Command topic: bacnet/cmd/read_value
JSON paylod: {"objectType":"1","objectInstance":"1","property":"85","deviceInstance":"1234","mac":"10.104.0.11:47808", "dnet": "0", "dadr": "0", "txn_source": "ff", "txn_number": "23b00e85d1fb-test", "timeout": 2}
```

Example 2: Fetch the priority array values of analog-output object @1 of device 1234
```
Command topic: bacnet/cmd/read_value
JSON payload: {"objectType":"1","objectInstance":"1","property":"87","deviceInstance":"1234","mac":"10.104.0.11:47808", "dnet": "0", "dadr": "0", "txn_source": "ff", "txn_number": "23b00e85d1fb-test", "timeout": 2}
```
Example 3: Fetch the priority array value @3 of analog-output object @1 of device 1234
```
Command topic: bacnet/cmd/read_value
JSON payload: {"objectType":"1","objectInstance":"1","property":"87","deviceInstance":"1234","mac":"10.104.0.11:47808", "dnet": "0", "dadr": "0", "txn_source": "ff", "txn_number": "23b00e85d1fb-test", "timeout": 2, "index":"3"}
```

<br/><b>Response</b><br/>
Example 1: Fetch the point value of analog-output object @1 of device 1234
```
Command topic: bacnet/cmd_result/read_value/ao/1/pv
JSON payload: { "value" : "101.000000" , "deviceInstance" : "1234" , "dnet" : "0" , "dadr" : "0" , "mac" : "10.104.0.11:47808" , "txn_source" : "ff", "txn_number" : "23b00e85d1fb-test"}
```
Example 2: Fetch the priority array values of analog-output object @1 of device 1234
```
Command topic: bacnet/cmd_result/read_value/ao/1/pri
JSON payload: { "value" : ["101.000000","201.000000","300.000000","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","Null","30.000000"] , "deviceInstance" : "1234" , "dnet" : "0" , "dadr" : "0" , "mac" : "10.104.0.11:47808" , "txn_source" : "ff", "txn_number" : "23b00e85d1fb-test"}
```
Example 3: Fetch the priority array value @3 of analog-output object @1 of device 1234
```
Command topic: bacnet/cmd_result/read_value/ao/1/pri
JSON payload: { "value" : "300.000000" , "deviceInstance" : "1234" , "dnet" : "0" , "dadr" : "0" , "mac" : "10.104.0.11:47808" , "index" : "3" , "txn_source" : "ff", "txn_number" : "23b00e85d1fb-test"}
```

Write Value Command
--
The <i>write_value</i> command is used to update the value of an object property.

<b>Request</b><br/>
Example 1: Update the point value of analog-output object @1 of device 1234
```
Command topic: bacnet/cmd/write_value
JSON paylod: {"objectType":"1","objectInstance":"1","property":"85","deviceInstance":"1234","mac":"10.104.0.11:47808", "dnet": "0", "dadr": "0", "txn_source": "ff", "txn_number": "23b00e85d1fb-test", "timeout": 2, "value":"123"}
```
Example 2: Update the value of priority array @1 of analog-output object @1 of device 1234
```
Command topic: bacnet/cmd/write_value
JSON paylod: {"objectType":"1","objectInstance":"1","property":"85","deviceInstance":"1234","mac":"10.104.0.11:47808", "dnet": "0", "dadr": "0", "txn_source": "ff", "txn_number": "23b00e85d1fb-test", "timeout": 2, "value":"111", "priority":"1"}
```
Example 3: Update the values of priority array @1,@2 and @3 of analog-output object @1 of device 1234
```
Command topic: bacnet/cmd/write_value
JSON paylod: {"objectType":"1","objectInstance":"1","property":"85","deviceInstance":"1234","mac":"10.104.0.11:47808", "dnet": "0", "dadr": "0", "txn_source": "ff", "txn_number": "23b00e85d1fb-test", "timeout": 2, "priorityArray":{"_1":"101", "_2":"201", "_3":"301"}}
```
Example 4: Reset the values of priority array @1 and @16 of analog-output object @1 of device 1234
```
Command topic: bacnet/cmd/write_value
JSON paylod: {"objectType":"1","objectInstance":"1","property":"85","deviceInstance":"1234","mac":"10.104.0.11:47808", "dnet": "0", "dadr": "0", "txn_source": "ff", "txn_number": "23b00e85d1fb-test", "timeout": 2, "priorityArray":{"_1":"null", "_3":"null"}}
```
<br/><b>Response</b><br/>
The payload from the request message is sent in the response. When there is an error, the attribute <i>error</i> is set in the response payload.

Example 1: Update the point value of analog-output object @1 of device 1234
```
Command topic: bacnet/cmd_result/write_value/ao/1/pv
JSON payload: { "value" : "123" , "deviceInstance" : "1234" , "dnet" : "0" , "dadr" : "0" , "mac" : "10.104.0.11:47808" , "txn_source" : "ff", "txn_number" : "23b00e85d1fb-test"}
```
Example 2: Error updating the point value of analog-output object @1000 of device 1234
```
Command topic: bacnet/cmd_result/write_value/ao/1000/pv
JSON payload: { "error" : "unknown-object" , "objectType" : "1" , "objectInstance" : "1000" , "property" : "85" , "priority" : "0" , "deviceInstance" : "1234" , "mac" : "10.104.0.11:47808" , "dadr" : "0" , "dnet" : "0" , "value" : "123" , "txn_source" : "ff" , "txn_number" : "23b00e85d1fb-test" }
```

Whois Command
---
The <i>whois</i> command is used to discover the devices on the network.

<b>Request</b><br/>
Example 1: Get the list of devices on the network.
```
Command topic: bacnet/cmd/whois
JSON payload: {}
```
<b>Response</b><br/>
Example 1: Get the list of devices on the network.
```
Command topic: bacnet/cmd_result/whois
JSON payload: { "value" : [ {"device_id" : "17", "mac_address" : "192.168.15.17:47808", "snet" : "0", "sadr" : "00", "apdu" : "1476"}, {"device_id" : "0", "mac_address" : "192.168.15.222:47808", "snet" : "0", "sadr" : "00", "apdu" : "1476"}, {"device_id" : "5132", "mac_address" : "192.168.15.132:47808", "snet" : "0", "sadr" : "00", "apdu" : "1476"}, {"device_id" : "245", "mac_address" : "192.168.15.245:47808", "snet" : "0", "sadr" : "00", "apdu" : "1476"}, {"device_id" : "1", "mac_address" : "192.168.15.140:47808", "snet" : "0", "sadr" : "00", "apdu" : "480"} ] }
```

point_discovery Command
---
The <i>point_discovery</i> command is used to retrieve the list of points (object instances) of a device.

<b>Request</b><br/>
Example 1: Get the list of points of a device.
```
Command topic: bacnet/cmd/point_discovery
JSON payload: {"deviceInstance":"5678","mac":"10.104.0.11:47900"}
```
<b>Response</b><br/>
Example 1: Get the list of points of a device.
```
Command topic: bacnet/cmd_result/point_discovery
JSON payload: {"deviceInstance" : "5678", "mac" : "10.104.0.11:47900", "value" : {"objects" : ["ai-1","ai-2","ao-1","ao-2","av-1","av-2","av-3","av-4","av-5","av-6","bi-1","bi-2","bi-3","bi-4","bo-1","bo-2","bv-1","bv-2","bv-3","bv-4","bv-5","msi-1","msi-2","msi-3","msi-4","mso-1","mso-2","mso-3","mso-4","msv-1","msv-2","msv-3","msv-4"], "count" : "33"}}

```


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
Compile and build the bacnet server on your local development machine. Tested on Ubuntu 18.04.5 LTS.
```
sudo apt install build-essential net-tools git libssl-dev cmake -y
mkdir bacnet && cd bacnet
git clone https://github.com/eclipse/paho.mqtt.c paho.mqtt.c
cd paho.mqtt.c && cmake -DPAHO_BUILD_STATIC=TRUE && sudo make install
cd .. && git clone https://github.com/json-c/json-c json-c
cd json-c && mkdir -p json-c-build && cmake ../json-c && make && sudo make install
sudo apt-get install libyaml-dev -y
cd .. && git clone https://github.com/tlsa/libcyaml.git
cd libcyaml && sudo make install VARIANT=release
find / -name libyaml.a -print -exec cp -fR {} /usr/lib \;
cd .. && git clone git@github.com:NubeIO/bacnet-server-c.git
cd bacnet-server-c/bacnet-stack && make clean all
```

Running The Bacnet Server (bacserv)
---
The bacnet server binary file is at bacnet-stack/bin/bacserv. You need to specify the configuration file and folder to run the server using the "g" and "s" ENV variables like below.
```
g=/data/bacnet-server-driver s=config.yml ./bin/bacserv
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

