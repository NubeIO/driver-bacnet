# CHANGELOG



## [v1.0.2](https://github.com/NubeIO/driver-bacnet/tree/v1.0.2) (2024-11-08)
- Fixed device ID returned from reading property list.
- Prerequisites:
  - ROS >= 1.0.0

## [v1.0.2-rc.4](https://github.com/NubeIO/driver-bacnet/tree/v1.0.2-rc.4) (2024-10-26)
- Fixed device ID returned from reading property list.
- Prerequisites:
  - ROS >= 1.0.0

## [v1.0.2-rc.3](https://github.com/NubeIO/driver-bacnet/tree/v1.0.2-rc.3) (2024-08-08)
- Fixed duplicate MQTT publish message on write
- Prerequisites:
  - ROS >= 1.0.0

## [v1.0.2-rc.2](https://github.com/NubeIO/driver-bacnet/tree/v1.0.2-rc.2) (2024-07-19)
- Support for Multistate objects similar to Analog and Binary objects.
- Fixed incorrect JSON format when ready Multistate Output priority array.
- Prerequisites:
  - ROS >= 1.0.0

## [v1.0.2-rc.1](https://github.com/NubeIO/driver-bacnet/tree/v1.0.2-rc.1) (2024-07-07)
- Re-align debugging lines. Disabled by default.
- Prerequisites:
  - ROS >= 1.0.0

## [v1.0.1](https://github.com/NubeIO/bacnet-server-c/tree/v1.0.1) (2024-06-24)

- Issue-121: Fix for the bug when driver bacnet responds with an error to all MQTT read/write commands
- Prerequisites:
  - ROS >= 1.0.0

## [v1.0.0](https://github.com/NubeIO/bacnet-server-c/tree/v1.0.0) (2024-05-28)

- Complete persistent storage initialization before main loop.
- Public Release
- Prerequisites:
    - ROS >= 1.0.0

## v1.0.0-rc.1
- initial release

## v0.0.65-rc.2
- Support for persistent MQTT published message. Enabled by default. To disable, set disable_persistence to true in the config under mqtt segment.
- Initialize object property values (name, present value value and priority array) from last published message (persistent) when bacnet-server starts up.
- Fixed the format of the priority array value in the payload of published message into a valid JSON format from
```
"value": "{123.500000,Null,Null,Null,Null,Null,Null,Null,Null,300.000000,Null,Null,Null,Null,Null,222.300003}"
```
into
```
"value" : ["123.500000","Null","Null","Null","Null","Null","Null","Null","Null","300.000000","Null","Null","Null","Null","Null","222.300003"]
```

## [v0.0.15](https://github.com/NubeIO/bacnet-server-c/tree/v0.0.15) (2022-08-19)

- Update command c to s (setting file) as per other conventions
- Add config.example.yml
