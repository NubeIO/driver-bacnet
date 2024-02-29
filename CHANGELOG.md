# CHANGELOG

## v0.0.65-rc1
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
