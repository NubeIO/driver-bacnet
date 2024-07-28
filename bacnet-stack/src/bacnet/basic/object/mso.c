/**************************************************************************
 *
 * Copyright (C) 2006 Steve Karg <skarg@users.sourceforge.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 *********************************************************************/

/* Multi-state Output Objects - customize for your use */

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bacnet/bacdef.h"
#include "bacnet/bacdcode.h"
#include "bacnet/bacenum.h"
#include "bacnet/bacapp.h"
#include "bacnet/config.h" /* the custom stuff */
#include "bacnet/rp.h"
#include "bacnet/wp.h"
#include "bacnet/basic/object/device.h"
#include "bacnet/basic/object/mso.h"
#include "bacnet/basic/services.h"
#if defined(MQTT)
#include "MQTTClient.h"
#include "mqtt_client.h"
#endif /* defined(MQTT) */
#if defined(YAML_CONFIG)
#include "yaml_config.h"
#endif /* defined(YAML_CONFIG) */

#ifndef MAX_MULTISTATE_OUTPUTS
#define MAX_MULTISTATE_OUTPUTS 4
#endif

/* When all the priorities are level null, the present value returns */
/* the Relinquish Default value, 0 is not allowed */
#define MULTISTATE_RELINQUISH_DEFAULT 1

/* NULL part of the array */
#define MULTISTATE_NULL (255)
/* how many states? 1 to 254 states, 0 is not allowed */
#define MULTISTATE_NUMBER_OF_STATES (254)

/* Run-time Multistate Input Instances */
static int Multistate_Output_Instances = 0;

/* Here is our Priority Array.*/
static uint8_t **Multistate_Output_Level = NULL;

/* Writable */
static char **Object_Name = NULL;

/* Writable out-of-service allows others to play with our Present Value */
/* without changing the physical output */
static bool *Out_Of_Service = NULL;

/* These three arrays are used by the ReadPropertyMultiple handler */
static const int Multistate_Output_Properties_Required[] = {
    PROP_OBJECT_IDENTIFIER, PROP_OBJECT_NAME, PROP_OBJECT_TYPE,
    PROP_PRESENT_VALUE, PROP_STATUS_FLAGS, PROP_EVENT_STATE,
    PROP_OUT_OF_SERVICE, PROP_NUMBER_OF_STATES, PROP_PRIORITY_ARRAY,
    PROP_RELINQUISH_DEFAULT, -1
};

static const int Multistate_Output_Properties_Optional[] = { PROP_DESCRIPTION,
    -1 };

static const int Multistate_Output_Properties_Proprietary[] = { -1 };

void Multistate_Output_Property_Lists(
    const int **pRequired, const int **pOptional, const int **pProprietary)
{
    if (pRequired) {
        *pRequired = Multistate_Output_Properties_Required;
    }
    if (pOptional) {
        *pOptional = Multistate_Output_Properties_Optional;
    }
    if (pProprietary) {
        *pProprietary = Multistate_Output_Properties_Proprietary;
    }

    return;
}

void Multistate_Output_Init(void)
{
    unsigned i, j;
    char *pEnv;
    static bool initialized = false;

#if defined(YAML_CONFIG)
    Multistate_Output_Instances = yaml_config_mso_max();
    if (Multistate_Output_Instances == 0) {
#endif
    pEnv = getenv("MSO");
    if (pEnv) {
        Multistate_Output_Instances = atoi(pEnv);
    }
#if defined(YAML_CONFIG)
    }
#endif

    if (!initialized) {
        initialized = true;

        if (Multistate_Output_Instances > 0) {
            Multistate_Output_Level = malloc(Multistate_Output_Instances * sizeof(uint8_t *));
            Out_Of_Service = malloc(Multistate_Output_Instances  * sizeof(bool));
            memset(Out_Of_Service, 0, Multistate_Output_Instances  * sizeof(bool));
            Object_Name = malloc(Multistate_Output_Instances * sizeof(char *));
        }

        /* initialize all the analog output priority arrays to NULL */
        for (i = 0; i < Multistate_Output_Instances; i++) {
            Multistate_Output_Level[i] = malloc(BACNET_MAX_PRIORITY * sizeof(uint8_t));
            for (j = 0; j < BACNET_MAX_PRIORITY; j++) {
                Multistate_Output_Level[i][j] = MULTISTATE_NULL;
            }

            Object_Name[i] = malloc(64 * sizeof(char));
            sprintf(&Object_Name[i][0], "MULTISTATE OUTPUT %u", i + 1);
        }
    }

    return;
}

/* we simply have 0-n object instances.  Yours might be */
/* more complex, and then you need validate that the */
/* given instance exists */
bool Multistate_Output_Valid_Instance(uint32_t object_instance)
{
    if (object_instance > 0 && object_instance <= Multistate_Output_Instances) {
        return true;
    }

    return false;
}

/* we simply have 0-n object instances.  Yours might be */
/* more complex, and then count how many you have */
unsigned Multistate_Output_Count(void)
{
    return (Multistate_Output_Instances);
}

/* we simply have 0-n object instances.  Yours might be */
/* more complex, and then you need to return the instance */
/* that correlates to the correct index */
uint32_t Multistate_Output_Index_To_Instance(unsigned index)
{
    return index;
}

/* we simply have 0-n object instances.  Yours might be */
/* more complex, and then you need to return the index */
/* that correlates to the correct instance number */
unsigned Multistate_Output_Instance_To_Index(uint32_t object_instance)
{
    unsigned index = Multistate_Output_Instances;

    if (object_instance > 0 && object_instance <= Multistate_Output_Instances) {
        index = object_instance;
    }

    return index;
}

uint32_t Multistate_Output_Present_Value(uint32_t object_instance)
{
    uint32_t value = MULTISTATE_RELINQUISH_DEFAULT;
    unsigned index = 0;
    unsigned i = 0;

    index = Multistate_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Output_Instances) {
        for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
            if (Multistate_Output_Level[index - 1][i] != MULTISTATE_NULL) {
                value = Multistate_Output_Level[index - 1][i];
                break;
            }
        }
    }

    return value;
}

/* publish the values of priority array over mqtt */
void publish_mso_priority_array(uint32_t object_instance, char *uuid)
{
    unsigned value;
    char buf[1024] = {0};
    char *first = "";
    unsigned index = 0;
    unsigned i;


    index = Multistate_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Output_Instances) {
        strcpy(buf, "[");
        for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
            value = Multistate_Output_Level[index - 1][i];
            if (value == MULTISTATE_NULL) {
                sprintf(&buf[strlen(buf)], "%s\"Null\"", first);
            } else {
                sprintf(&buf[strlen(buf)], "%s\"%d\"", first, value);
            }

            first = ",";
        }

        sprintf(&buf[strlen(buf)], "]");
        if (yaml_config_mqtt_enable()) {
            mqtt_publish_topic(OBJECT_MULTI_STATE_OUTPUT, object_instance, PROP_PRIORITY_ARRAY,
                MQTT_TOPIC_VALUE_STRING, buf, uuid);
        }
    }
}

bool Multistate_Output_Present_Value_Set(
    uint32_t object_instance, unsigned value, unsigned priority, char *uuid, int bacnet_client)
{
    unsigned index = 0;
    bool status = false;

    index = Multistate_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Output_Instances) {
        if (priority && (priority <= BACNET_MAX_PRIORITY) &&
            (priority != 6 /* reserved */)) {
            Multistate_Output_Level[index - 1][priority - 1] = value;
            status = true;
#if defined(MQTT)
            if (yaml_config_mqtt_enable() && !bacnet_client) {
                if (value == MULTISTATE_NULL) {
                  mqtt_publish_topic(OBJECT_MULTI_STATE_OUTPUT, object_instance, PROP_PRESENT_VALUE,
                      MQTT_TOPIC_VALUE_STRING, "Null", uuid);
                } else {
                  mqtt_publish_topic(OBJECT_MULTI_STATE_OUTPUT, object_instance, PROP_PRESENT_VALUE,
                      MQTT_TOPIC_VALUE_INTEGER, &value, uuid);
                }

                publish_mso_priority_array(object_instance, uuid);
            }
#endif /* defined(MQTT) */
        }
    }

    return status;
}

/* note: the object name must be unique within this device */
bool Multistate_Output_Object_Name(
    uint32_t object_instance, BACNET_CHARACTER_STRING *object_name)
{
    unsigned index = 0;
    bool status = false;

    index = Multistate_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Output_Instances) {
        status = characterstring_init_ansi(object_name, Object_Name[index - 1]);
    }

    return status;
}

bool Multistate_Output_Object_Name_Set(uint32_t object_instance,
    BACNET_CHARACTER_STRING *char_string,
    BACNET_ERROR_CLASS *error_class,
    BACNET_ERROR_CODE *error_code,
    char *uuid, int bacnet_client)
{
    unsigned index = 0; /* offset from instance lookup */
    size_t length = 0;
    uint8_t encoding = 0;
    bool status = false; /* return value */
    size_t obj_name_size = 64 * sizeof(char);

    index = Multistate_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Output_Instances) {
        length = characterstring_length(char_string);
        if (length <= obj_name_size) {
            encoding = characterstring_encoding(char_string);
            if (encoding == CHARACTER_UTF8) {
                status = characterstring_ansi_copy(Object_Name[index - 1],
                    obj_name_size, char_string);
                if (!status) {
                    *error_class = ERROR_CLASS_PROPERTY;
                    *error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                } else {
#if defined(MQTT)
                  if (yaml_config_mqtt_enable() && !bacnet_client) {
                    mqtt_publish_topic(OBJECT_MULTI_STATE_OUTPUT, object_instance, PROP_OBJECT_NAME,
                      MQTT_TOPIC_VALUE_BACNET_STRING, char_string, uuid);
                  }
#endif /* defined(MQTT) */
                }
            } else {
                *error_class = ERROR_CLASS_PROPERTY;
                *error_code = ERROR_CODE_CHARACTER_SET_NOT_SUPPORTED;
            }
        } else {
            *error_class = ERROR_CLASS_PROPERTY;
            *error_code = ERROR_CODE_NO_SPACE_TO_WRITE_PROPERTY;
        }
    }

    return status;
}

bool Multistate_Output_Out_Of_Service(uint32_t instance)
{
    unsigned index = 0;
    bool oos_flag = false;

    index = Multistate_Output_Instance_To_Index(instance);
    if (index > 0 && index <= Multistate_Output_Instances) {
        oos_flag = Out_Of_Service[index - 1];
    }

    return oos_flag;
}

void Multistate_Output_Out_Of_Service_Set(uint32_t instance, bool oos_flag)
{
    unsigned index = 0;

    index = Multistate_Output_Instance_To_Index(instance);
    if (index > 0 && index <= Multistate_Output_Instances) {
        Out_Of_Service[index - 1] = oos_flag;
    }
}

/* return apdu len, or BACNET_STATUS_ERROR on error */
int Multistate_Output_Read_Property(BACNET_READ_PROPERTY_DATA *rpdata)
{
    int len = 0;
    int apdu_len = 0; /* return value */
    BACNET_BIT_STRING bit_string;
    BACNET_CHARACTER_STRING char_string;
    uint32_t present_value = 0;
    unsigned object_index = 0;
    unsigned i = 0;
    bool state = false;
    uint8_t *apdu = NULL;

    if ((rpdata == NULL) || (rpdata->application_data == NULL) ||
        (rpdata->application_data_len == 0)) {
        return 0;
    }
    apdu = rpdata->application_data;
    switch (rpdata->object_property) {
        case PROP_OBJECT_IDENTIFIER:
            apdu_len = encode_application_object_id(
                &apdu[0], OBJECT_MULTI_STATE_OUTPUT, rpdata->object_instance);
            break;
            /* note: Name and Description don't have to be the same.
               You could make Description writable and different */
        case PROP_OBJECT_NAME:
        case PROP_DESCRIPTION:
            Multistate_Output_Object_Name(
                rpdata->object_instance, &char_string);
            apdu_len =
                encode_application_character_string(&apdu[0], &char_string);
            break;
        case PROP_OBJECT_TYPE:
            apdu_len = encode_application_enumerated(
                &apdu[0], OBJECT_MULTI_STATE_OUTPUT);
            break;
        case PROP_PRESENT_VALUE:
            present_value =
                Multistate_Output_Present_Value(rpdata->object_instance);
            apdu_len = encode_application_unsigned(&apdu[0], present_value);
            break;
        case PROP_STATUS_FLAGS:
            /* note: see the details in the standard on how to use these */
            bitstring_init(&bit_string);
            bitstring_set_bit(&bit_string, STATUS_FLAG_IN_ALARM, false);
            bitstring_set_bit(&bit_string, STATUS_FLAG_FAULT, false);
            bitstring_set_bit(&bit_string, STATUS_FLAG_OVERRIDDEN, false);
            state = Multistate_Output_Out_Of_Service(rpdata->object_instance);
            bitstring_set_bit(&bit_string, STATUS_FLAG_OUT_OF_SERVICE, state);
            apdu_len = encode_application_bitstring(&apdu[0], &bit_string);
            break;
        case PROP_EVENT_STATE:
            /* note: see the details in the standard on how to use this */
            apdu_len =
                encode_application_enumerated(&apdu[0], EVENT_STATE_NORMAL);
            break;
        case PROP_OUT_OF_SERVICE:
            state = Multistate_Output_Out_Of_Service(rpdata->object_instance);
            apdu_len = encode_application_boolean(&apdu[0], state);
            break;
        case PROP_PRIORITY_ARRAY:
            /* Array element zero is the number of elements in the array */
            if (rpdata->array_index == 0) {
                apdu_len =
                    encode_application_unsigned(&apdu[0], BACNET_MAX_PRIORITY);
                /* if no index was specified, then try to encode the entire list
                 */
                /* into one packet. */
            } else if (rpdata->array_index == BACNET_ARRAY_ALL) {
                object_index = Multistate_Output_Instance_To_Index(
                    rpdata->object_instance);
                for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
                    /* FIXME: check if we have room before adding it to APDU */
                    if (Multistate_Output_Level[object_index - 1][i] ==
                        MULTISTATE_NULL) {
                        len = encode_application_null(&apdu[apdu_len]);
                    } else {
                        present_value =
                            Multistate_Output_Level[object_index - 1][i];
                        len = encode_application_unsigned(
                            &apdu[apdu_len], present_value);
                    }
                    /* add it if we have room */
                    if ((apdu_len + len) < MAX_APDU) {
                        apdu_len += len;
                    } else {
                        rpdata->error_code =
                            ERROR_CODE_ABORT_SEGMENTATION_NOT_SUPPORTED;
                        apdu_len = BACNET_STATUS_ABORT;
                        break;
                    }
                }
            } else {
                object_index = Multistate_Output_Instance_To_Index(
                    rpdata->object_instance);
                if (rpdata->array_index <= BACNET_MAX_PRIORITY) {
                    if (Multistate_Output_Level[object_index - 1]
                                               [rpdata->array_index - 1] ==
                        MULTISTATE_NULL) {
                        apdu_len = encode_application_null(&apdu[0]);
                    } else {
                        present_value =
                            Multistate_Output_Level[object_index - 1]
                                                   [rpdata->array_index - 1];
                        apdu_len = encode_application_unsigned(
                            &apdu[0], present_value);
                    }
                } else {
                    rpdata->error_class = ERROR_CLASS_PROPERTY;
                    rpdata->error_code = ERROR_CODE_INVALID_ARRAY_INDEX;
                    apdu_len = BACNET_STATUS_ERROR;
                }
            }

            break;
        case PROP_RELINQUISH_DEFAULT:
            present_value = MULTISTATE_RELINQUISH_DEFAULT;
            apdu_len = encode_application_unsigned(&apdu[0], present_value);
            break;
        case PROP_NUMBER_OF_STATES:
            apdu_len = encode_application_unsigned(
                &apdu[apdu_len], MULTISTATE_NUMBER_OF_STATES);
            break;

        default:
            rpdata->error_class = ERROR_CLASS_PROPERTY;
            rpdata->error_code = ERROR_CODE_UNKNOWN_PROPERTY;
            apdu_len = BACNET_STATUS_ERROR;
            break;
    }
    /*  only array properties can have array options */
    if ((apdu_len >= 0) && (rpdata->object_property != PROP_STATE_TEXT) &&
        (rpdata->object_property != PROP_PRIORITY_ARRAY) &&
        (rpdata->array_index != BACNET_ARRAY_ALL)) {
        rpdata->error_class = ERROR_CLASS_PROPERTY;
        rpdata->error_code = ERROR_CODE_PROPERTY_IS_NOT_AN_ARRAY;
        apdu_len = BACNET_STATUS_ERROR;
    }

    return apdu_len;
}

/* returns true if successful */
bool Multistate_Output_Write_Property(BACNET_WRITE_PROPERTY_DATA *wp_data)
{
    bool status = false; /* return value */
    unsigned int object_index = 0;
    unsigned int priority = 0;
    uint32_t level = 0;
    int len = 0;
    BACNET_APPLICATION_DATA_VALUE value;
    BACNET_OBJECT_TYPE object_type = OBJECT_NONE;
    uint32_t object_instance = 0;

    /* decode the some of the request */
    len = bacapp_decode_application_data(
        wp_data->application_data, wp_data->application_data_len, &value);
    /* FIXME: len < application_data_len: more data? */
    if (len < 0) {
        /* error while decoding - a value larger than we can handle */
        wp_data->error_class = ERROR_CLASS_PROPERTY;
        wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
        return false;
    }
    if ((wp_data->object_property != PROP_STATE_TEXT) &&
        (wp_data->object_property != PROP_PRIORITY_ARRAY) &&
        (wp_data->array_index != BACNET_ARRAY_ALL)) {
        /*  only array properties can have array options */
        wp_data->error_class = ERROR_CLASS_PROPERTY;
        wp_data->error_code = ERROR_CODE_PROPERTY_IS_NOT_AN_ARRAY;
        return false;
    }
    switch (wp_data->object_property) {
        case PROP_PRESENT_VALUE:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_UNSIGNED_INT);
            if (status) {
                priority = wp_data->priority;
                /* Command priority 6 is reserved for use by Minimum On/Off
                   algorithm and may not be used for other purposes in any
                   object. */
                if (priority && (priority <= BACNET_MAX_PRIORITY) &&
                    (priority != 6 /* reserved */) &&
                    (value.type.Unsigned_Int > 0) &&
                    (value.type.Unsigned_Int <= MULTISTATE_NUMBER_OF_STATES)) {
                    level = value.type.Unsigned_Int;
                    object_index = Multistate_Output_Instance_To_Index(
                        wp_data->object_instance);
                    Multistate_Output_Present_Value_Set(object_index, level, priority, NULL, false);
                    /* Note: you could set the physical output here if we
                       are the highest priority.
                       However, if Out of Service is TRUE, then don't set the
                       physical output.  This comment may apply to the
                       main loop (i.e. check out of service before changing
                       output) */
                    status = true;
                } else if (priority == 6) {
                    /* Command priority 6 is reserved for use by Minimum On/Off
                       algorithm and may not be used for other purposes in any
                       object. */
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_WRITE_ACCESS_DENIED;
                } else {
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                }
            } else {
                status = write_property_type_valid(wp_data, &value,
                    BACNET_APPLICATION_TAG_NULL);
                if (status) {
                    level = MULTISTATE_NULL;
                    object_index = Multistate_Output_Instance_To_Index(
                        wp_data->object_instance);
                    priority = wp_data->priority;
                    if (priority && (priority <= BACNET_MAX_PRIORITY)) {
                        /* priority--;
                        Multistate_Output_Level[object_index - 1][priority] =
                            (uint8_t)level;
                         */
                        Multistate_Output_Present_Value_Set(object_index, level, priority, NULL, false);
                        /* Note: you could set the physical output here to the
                           next highest priority, or to the relinquish default
                           if no priorities are set. However, if Out of Service
                           is TRUE, then don't set the physical output.  This
                           comment may apply to the
                           main loop (i.e. check out of service before changing
                           output) */
                    } else {
                        status = false;
                        wp_data->error_class = ERROR_CLASS_PROPERTY;
                        wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                    }
                }
            }
            break;
        case PROP_OUT_OF_SERVICE:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_BOOLEAN);
            if (status) {
                Multistate_Output_Out_Of_Service_Set(
                    wp_data->object_instance, value.type.Boolean);
            }
            break;
        case PROP_OBJECT_NAME:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_CHARACTER_STRING);
            if (status) {
                /* All the object names in a device must be unique */
                if (Device_Valid_Object_Name(&value.type.Character_String,
                        &object_type, &object_instance)) {
                    if ((object_type == wp_data->object_type) &&
                        (object_instance == wp_data->object_instance)) {
                        /* writing same name to same object */
                        status = true;
                    } else {
                        status = false;
                        wp_data->error_class = ERROR_CLASS_PROPERTY;
                        wp_data->error_code = ERROR_CODE_DUPLICATE_NAME;
                    }
                } else {
                    status = Multistate_Output_Object_Name_Set(
                        wp_data->object_instance, &value.type.Character_String,
                        &wp_data->error_class, &wp_data->error_code, NULL, false);
                }
            }
            break;
        case PROP_OBJECT_IDENTIFIER:
        case PROP_OBJECT_TYPE:
        case PROP_STATUS_FLAGS:
        case PROP_EVENT_STATE:
        case PROP_NUMBER_OF_STATES:
        case PROP_DESCRIPTION:
        case PROP_PRIORITY_ARRAY:
        case PROP_RELINQUISH_DEFAULT:
            wp_data->error_class = ERROR_CLASS_PROPERTY;
            wp_data->error_code = ERROR_CODE_WRITE_ACCESS_DENIED;
            break;
        default:
            wp_data->error_class = ERROR_CLASS_PROPERTY;
            wp_data->error_code = ERROR_CODE_UNKNOWN_PROPERTY;
            break;
    }

    return status;
}

bool Multistate_Output_Priority_Array_Set2(
    uint32_t object_instance, unsigned value, unsigned priority)
{
    unsigned index = 0;
    bool status = false;

    index = Multistate_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Output_Instances) {
        if (priority && (priority <= BACNET_MAX_PRIORITY) &&
            (priority != 6 /* reserved */)) {
            Multistate_Output_Level[index - 1][priority - 1] = value;
            status = true;
        }
    }

    return status;
}
