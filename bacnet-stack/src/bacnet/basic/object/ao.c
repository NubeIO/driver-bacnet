/**************************************************************************
 *
 * Copyright (C) 2005 Steve Karg <skarg@users.sourceforge.net>
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

/* Analog Output Objects - customize for your use */

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
#include "bacnet/wp.h"
#include "bacnet/basic/object/ao.h"
#include "bacnet/basic/services.h"
#if defined(MQTT)
#include "MQTTClient.h"
#include "mqtt_client.h"
#endif /* defined(MQTT) */
#if defined(YAML_CONFIG)
#include "yaml_config.h"
#endif /* defined(YAML_CONFIG) */

#ifndef MAX_ANALOG_OUTPUTS
#define MAX_ANALOG_OUTPUTS 4
#endif

/* Run-time Analog Output Instances */
static int Analog_Output_Instances = 0;

/* we choose to have a NULL level in our system represented by */
/* a particular value.  When the priorities are not in use, they */
/* will be relinquished (i.e. set to the NULL level). */
#define AO_LEVEL_NULL 255
/* When all the priorities are level null, the present value returns */
/* the Relinquish Default value */
#define AO_RELINQUISH_DEFAULT 0
static float *Analog_Output_Relinquish_Defaults = NULL;
/* Here is our Priority Array.  They are supposed to be Real, but */
/* we don't have that kind of memory, so we will use a single byte */
/* and load a Real for returning the value when asked. */
static float **Analog_Output_Level = NULL;
/* Writable out-of-service allows others to play with our Present Value */
/* without changing the physical output */
static bool *Out_Of_Service = NULL;
/* Analog Output Instances Object Name */
static BACNET_CHARACTER_STRING *Analog_Output_Instance_Names = NULL;

/* we need to have our arrays initialized before answering any calls */
static bool Analog_Output_Initialized = false;

/* These three arrays are used by the ReadPropertyMultiple handler */
static const int Properties_Required[] = { PROP_OBJECT_IDENTIFIER,
    PROP_OBJECT_NAME, PROP_OBJECT_TYPE, PROP_PRESENT_VALUE, PROP_STATUS_FLAGS,
    PROP_EVENT_STATE, PROP_OUT_OF_SERVICE, PROP_UNITS, PROP_PRIORITY_ARRAY,
    PROP_RELINQUISH_DEFAULT, -1 };

static const int Properties_Optional[] = { -1 };

static const int Properties_Proprietary[] = { -1 };

void Analog_Output_Property_Lists(
    const int **pRequired, const int **pOptional, const int **pProprietary)
{
    if (pRequired) {
        *pRequired = Properties_Required;
    }
    if (pOptional) {
        *pOptional = Properties_Optional;
    }
    if (pProprietary) {
        *pProprietary = Properties_Proprietary;
    }

    return;
}

void Analog_Output_Init(void)
{
    char buf[51];
    char *pEnv;
    unsigned i, j;

    if (!Analog_Output_Initialized) {
        Analog_Output_Initialized = true;

#if defined(YAML_CONFIG)
        Analog_Output_Instances = yaml_config_ao_max();
        if (Analog_Output_Instances == 0) {
#endif
        pEnv = getenv("AO");
        if (pEnv) {
            Analog_Output_Instances = atoi(pEnv);
        }
#if defined(YAML_CONFIG)
        }
#endif

        /* initialize all the analog output priority arrays to NULL */
        if (Analog_Output_Instances > 0) {
            Analog_Output_Level = malloc(Analog_Output_Instances * sizeof(float *));

            for (i = 0; i < Analog_Output_Instances; i++) {
                Analog_Output_Level[i] = malloc(BACNET_MAX_PRIORITY * sizeof(float));
                for (j = 0; j < BACNET_MAX_PRIORITY; j++) {
                    Analog_Output_Level[i][j] = AO_LEVEL_NULL;
                }
            }

            Out_Of_Service = malloc(Analog_Output_Instances * sizeof(bool));
            Analog_Output_Instance_Names = malloc(Analog_Output_Instances * sizeof(BACNET_CHARACTER_STRING));
            for (i = 0; i < Analog_Output_Instances; i++) {
                sprintf(buf, "AO_%d_SPARE", i + 1);
                characterstring_init_ansi(&Analog_Output_Instance_Names[i], buf);
            }

            Analog_Output_Relinquish_Defaults = malloc(Analog_Output_Instances * sizeof(float));
            for (i = 0; i < Analog_Output_Instances; i++) {
                Analog_Output_Relinquish_Defaults[i] = AO_RELINQUISH_DEFAULT;
            }
        }
    }

    return;
}

/* we simply have 0-n object instances.  Yours might be */
/* more complex, and then you need validate that the */
/* given instance exists */
bool Analog_Output_Valid_Instance(uint32_t object_instance)
{
    if (object_instance > 0 && object_instance <= Analog_Output_Instances) {
        return true;
    }

    return false;
}

/* we simply have 0-n object instances.  Yours might be */
/* more complex, and then count how many you have */
unsigned Analog_Output_Count(void)
{
    return (Analog_Output_Instances);
}

/* we simply have 0-n object instances.  Yours might be */
/* more complex, and then you need to return the instance */
/* that correlates to the correct index */
uint32_t Analog_Output_Index_To_Instance(unsigned index)
{
    return index;
}

/* we simply have 0-n object instances.  Yours might be */
/* more complex, and then you need to return the index */
/* that correlates to the correct instance number */
unsigned Analog_Output_Instance_To_Index(uint32_t object_instance)
{
    unsigned index = Analog_Output_Instances;

    if (object_instance > 0 && object_instance <= Analog_Output_Instances) {
        index = object_instance;
    }

    return index;
}

float Analog_Output_Present_Value(uint32_t object_instance)
{
    float value = AO_RELINQUISH_DEFAULT;
    unsigned index = 0;
    unsigned i = 0;

    index = Analog_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        value = Analog_Output_Relinquish_Defaults[index - 1];
        for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
            if (Analog_Output_Level[index - 1][i] != AO_LEVEL_NULL) {
                value = Analog_Output_Level[index - 1][i];
                break;
            }
        }
    }

    return value;
}

unsigned Analog_Output_Present_Value_Priority(uint32_t object_instance)
{
    unsigned index = 0; /* instance to index conversion */
    unsigned i = 0; /* loop counter */
    unsigned priority = 0; /* return value */

    index = Analog_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
            if (Analog_Output_Level[index - 1][i] != AO_LEVEL_NULL) {
                priority = i + 1;
                break;
            }
        }
    }

    return priority;
}

bool Analog_Output_Present_Value_Set(
    uint32_t object_instance, float value, unsigned priority, char *uuid, int bacnet_client)
{
    unsigned index = 0;
    bool status = false;

    index = Analog_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        if (priority && (priority <= BACNET_MAX_PRIORITY) &&
            (priority != 6 /* reserved */)) {
            Analog_Output_Level[index - 1][priority - 1] = value;
            /* Note: you could set the physical output here to the next
               highest priority, or to the relinquish default if no
               priorities are set.
               However, if Out of Service is TRUE, then don't set the
               physical output.  This comment may apply to the
               main loop (i.e. check out of service before changing output) */
            status = true;
#if defined(MQTT)
            /* if (yaml_config_mqtt_enable() && !bacnet_client) {
                if (value == AO_LEVEL_NULL) {
                  mqtt_publish_topic(OBJECT_ANALOG_OUTPUT, object_instance, PROP_PRESENT_VALUE,
                      MQTT_TOPIC_VALUE_STRING, "null", uuid);
                } else {
                  mqtt_publish_topic(OBJECT_ANALOG_OUTPUT, object_instance, PROP_PRESENT_VALUE,
                      MQTT_TOPIC_VALUE_FLOAT, &value, uuid);
                }

                publish_ao_priority_array(object_instance, uuid);
            } */
#endif /* defined(MQTT) */
        }
    }

    return status;
}


bool Analog_Output_Priority_Array_Set(
    uint32_t object_instance, float value, unsigned priority, char *uuid)
{
    unsigned index = 0;
    bool status = false;

    index = Analog_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        if (priority && (priority <= BACNET_MAX_PRIORITY) &&
            (priority != 6 /* reserved */)) {
            Analog_Output_Level[index - 1][priority - 1] = value;
            status = true;
#if defined(MQTT)
            /* if (yaml_config_mqtt_enable()) {
                publish_ao_priority_array(object_instance, uuid);
            } */
#endif /* defined(MQTT) */
        }
    }

    return status;
}


bool Analog_Output_Priority_Array_Set2(
    uint32_t object_instance, float value, unsigned priority)
{
    unsigned index = 0;
    bool status = false;

    index = Analog_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        if (priority && (priority <= BACNET_MAX_PRIORITY) &&
            (priority != 6 /* reserved */)) {
            Analog_Output_Level[index - 1][priority - 1] = value;
            status = true;
        }
    }

    return status;
}


bool Analog_Output_Present_Value_Relinquish(
    uint32_t object_instance, unsigned priority)
{
    unsigned index = 0;
    bool status = false;

    index = Analog_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        if (priority && (priority <= BACNET_MAX_PRIORITY) &&
            (priority != 6 /* reserved */)) {
            Analog_Output_Level[index - 1][priority - 1] = AO_LEVEL_NULL;
            /* Note: you could set the physical output here to the next
               highest priority, or to the relinquish default if no
               priorities are set.
               However, if Out of Service is TRUE, then don't set the
               physical output.  This comment may apply to the
               main loop (i.e. check out of service before changing output) */
            status = true;
#if defined(MQTT)
            /* if (yaml_config_mqtt_enable()) {
                mqtt_publish_topic(OBJECT_ANALOG_OUTPUT, object_instance, PROP_PRESENT_VALUE,
                    MQTT_TOPIC_VALUE_STRING, "null", NULL);

                publish_ao_priority_array(object_instance, NULL);
            } */
#endif /* defined(MQTT) */
        }
    }

    return status;
}

/* note: the object name must be unique within this device */
bool Analog_Output_Object_Name(
    uint32_t object_instance, BACNET_CHARACTER_STRING *object_name)
{
    bool status = false;
    unsigned index = 0;

    index = Analog_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        status = characterstring_copy(object_name, &Analog_Output_Instance_Names[index - 1]);
    }

    return status;
}

bool Analog_Output_Set_Object_Name(
    uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid, int bacnet_client)
{
    bool status = false;
    unsigned index = 0;

    index = Analog_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        if (!characterstring_same(&Analog_Output_Instance_Names[index - 1], object_name)) {
            status = characterstring_copy(&Analog_Output_Instance_Names[index - 1], object_name);
        }
#if defined(MQTT)
        /* if (yaml_config_mqtt_enable() && !!bacnet_client) {
            mqtt_publish_topic(OBJECT_ANALOG_OUTPUT, object_instance, PROP_OBJECT_NAME,
                MQTT_TOPIC_VALUE_BACNET_STRING, object_name, uuid);
        } */
#endif /* defined(MQTT) */
    }

    return status;
}

bool Analog_Output_Out_Of_Service(uint32_t instance)
{
    unsigned index = 0;
    bool oos_flag = false;

    index = Analog_Output_Instance_To_Index(instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        oos_flag = Out_Of_Service[index - 1];
    }

    return oos_flag;
}

void Analog_Output_Out_Of_Service_Set(uint32_t instance, bool oos_flag)
{
    unsigned index = 0;

    index = Analog_Output_Instance_To_Index(instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        Out_Of_Service[index - 1] = oos_flag;
    }
}

float Analog_Output_Relinquish_Default(uint32_t object_instance)
{
    float value = AO_RELINQUISH_DEFAULT;
    unsigned index = 0;

    index = Analog_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        value = Analog_Output_Relinquish_Defaults[index - 1];
    }

    return value;
}

/* return apdu len, or BACNET_STATUS_ERROR on error */
int Analog_Output_Read_Property(BACNET_READ_PROPERTY_DATA *rpdata)
{
    int len = 0;
    int apdu_len = 0; /* return value */
    BACNET_BIT_STRING bit_string;
    BACNET_CHARACTER_STRING char_string;
    float real_value = (float)1.414;
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
                &apdu[0], OBJECT_ANALOG_OUTPUT, rpdata->object_instance);
            break;
        case PROP_OBJECT_NAME:
            Analog_Output_Object_Name(rpdata->object_instance, &char_string);
            apdu_len =
                encode_application_character_string(&apdu[0], &char_string);
            break;
        case PROP_OBJECT_TYPE:
            apdu_len =
                encode_application_enumerated(&apdu[0], OBJECT_ANALOG_OUTPUT);
            break;
        case PROP_PRESENT_VALUE:
            real_value = Analog_Output_Present_Value(rpdata->object_instance);
            apdu_len = encode_application_real(&apdu[0], real_value);
            break;
        case PROP_STATUS_FLAGS:
            bitstring_init(&bit_string);
            bitstring_set_bit(&bit_string, STATUS_FLAG_IN_ALARM, false);
            bitstring_set_bit(&bit_string, STATUS_FLAG_FAULT, false);
            bitstring_set_bit(&bit_string, STATUS_FLAG_OVERRIDDEN, false);
            state = Analog_Output_Out_Of_Service(rpdata->object_instance);
            bitstring_set_bit(&bit_string, STATUS_FLAG_OUT_OF_SERVICE, state);
            apdu_len = encode_application_bitstring(&apdu[0], &bit_string);
            break;
        case PROP_EVENT_STATE:
            apdu_len =
                encode_application_enumerated(&apdu[0], EVENT_STATE_NORMAL);
            break;
        case PROP_OUT_OF_SERVICE:
            state = Analog_Output_Out_Of_Service(rpdata->object_instance);
            apdu_len = encode_application_boolean(&apdu[0], state);
            break;
        case PROP_UNITS:
            apdu_len = encode_application_enumerated(&apdu[0], UNITS_NO_UNITS);
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
                object_index =
                    Analog_Output_Instance_To_Index(rpdata->object_instance);
                for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
                    /* FIXME: check if we have room before adding it to APDU */
                    if (Analog_Output_Level[object_index - 1][i] == AO_LEVEL_NULL) {
                        len = encode_application_null(&apdu[apdu_len]);
                    } else {
                        real_value =
                            Analog_Output_Level[object_index - 1][i];
                        len = encode_application_real(
                            &apdu[apdu_len], real_value);
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
                object_index =
                    Analog_Output_Instance_To_Index(rpdata->object_instance);
                if (rpdata->array_index <= BACNET_MAX_PRIORITY) {
                    if (Analog_Output_Level[object_index - 1][rpdata->array_index -
                            1] == AO_LEVEL_NULL) {
                        apdu_len = encode_application_null(&apdu[0]);
                    } else {
                        real_value =
                            Analog_Output_Level[object_index - 1]
                                               [rpdata->array_index - 1];
                        apdu_len =
                            encode_application_real(&apdu[0], real_value);
                    }
                } else {
                    rpdata->error_class = ERROR_CLASS_PROPERTY;
                    rpdata->error_code = ERROR_CODE_INVALID_ARRAY_INDEX;
                    apdu_len = BACNET_STATUS_ERROR;
                }
            }
            break;
        case PROP_RELINQUISH_DEFAULT:
            real_value =
                Analog_Output_Relinquish_Default(rpdata->object_instance);
            apdu_len = encode_application_real(&apdu[0], real_value);
            break;
        default:
            rpdata->error_class = ERROR_CLASS_PROPERTY;
            rpdata->error_code = ERROR_CODE_UNKNOWN_PROPERTY;
            apdu_len = BACNET_STATUS_ERROR;
            break;
    }
    /*  only array properties can have array options */
    if ((apdu_len >= 0) && (rpdata->object_property != PROP_PRIORITY_ARRAY) &&
        (rpdata->array_index != BACNET_ARRAY_ALL)) {
        rpdata->error_class = ERROR_CLASS_PROPERTY;
        rpdata->error_code = ERROR_CODE_PROPERTY_IS_NOT_AN_ARRAY;
        apdu_len = BACNET_STATUS_ERROR;
    }

    return apdu_len;
}

/* publish the values of priority array over mqtt */
void publish_ao_priority_array(uint32_t object_instance, char *uuid)
{
    float value;
    char buf[1024] = {0};
    char *first = "";
    unsigned index = 0;
    unsigned i;


    index = Analog_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        strcpy(buf, "{");
        for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
            value = Analog_Output_Level[index - 1][i];
            if (value == AO_LEVEL_NULL) {
                sprintf(&buf[strlen(buf)], "%sNull", first);
            } else {
                sprintf(&buf[strlen(buf)], "%s%.6f", first, value);
            }

            first = ",";
        }

        sprintf(&buf[strlen(buf)], "}");
        if (yaml_config_mqtt_enable()) {
            mqtt_publish_topic(OBJECT_ANALOG_OUTPUT, object_instance, PROP_PRIORITY_ARRAY,
                MQTT_TOPIC_VALUE_STRING, buf, uuid);
        }
    }
}

/* get the values of priority array */
void get_ao_priority_array(uint32_t object_instance, float *pa, int pa_length)
{
    unsigned index = 0;
    unsigned i;
    unsigned max;

    index = Analog_Output_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Output_Instances) {
        max = (pa_length < BACNET_MAX_PRIORITY) ? pa_length : BACNET_MAX_PRIORITY;
        for (i = 0; i < max; i++) {
            pa[i] = Analog_Output_Level[index - 1][i];
        }
    }
}

/* returns true if successful */
bool Analog_Output_Write_Property(BACNET_WRITE_PROPERTY_DATA *wp_data)
{
    unsigned int object_index = 0;
    bool status = false; /* return value */
    int len = 0;
    BACNET_APPLICATION_DATA_VALUE value;

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
    /*  only array properties can have array options */
    if ((wp_data->object_property != PROP_PRIORITY_ARRAY) &&
        (wp_data->array_index != BACNET_ARRAY_ALL)) {
        wp_data->error_class = ERROR_CLASS_PROPERTY;
        wp_data->error_code = ERROR_CODE_PROPERTY_IS_NOT_AN_ARRAY;
        return false;
    }
    switch (wp_data->object_property) {
        case PROP_PRESENT_VALUE:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_REAL);
            if (status) {
                /* Command priority 6 is reserved for use by Minimum On/Off
                   algorithm and may not be used for other purposes in any
                   object. */
                status =
                    Analog_Output_Present_Value_Set(wp_data->object_instance,
                        value.type.Real, wp_data->priority, NULL, true);
                if (wp_data->priority == 6) {
                    /* Command priority 6 is reserved for use by Minimum On/Off
                       algorithm and may not be used for other purposes in any
                       object. */
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_WRITE_ACCESS_DENIED;
                    status = false;
                } else if (!status) {
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                    status = false;
                }
            } else {
                status = write_property_type_valid(wp_data, &value,
                    BACNET_APPLICATION_TAG_NULL);
                if (status) {
                    status = Analog_Output_Present_Value_Relinquish(
                        wp_data->object_instance, wp_data->priority);
                    if (!status) {
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
                Analog_Output_Out_Of_Service_Set(
                    wp_data->object_instance, value.type.Boolean);
            }
            break;
        case PROP_OBJECT_IDENTIFIER:
        case PROP_OBJECT_NAME:
            status = write_property_string_valid(wp_data, &value,
                characterstring_capacity(&Analog_Output_Instance_Names[0]));
            if (status) {
                Analog_Output_Set_Object_Name(wp_data->object_instance,
                    &value.type.Character_String, NULL, true);
            }
            break;
        case PROP_OBJECT_TYPE:
        case PROP_STATUS_FLAGS:
        case PROP_EVENT_STATE:
        case PROP_UNITS:
            wp_data->error_class = ERROR_CLASS_PROPERTY;
            wp_data->error_code = ERROR_CODE_WRITE_ACCESS_DENIED;
            break;
        case PROP_PRIORITY_ARRAY:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_REAL);
            if (status) {
                if (wp_data->array_index > 0 && wp_data->array_index <= BACNET_MAX_PRIORITY) {
                    float f_value;
                    f_value = value.type.Real;

                    Analog_Output_Priority_Array_Set(wp_data->object_instance, f_value,
                        wp_data->array_index, NULL);
                } else {
                    status = false;
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_INVALID_ARRAY_INDEX;
                }
            }
            break;
        case PROP_RELINQUISH_DEFAULT:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_REAL);
            if (status) {
                float f_value;
                f_value = value.type.Real;
                object_index = Analog_Output_Instance_To_Index(
                    wp_data->object_instance);
                Analog_Output_Relinquish_Defaults[object_index - 1] = f_value;
#if defined(MQTT)
                /* if (yaml_config_mqtt_enable()) {
                    mqtt_publish_topic(OBJECT_ANALOG_OUTPUT, wp_data->object_instance, PROP_RELINQUISH_DEFAULT,
                        MQTT_TOPIC_VALUE_FLOAT, &f_value, NULL);
                } */
#endif /* defined(MQTT) */
            }
            break;
        default:
            wp_data->error_class = ERROR_CLASS_PROPERTY;
            wp_data->error_code = ERROR_CODE_UNKNOWN_PROPERTY;
            break;
    }

    return status;
}
