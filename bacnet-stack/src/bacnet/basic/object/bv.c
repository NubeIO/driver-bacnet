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

/* Binary Output Objects - customize for your use */

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
#include "bacnet/rp.h"
#include "bacnet/basic/object/bv.h"
#include "bacnet/basic/services.h"
#if defined(MQTT)
#include "MQTTClient.h"
#include "mqtt_client.h"
#endif /* defined(MQTT) */
#if defined(YAML_CONFIG)
#include "yaml_config.h"
#endif /* defined(YAML_CONFIG) */

#ifndef MAX_BINARY_VALUES
#define MAX_BINARY_VALUES 10
#endif

/* Run-time Binary Value Instances */
static int Binary_Value_Instances = 0;

/* When all the priorities are level null, the present value returns */
/* the Relinquish Default value */
#define RELINQUISH_DEFAULT BINARY_INACTIVE
static BACNET_BINARY_PV *Binary_Value_Relinquish_Defaults = NULL;
/* Here is our Priority Array.*/
static BACNET_BINARY_PV **Binary_Value_Level = NULL;
/* Writable out-of-service allows others to play with our Present Value */
/* without changing the physical output */
static bool *Out_Of_Service = NULL;
/* Binary Value Instances Object Name */
static BACNET_CHARACTER_STRING *Binary_Value_Instance_Names = NULL;

/* These three arrays are used by the ReadPropertyMultiple handler */
static const int Binary_Value_Properties_Required[] = { PROP_OBJECT_IDENTIFIER,
    PROP_OBJECT_NAME, PROP_OBJECT_TYPE, PROP_PRESENT_VALUE, PROP_STATUS_FLAGS,
    PROP_EVENT_STATE, PROP_OUT_OF_SERVICE, -1 };

static const int Binary_Value_Properties_Optional[] = { PROP_DESCRIPTION,
    PROP_PRIORITY_ARRAY, PROP_RELINQUISH_DEFAULT, -1 };

static const int Binary_Value_Properties_Proprietary[] = { -1 };

/**
 * Initialize the pointers for the required, the optional and the properitary
 * value properties.
 *
 * @param pRequired - Pointer to the pointer of required values.
 * @param pOptional - Pointer to the pointer of optional values.
 * @param pProprietary - Pointer to the pointer of properitary values.
 */
void Binary_Value_Property_Lists(
    const int **pRequired, const int **pOptional, const int **pProprietary)
{
    if (pRequired) {
        *pRequired = Binary_Value_Properties_Required;
    }
    if (pOptional) {
        *pOptional = Binary_Value_Properties_Optional;
    }
    if (pProprietary) {
        *pProprietary = Binary_Value_Properties_Proprietary;
    }

    return;
}

/**
 * Initialize the binary values.
 */
void Binary_Value_Init(void)
{
    char buf[51];
    char *pEnv;
    unsigned i, j;
    static bool initialized = false;

    if (!initialized) {
        initialized = true;

#if defined(YAML_CONFIG)
        Binary_Value_Instances = yaml_config_bv_max();
        if (Binary_Value_Instances == 0) {
#endif
        pEnv = getenv("BV");
        if (pEnv) {
            Binary_Value_Instances = atoi(pEnv);
        }
#if defined(YAML_CONFIG)
        }
#endif

        /* initialize all the analog output priority arrays to NULL */
        if (Binary_Value_Instances > 0) {
            Binary_Value_Level = malloc(Binary_Value_Instances * sizeof(BACNET_BINARY_PV*));

            for (i = 0; i < Binary_Value_Instances; i++) {
                Binary_Value_Level [i] = malloc(BACNET_MAX_PRIORITY * sizeof(BACNET_BINARY_PV));
                for (j = 0; j < BACNET_MAX_PRIORITY; j++) {
                    Binary_Value_Level[i][j] = BINARY_NULL;
                }
            }

            Out_Of_Service = malloc(Binary_Value_Instances * sizeof(bool));
            Binary_Value_Instance_Names = malloc(Binary_Value_Instances * sizeof(BACNET_CHARACTER_STRING));
            for (i = 0; i < Binary_Value_Instances; i++) {
                sprintf(buf, "BV_%d_SPARE", i + 1);
                characterstring_init_ansi(&Binary_Value_Instance_Names[i], buf);
            }

            Binary_Value_Relinquish_Defaults = malloc(Binary_Value_Instances * sizeof(BACNET_BINARY_PV));
            for (i = 0; i < Binary_Value_Instances; i++) {
                Binary_Value_Relinquish_Defaults[i] = RELINQUISH_DEFAULT;
            }
        }
    }

    return;
}

/**
 * We simply have 0-n object instances. Yours might be
 * more complex, and then you need validate that the
 * given instance exists.
 *
 * @param object_instance Object instance
 *
 * @return true/false
 */
bool Binary_Value_Valid_Instance(uint32_t object_instance)
{
    if (object_instance > 0 && object_instance <= Binary_Value_Instances) {
        return true;
    }

    return false;
}

/**
 * Return the count of analog values.
 *
 * @return Count of binary values.
 */
unsigned Binary_Value_Count(void)
{
    return (Binary_Value_Instances + 1);
}

/**
 * We simply have 0-n object instances.  Yours might be
 * more complex, and then you need to return the instance
 * that correlates to the correct index.
 *
 * @param index Index
 *
 * @return Object instance
 */
uint32_t Binary_Value_Index_To_Instance(unsigned index)
{
    return index;
}

/**
 * We simply have 0-n object instances. Yours might be
 * more complex, and then you need to return the index
 * that correlates to the correct instance number
 *
 * @param object_instance Object instance
 *
 * @return Index in the object table.
 */
unsigned Binary_Value_Instance_To_Index(uint32_t object_instance)
{
    unsigned index = Binary_Value_Instances;

    if (object_instance > 0 && object_instance <= Binary_Value_Instances) {
        index = object_instance;
    }

    return index;
}

/**
 * For a given object instance-number, return the present value.
 *
 * @param  object_instance - object-instance number of the object
 *
 * @return  Present value
 */
BACNET_BINARY_PV Binary_Value_Present_Value(uint32_t object_instance)
{
    BACNET_BINARY_PV value = RELINQUISH_DEFAULT;
    unsigned index = 0;
    unsigned i = 0;

    index = Binary_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Binary_Value_Instances) {
        value = Binary_Value_Relinquish_Defaults[index - 1];
        for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
            if (Binary_Value_Level[index - 1][i] != BINARY_NULL) {
                value = Binary_Value_Level[index - 1][i];
                break;
            }
        }
    }

    return value;
}

/**
 * For a given object instance-number, return the name.
 *
 * Note: the object name must be unique within this device
 *
 * @param  object_instance - object-instance number of the object
 * @param  object_name - object name/string pointer
 *
 * @return  true/false
 */
bool Binary_Value_Object_Name(
    uint32_t object_instance, BACNET_CHARACTER_STRING *object_name)
{
    bool status = false;
    unsigned index = 0;

    index = Binary_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Binary_Value_Instances) {
        status = characterstring_copy(object_name, &Binary_Value_Instance_Names[index - 1]);
    }

    return status;
}

bool Binary_Value_Set_Object_Name(
    uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid)
{
    bool status = false;
    unsigned index = 0;

    index = Binary_Value_Instance_To_Index(object_instance);
    if (index > 0 &&  index <= Binary_Value_Instances) {
        if (!characterstring_same(&Binary_Value_Instance_Names[index - 1], object_name)) {
            status = characterstring_copy(&Binary_Value_Instance_Names[index - 1], object_name);
        }
#if defined(MQTT)
        if (yaml_config_mqtt_enable()) {
            mqtt_publish_topic(OBJECT_BINARY_VALUE, object_instance, PROP_OBJECT_NAME,
                MQTT_TOPIC_VALUE_BACNET_STRING, object_name, uuid);
        }
#endif /* defined(MQTT) */
    }

    return status;
}

BACNET_BINARY_PV Binary_Value_Relinquish_Default(uint32_t object_instance)
{
    BACNET_BINARY_PV value = RELINQUISH_DEFAULT;
    unsigned index = 0;

    index = Binary_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Binary_Value_Instances) {
        value = Binary_Value_Relinquish_Defaults[index - 1];
    }

    return value;
}

/**
 * Return the OOO value, if any.
 *
 * @param instance Object instance.
 *
 * @return true/false
 */
bool Binary_Value_Out_Of_Service(uint32_t instance)
{
    unsigned index = 0;
    bool oos_flag = false;

    index = Binary_Value_Instance_To_Index(instance);
    if (index > 0 && index <= Binary_Value_Instances) {
        oos_flag = Out_Of_Service[index - 1];
    }

    return oos_flag;
}

/**
 * Set the OOO value, if any.
 *
 * @param instance Object instance.
 * @param oos_flag New OOO value.
 */
void Binary_Value_Out_Of_Service_Set(uint32_t instance, bool oos_flag)
{
    unsigned index = 0;

    index = Binary_Value_Instance_To_Index(instance);
    if (index > 0 && index <= Binary_Value_Instances) {
        Out_Of_Service[index - 1] = oos_flag;
    }
}

/**
 * Return the requested property of the binary value.
 *
 * @param rpdata  Property requested, see for BACNET_READ_PROPERTY_DATA details.
 *
 * @return apdu len, or BACNET_STATUS_ERROR on error
 */
int Binary_Value_Read_Property(BACNET_READ_PROPERTY_DATA *rpdata)
{
    int len = 0;
    int apdu_len = 0; /* return value */
    BACNET_BIT_STRING bit_string;
    BACNET_CHARACTER_STRING char_string;
    BACNET_BINARY_PV present_value = BINARY_INACTIVE;
    unsigned object_index = 0;
    unsigned i = 0;
    bool state = false;
    uint8_t *apdu = NULL;

    /* Valid data? */
    if (rpdata == NULL) {
        return 0;
    }
    if ((rpdata->application_data == NULL) ||
        (rpdata->application_data_len == 0)) {
        return 0;
    }

    /* Valid object index? */
    object_index = Binary_Value_Instance_To_Index(rpdata->object_instance);
    if (object_index < 1 || object_index > Binary_Value_Instances) {
        rpdata->error_class = ERROR_CLASS_OBJECT;
        rpdata->error_code = ERROR_CODE_UNKNOWN_OBJECT;
        return BACNET_STATUS_ERROR;
    }

    apdu = rpdata->application_data;
    switch (rpdata->object_property) {
        case PROP_OBJECT_IDENTIFIER:
            apdu_len = encode_application_object_id(
                &apdu[0], OBJECT_BINARY_VALUE, rpdata->object_instance);
            break;
            /* note: Name and Description don't have to be the same.
               You could make Description writable and different */
        case PROP_OBJECT_NAME:
        case PROP_DESCRIPTION:
            if (Binary_Value_Object_Name(
                    rpdata->object_instance, &char_string)) {
                apdu_len =
                    encode_application_character_string(&apdu[0], &char_string);
            }
            break;
        case PROP_OBJECT_TYPE:
            apdu_len =
                encode_application_enumerated(&apdu[0], OBJECT_BINARY_VALUE);
            break;
        case PROP_PRESENT_VALUE:
            present_value = Binary_Value_Present_Value(rpdata->object_instance);
            apdu_len = encode_application_enumerated(&apdu[0], present_value);
            break;
        case PROP_STATUS_FLAGS:
            /* note: see the details in the standard on how to use these */
            bitstring_init(&bit_string);
            bitstring_set_bit(&bit_string, STATUS_FLAG_IN_ALARM, false);
            bitstring_set_bit(&bit_string, STATUS_FLAG_FAULT, false);
            bitstring_set_bit(&bit_string, STATUS_FLAG_OVERRIDDEN, false);
            state = Binary_Value_Out_Of_Service(rpdata->object_instance);
            bitstring_set_bit(&bit_string, STATUS_FLAG_OUT_OF_SERVICE, state);
            apdu_len = encode_application_bitstring(&apdu[0], &bit_string);
            break;
        case PROP_EVENT_STATE:
            /* note: see the details in the standard on how to use this */
            apdu_len =
                encode_application_enumerated(&apdu[0], EVENT_STATE_NORMAL);
            break;
        case PROP_OUT_OF_SERVICE:
            state = Binary_Value_Out_Of_Service(rpdata->object_instance);
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
                for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
                    /* FIXME: check if we have room before adding it to APDU */
                    if (Binary_Value_Level[object_index - 1][i] == BINARY_NULL) {
                        len = encode_application_null(&apdu[apdu_len]);
                    } else {
                        present_value = Binary_Value_Level[object_index - 1][i];
                        len = encode_application_enumerated(
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
                if (rpdata->array_index <= BACNET_MAX_PRIORITY) {
                    if (Binary_Value_Level[object_index - 1][rpdata->array_index] ==
                        BINARY_NULL) {
                        apdu_len = encode_application_null(&apdu[apdu_len]);
                    } else {
                        present_value = Binary_Value_Level[object_index - 1]
                                                          [rpdata->array_index];
                        apdu_len = encode_application_enumerated(
                            &apdu[apdu_len], present_value);
                    }
                } else {
                    rpdata->error_class = ERROR_CLASS_PROPERTY;
                    rpdata->error_code = ERROR_CODE_INVALID_ARRAY_INDEX;
                    apdu_len = BACNET_STATUS_ERROR;
                }
            }
            break;
        case PROP_RELINQUISH_DEFAULT:
            present_value =
                Binary_Value_Relinquish_Default(rpdata->object_instance);
            apdu_len = encode_application_enumerated(&apdu[0], present_value);
            break;
        default:
            rpdata->error_class = ERROR_CLASS_PROPERTY;
            rpdata->error_code = ERROR_CODE_UNKNOWN_PROPERTY;
            apdu_len = BACNET_STATUS_ERROR;
            break;
    }

    /* Only array properties can have array options. */
    if ((apdu_len >= 0) && (rpdata->object_property != PROP_PRIORITY_ARRAY) &&
        (rpdata->array_index != BACNET_ARRAY_ALL)) {
        rpdata->error_class = ERROR_CLASS_PROPERTY;
        rpdata->error_code = ERROR_CODE_PROPERTY_IS_NOT_AN_ARRAY;
        apdu_len = BACNET_STATUS_ERROR;
    }

    return apdu_len;
}

/* publish the values of priority array over mqtt */
void publish_bv_priority_array(uint32_t object_instance, char *uuid)
{   
    BACNET_BINARY_PV value;
    char buf[1024] = {0};
    char *first = "";
    unsigned index = 0;
    unsigned i;
    

    index = Binary_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Binary_Value_Instances) {
        strcpy(buf, "{");
        for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
            value = Binary_Value_Level[index - 1][i];
            if (value == BINARY_NULL) {
                sprintf(&buf[strlen(buf)], "%sNull", first);
            } else { 
                sprintf(&buf[strlen(buf)], "%s%d", first, value);
            }
            
            first = ",";
        }
        
        sprintf(&buf[strlen(buf)], "}");
        if (yaml_config_mqtt_enable()) {
            mqtt_publish_topic(OBJECT_BINARY_VALUE, object_instance, PROP_PRIORITY_ARRAY,
                MQTT_TOPIC_VALUE_STRING, buf, uuid);
        }
    }
}


bool Binary_Value_Present_Value_Set(
    uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid)
{
    unsigned index = 0;
    bool status = false;

    index = Binary_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Binary_Value_Instances) {
        Binary_Value_Level[index - 1][priority - 1] = value;
        status = true;
#if defined(MQTT)
        if (yaml_config_mqtt_enable()) {
            if (value == BINARY_NULL) {
                mqtt_publish_topic(OBJECT_BINARY_VALUE, object_instance, PROP_PRESENT_VALUE,
                    MQTT_TOPIC_VALUE_STRING, "null", uuid);
            } else {
                mqtt_publish_topic(OBJECT_BINARY_VALUE, object_instance, PROP_PRESENT_VALUE,
                    MQTT_TOPIC_VALUE_INTEGER, &value, uuid);
            }
            publish_bv_priority_array(object_instance, uuid);
        }
#endif /* defined(MQTT) */
    }

    return status;
}


bool Binary_Value_Priority_Array_Set(
    uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority, char *uuid)
{
    unsigned index = 0;
    bool status = false;

    index = Binary_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Binary_Value_Instances) {
        Binary_Value_Level[index - 1][priority - 1] = value;
        status = true;
#if defined(MQTT)
        if (yaml_config_mqtt_enable()) {
            publish_bv_priority_array(object_instance, uuid);
        }
#endif /* defined(MQTT) */
    }

    return status;
}


bool Binary_Value_Priority_Array_Set2(
    uint32_t object_instance, BACNET_BINARY_PV value, unsigned int priority)
{
    unsigned index = 0;
    bool status = false;

    index = Binary_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Binary_Value_Instances) {
        Binary_Value_Level[index - 1][priority - 1] = value;
        status = true;
    }

    return status;
}


/**
 * Set the requested property of the binary value.
 *
 * @param wp_data  Property requested, see for BACNET_WRITE_PROPERTY_DATA
 * details.
 *
 * @return true if successful
 */
bool Binary_Value_Write_Property(BACNET_WRITE_PROPERTY_DATA *wp_data)
{
    bool status = false; /* return value */
    unsigned int object_index = 0;
    unsigned int priority = 0;
    BACNET_BINARY_PV level = BINARY_NULL;
    int len = 0;
    BACNET_APPLICATION_DATA_VALUE value;

    /* Valid data? */
    if (wp_data == NULL) {
        return false;
    }
    if (wp_data->application_data_len == 0) {
        return false;
    }

    /* Decode the some of the request. */
    len = bacapp_decode_application_data(
        wp_data->application_data, wp_data->application_data_len, &value);
    /* FIXME: len < application_data_len: more data? */
    if (len < 0) {
        /* error while decoding - a value larger than we can handle */
        wp_data->error_class = ERROR_CLASS_PROPERTY;
        wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
        return false;
    }
    /* Only array properties can have array options. */
    if ((wp_data->object_property != PROP_PRIORITY_ARRAY) &&
        (wp_data->array_index != BACNET_ARRAY_ALL)) {
        wp_data->error_class = ERROR_CLASS_PROPERTY;
        wp_data->error_code = ERROR_CODE_PROPERTY_IS_NOT_AN_ARRAY;
        return false;
    }

    /* Valid object index? */
    object_index = Binary_Value_Instance_To_Index(wp_data->object_instance);
    if (object_index < 1 || object_index > Binary_Value_Instances) {
        wp_data->error_class = ERROR_CLASS_OBJECT;
        wp_data->error_code = ERROR_CODE_UNKNOWN_OBJECT;
        return false;
    }

    switch (wp_data->object_property) {
        case PROP_PRESENT_VALUE:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_ENUMERATED);
            if (status) {
                priority = wp_data->priority;
                /* Command priority 6 is reserved for use by Minimum On/Off
                   algorithm and may not be used for other purposes in any
                   object. */
                if (priority && (priority <= BACNET_MAX_PRIORITY) &&
                    (priority != 6 /* reserved */) &&
                    (value.type.Enumerated <= MAX_BINARY_PV)) {
                    /* level = (BACNET_BINARY_PV)value.type.Enumerated;
                    priority--;
                    Binary_Value_Level[object_index][priority] = level; */
                    /* Note: you could set the physical output here if we
                       are the highest priority.
                       However, if Out of Service is TRUE, then don't set the
                       physical output.  This comment may apply to the
                       main loop (i.e. check out of service before changing
                       output) */
                    Binary_Value_Present_Value_Set(wp_data->object_instance,
                        (BACNET_BINARY_PV)value.type.Enumerated, wp_data->priority, NULL);
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
                    level = BINARY_NULL;
                    priority = wp_data->priority;
                    if (priority && (priority <= BACNET_MAX_PRIORITY)) {
                        /* priority--;
                        Binary_Value_Level[object_index][priority] = level; */
                        /* Note: you could set the physical output here to the
                           next highest priority, or to the relinquish default
                           if no priorities are set. However, if Out of Service
                           is TRUE, then don't set the physical output.  This
                           comment may apply to the
                           main loop (i.e. check out of service before changing
                           output) */
                        Binary_Value_Present_Value_Set(wp_data->object_instance,
                            level, wp_data->priority, NULL);
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
                Binary_Value_Out_Of_Service_Set(
                    wp_data->object_instance, value.type.Boolean);
            }
            break;
        case PROP_OBJECT_IDENTIFIER:
        case PROP_OBJECT_NAME:
            status = write_property_string_valid(wp_data, &value,
                characterstring_capacity(&Binary_Value_Instance_Names[0]));
            if (status) {
                Binary_Value_Set_Object_Name(wp_data->object_instance,
                    &value.type.Character_String, NULL);
            }
            break;
        case PROP_DESCRIPTION:
        case PROP_OBJECT_TYPE:
        case PROP_STATUS_FLAGS:
        case PROP_EVENT_STATE:
            wp_data->error_class = ERROR_CLASS_PROPERTY;
            wp_data->error_code = ERROR_CODE_WRITE_ACCESS_DENIED;
            break;
        case PROP_PRIORITY_ARRAY:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_ENUMERATED);
            if (status) {
                if (wp_data->array_index > 0 && wp_data->array_index <= BACNET_MAX_PRIORITY) {
                    if (value.type.Enumerated <= MAX_BINARY_PV) {
                        level = (BACNET_BINARY_PV)value.type.Enumerated;
                        Binary_Value_Priority_Array_Set(wp_data->object_instance, level,
                            wp_data->array_index, NULL);
                    } else {
                        status = false;
                        wp_data->error_class = ERROR_CLASS_PROPERTY;
                        wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                    }
                } else {
                    status = false;
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_INVALID_ARRAY_INDEX;
                }
            }
            break;
        case PROP_RELINQUISH_DEFAULT:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_ENUMERATED);
            if (status) {
                if (value.type.Enumerated <= MAX_BINARY_PV) {
                    level = (BACNET_BINARY_PV)value.type.Enumerated;
                    object_index = Binary_Value_Instance_To_Index(
                        wp_data->object_instance);
                    Binary_Value_Relinquish_Defaults[object_index - 1] = level;
#if defined(MQTT)
                    if (yaml_config_mqtt_enable()) {
                        mqtt_publish_topic(OBJECT_BINARY_VALUE, wp_data->object_instance, PROP_RELINQUISH_DEFAULT,
                            MQTT_TOPIC_VALUE_INTEGER, &level, NULL);
                    }
#endif /* defined(MQTT) */
                } else {
                    status = false;
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                }
            }
            break;
        default:
            wp_data->error_class = ERROR_CLASS_PROPERTY;
            wp_data->error_code = ERROR_CODE_UNKNOWN_PROPERTY;
            break;
    }

    return status;
}
