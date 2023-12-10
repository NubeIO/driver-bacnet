/**************************************************************************
 *
 * Copyright (C) 2006 Steve Karg <skarg@users.sourceforge.net>
 * Copyright (C) 2011 Krzysztof Malorny <malornykrzysztof@gmail.com>
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

/* Analog Value Objects - customize for your use */

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "bacnet/bacdef.h"
#include "bacnet/bacdcode.h"
#include "bacnet/bacenum.h"
#include "bacnet/bacapp.h"
#include "bacnet/bactext.h"
#include "bacnet/config.h" /* the custom stuff */
#include "bacnet/basic/object/device.h"
#include "bacnet/basic/services.h"
#include "bacnet/basic/object/av.h"
#if defined(MQTT)
#include "MQTTClient.h"
#include "mqtt_client.h"
#endif /* defined(MQTT) */
#if defined(YAML_CONFIG)
#include "yaml_config.h"
#endif /* defined(YAML_CONFIG) */

#ifndef MAX_ANALOG_VALUES
#define MAX_ANALOG_VALUES 4
#endif

/* Run-time Analog Value Instances */
static int Analog_Value_Instances = 0;

static ANALOG_VALUE_DESCR *AV_Descr = NULL;

#define AV_LEVEL_NULL 255
#define AV_RELINQUISH_DEFAULT 0

/* These three arrays are used by the ReadPropertyMultiple handler */
static const int Analog_Value_Properties_Required[] = { PROP_OBJECT_IDENTIFIER,
    PROP_OBJECT_NAME, PROP_OBJECT_TYPE, PROP_PRESENT_VALUE, PROP_STATUS_FLAGS,
    PROP_EVENT_STATE, PROP_OUT_OF_SERVICE, PROP_UNITS, PROP_PRIORITY_ARRAY,
    PROP_RELINQUISH_DEFAULT, -1 };

static const int Analog_Value_Properties_Optional[] = { PROP_DESCRIPTION,
    PROP_COV_INCREMENT,
#if defined(INTRINSIC_REPORTING)
    PROP_TIME_DELAY, PROP_NOTIFICATION_CLASS, PROP_HIGH_LIMIT, PROP_LOW_LIMIT,
    PROP_DEADBAND, PROP_LIMIT_ENABLE, PROP_EVENT_ENABLE, PROP_ACKED_TRANSITIONS,
    PROP_NOTIFY_TYPE, PROP_EVENT_TIME_STAMPS,
#endif
    -1 };

static const int Analog_Value_Properties_Proprietary[] = { -1 };

/* Analog Value Instances Object Name */
static BACNET_CHARACTER_STRING *Analog_Value_Instance_Names = NULL;

/**
 * Initialize the pointers for the required, the optional and the properitary
 * value properties.
 *
 * @param pRequired - Pointer to the pointer of required values.
 * @param pOptional - Pointer to the pointer of optional values.
 * @param pProprietary - Pointer to the pointer of properitary values.
 */
void Analog_Value_Property_Lists(
    const int **pRequired, const int **pOptional, const int **pProprietary)
{
    if (pRequired) {
        *pRequired = Analog_Value_Properties_Required;
    }
    if (pOptional) {
        *pOptional = Analog_Value_Properties_Optional;
    }
    if (pProprietary) {
        *pProprietary = Analog_Value_Properties_Proprietary;
    }

    return;
}

/**
 * Initialize the analog values.
 */
void Analog_Value_Init(void)
{
    char buf[51];
    char *pEnv;
    unsigned i;
#if defined(INTRINSIC_REPORTING)
    unsigned j;
#endif

#if defined(YAML_CONFIG)
    Analog_Value_Instances = yaml_config_av_max();
    if (Analog_Value_Instances == 0) {
#endif
    pEnv = getenv("AV");
    if (pEnv) {
        Analog_Value_Instances = atoi(pEnv);
    }
#if defined(YAML_CONFIG)
    }
#endif

    if (Analog_Value_Instances > 0) {
        AV_Descr = malloc(Analog_Value_Instances * sizeof(ANALOG_VALUE_DESCR));
        Analog_Value_Instance_Names = malloc(Analog_Value_Instances * sizeof(BACNET_CHARACTER_STRING));
    }

    for (i = 0; i < Analog_Value_Instances; i++) {
        memset(&AV_Descr[i], 0x00, sizeof(ANALOG_VALUE_DESCR));
        AV_Descr[i].Present_Value = 0.0;
        AV_Descr[i].Units = UNITS_NO_UNITS;
        AV_Descr[i].Prior_Value = 0.0f;
        AV_Descr[i].COV_Increment = 1.0f;
        AV_Descr[i].Changed = false;
#if defined(INTRINSIC_REPORTING)
        AV_Descr[i].Event_State = EVENT_STATE_NORMAL;
        /* notification class not connected */
        AV_Descr[i].Notification_Class = BACNET_MAX_INSTANCE;
        /* initialize Event time stamps using wildcards
           and set Acked_transitions */
        for (j = 0; j < MAX_BACNET_EVENT_TRANSITION; j++) {
            datetime_wildcard_set(&AV_Descr[i].Event_Time_Stamps[j]);
            AV_Descr[i].Acked_Transitions[j].bIsAcked = true;
        }

        /* Set handler for GetEventInformation function */
        handler_get_event_information_set(
            OBJECT_ANALOG_VALUE, Analog_Value_Event_Information);
        /* Set handler for AcknowledgeAlarm function */
        handler_alarm_ack_set(OBJECT_ANALOG_VALUE, Analog_Value_Alarm_Ack);
        /* Set handler for GetAlarmSummary Service */
        handler_get_alarm_summary_set(
            OBJECT_ANALOG_VALUE, Analog_Value_Alarm_Summary);
#endif

        sprintf(buf, "AV_%d_SPARE", i + 1);
        characterstring_init_ansi(&Analog_Value_Instance_Names[i], buf);

        for (j = 0; j < BACNET_MAX_PRIORITY; j++) {
            AV_Descr[i].Present_Value_Level[j] = AV_LEVEL_NULL;
        }

        AV_Descr[i].Relinquish_Default = AV_RELINQUISH_DEFAULT;
    }
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
bool Analog_Value_Valid_Instance(uint32_t object_instance)
{
    if (object_instance > 0 && object_instance <= Analog_Value_Instances) {
        return true;
    }

    return false;
}

/**
 * Return the count of analog values.
 *
 * @return Count of analog values.
 */
unsigned Analog_Value_Count(void)
{
    return (Analog_Value_Instances);
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
uint32_t Analog_Value_Index_To_Instance(unsigned index)
{
    return index;
}

/**
 * We simply have 0-n object instances.  Yours might be
 * more complex, and then you need to return the index
 * that correlates to the correct instance number
 *
 * @param object_instance Object instance
 *
 * @return Index in the object table.
 */
unsigned Analog_Value_Instance_To_Index(uint32_t object_instance)
{
    unsigned index = Analog_Value_Instances;

    if (object_instance > 0 && object_instance <= Analog_Value_Instances) {
        index = object_instance;
    }

    return index;
}

/**
 * This function is used to detect a value change,
 * using the new value compared against the prior
 * value, using a delta as threshold.
 *
 * This method will update the COV-changed attribute.
 *
 * @param index  Object index
 * @param value  Given present value.
 */
static void Analog_Value_COV_Detect(unsigned int index, float value)
{
    float prior_value = 0.0;
    float cov_increment = 0.0;
    float cov_delta = 0.0;

    if (index > 0 && index <= Analog_Value_Instances) {
        prior_value = AV_Descr[index - 1].Prior_Value;
        cov_increment = AV_Descr[index - 1].COV_Increment;
        if (prior_value > value) {
            cov_delta = prior_value - value;
        } else {
            cov_delta = value - prior_value;
        }
        if (cov_delta >= cov_increment) {
            AV_Descr[index - 1].Changed = true;
            AV_Descr[index - 1].Prior_Value = value;
        }
    }
}

/**
 * For a given object instance-number, sets the present-value at a given
 * priority 1..16.
 *
 * @param  object_instance - object-instance number of the object
 * @param  value - floating point analog value
 * @param  priority - priority 1..16
 *
 * @return  true if values are within range and present-value is set.
 */
bool Analog_Value_Present_Value_Set(
    uint32_t object_instance, float value, uint8_t priority, char *uuid, int bacnet_client)
{
    unsigned index = 0;
    bool status = false;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        if (priority && (priority <= BACNET_MAX_PRIORITY)) {
            Analog_Value_COV_Detect(index - 1, value);
            AV_Descr[index - 1].Present_Value_Level[priority - 1] = value;
            status = true;
#if defined(MQTT)
            /* if (yaml_config_mqtt_enable() && !bacnet_client) {
                if (value == AV_LEVEL_NULL) {
                    mqtt_publish_topic(OBJECT_ANALOG_VALUE, object_instance, PROP_PRESENT_VALUE,
                        MQTT_TOPIC_VALUE_STRING, "null", uuid);
                } else {
                    mqtt_publish_topic(OBJECT_ANALOG_VALUE, object_instance, PROP_PRESENT_VALUE,
                        MQTT_TOPIC_VALUE_FLOAT, &value, uuid);
                }
                publish_av_priority_array(object_instance, uuid);
            } */
#endif /* defined(MQTT) */
        }
    }

    return status;
}

bool Analog_Value_Priority_Array_Set(
    uint32_t object_instance, float value, uint8_t priority, char *uuid)
{
    unsigned index = 0;
    bool status = false;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        if (priority && (priority <= BACNET_MAX_PRIORITY)) {
            AV_Descr[index - 1].Present_Value_Level[priority - 1] = value;
            status = true;
#if defined(MQTT)
            /* if (yaml_config_mqtt_enable()) {
                publish_av_priority_array(object_instance, uuid);
            } */
#endif /* defined(MQTT) */
        }
    }

    return status;
}

bool Analog_Value_Priority_Array_Set2(
    uint32_t object_instance, float value, uint8_t priority)
{
    unsigned index = 0;
    bool status = false;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        if (priority && (priority <= BACNET_MAX_PRIORITY)) {
            AV_Descr[index -1].Present_Value_Level[priority - 1] = value;
            status = true;
        }
    }

    return status;
}

/**
 * For a given object instance-number, return the present value.
 *
 * @param  object_instance - object-instance number of the object
 *
 * @return  Present value
 */
float Analog_Value_Present_Value(uint32_t object_instance)
{
    float value = AV_RELINQUISH_DEFAULT;
    unsigned index = 0;
    unsigned i = 0;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        value = AV_Descr[index].Relinquish_Default;
        for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
            if (AV_Descr[index - 1].Present_Value_Level[i] != AV_LEVEL_NULL) {
                value = AV_Descr[index - 1].Present_Value_Level[i];
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
bool Analog_Value_Object_Name(
    uint32_t object_instance, BACNET_CHARACTER_STRING *object_name)
{
    bool status = false;
    unsigned index = 0;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        status = characterstring_copy(object_name, &Analog_Value_Instance_Names[index - 1]);
    }

    return status;
}

bool Analog_Value_Set_Object_Name(
    uint32_t object_instance, BACNET_CHARACTER_STRING *object_name, char *uuid, int bacnet_client)
{
    bool status = false;
    unsigned index = 0;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        if (!characterstring_same(&Analog_Value_Instance_Names[index - 1], object_name)) {
            status = characterstring_copy(&Analog_Value_Instance_Names[index - 1], object_name);
        }
#if defined(MQTT)
        /* if (yaml_config_mqtt_enable() && !bacnet_client) {
            mqtt_publish_topic(OBJECT_ANALOG_VALUE, object_instance, PROP_OBJECT_NAME,
                MQTT_TOPIC_VALUE_BACNET_STRING, object_name, uuid);
        } */
#endif /* defined(MQTT) */
    }

    return status;
}

/**
 * For a given object instance-number, gets the event-state property value
 *
 * @param  object_instance - object-instance number of the object
 *
 * @return  event-state property value
 */
unsigned Analog_Value_Event_State(uint32_t object_instance)
{
    unsigned state = EVENT_STATE_NORMAL;
#if defined(INTRINSIC_REPORTING)
    unsigned index = 0;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        state = AV_Descr[index - 1].Event_State;
    }
#endif

    return state;
}

/**
 * For a given object instance-number, determines if the COV flag
 * has been triggered.
 *
 * @param  object_instance - object-instance number of the object
 *
 * @return  true if the COV flag is set
 */
bool Analog_Value_Change_Of_Value(uint32_t object_instance)
{
    unsigned index = 0;
    bool changed = false;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        changed = AV_Descr[index - 1].Changed;
    }

    return changed;
}

/**
 * For a given object instance-number, clears the COV flag
 *
 * @param  object_instance - object-instance number of the object
 */
void Analog_Value_Change_Of_Value_Clear(uint32_t object_instance)
{
    unsigned index = 0;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        AV_Descr[index - 1].Changed = false;
    }
}

/**
 * For a given object instance-number, loads the value_list with the COV data.
 *
 * @param  object_instance - object-instance number of the object
 * @param  value_list - list of COV data
 *
 * @return  true if the value list is encoded
 */
bool Analog_Value_Encode_Value_List(
    uint32_t object_instance, BACNET_PROPERTY_VALUE *value_list)
{
    bool status = false;

    if (value_list) {
        value_list->propertyIdentifier = PROP_PRESENT_VALUE;
        value_list->propertyArrayIndex = BACNET_ARRAY_ALL;
        value_list->value.context_specific = false;
        value_list->value.tag = BACNET_APPLICATION_TAG_REAL;
        value_list->value.type.Real =
            Analog_Value_Present_Value(object_instance);
        value_list->value.next = NULL;
        value_list->priority = BACNET_NO_PRIORITY;
        value_list = value_list->next;
    }
    if (value_list) {
        value_list->propertyIdentifier = PROP_STATUS_FLAGS;
        value_list->propertyArrayIndex = BACNET_ARRAY_ALL;
        value_list->value.context_specific = false;
        value_list->value.tag = BACNET_APPLICATION_TAG_BIT_STRING;
        bitstring_init(&value_list->value.type.Bit_String);
        if (Analog_Value_Event_State(object_instance) == EVENT_STATE_NORMAL) {
            bitstring_set_bit(&value_list->value.type.Bit_String,
                STATUS_FLAG_IN_ALARM, false);
        } else {
            bitstring_set_bit(&value_list->value.type.Bit_String,
                STATUS_FLAG_IN_ALARM, true);
        }
        bitstring_set_bit(
            &value_list->value.type.Bit_String, STATUS_FLAG_FAULT, false);
        bitstring_set_bit(
            &value_list->value.type.Bit_String, STATUS_FLAG_OVERRIDDEN, false);
        if (Analog_Value_Out_Of_Service(object_instance)) {
            bitstring_set_bit(&value_list->value.type.Bit_String,
                STATUS_FLAG_OUT_OF_SERVICE, true);
        } else {
            bitstring_set_bit(&value_list->value.type.Bit_String,
                STATUS_FLAG_OUT_OF_SERVICE, false);
        }
        value_list->value.next = NULL;
        value_list->priority = BACNET_NO_PRIORITY;
        value_list->next = NULL;
        status = true;
    }

    return status;
}

float Analog_Value_COV_Increment(uint32_t object_instance)
{
    unsigned index = 0;
    float value = 0;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        value = AV_Descr[index - 1].COV_Increment;
    }

    return value;
}

void Analog_Value_COV_Increment_Set(uint32_t object_instance, float value)
{
    unsigned index = 0;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        AV_Descr[index - 1].COV_Increment = value;
        Analog_Value_COV_Detect(index - 1, Analog_Value_Present_Value(object_instance));
    }
}

bool Analog_Value_Out_Of_Service(uint32_t object_instance)
{
    unsigned index = 0;
    bool value = false;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        value = AV_Descr[index - 1].Out_Of_Service;
    }

    return value;
}

void Analog_Value_Out_Of_Service_Set(uint32_t object_instance, bool value)
{
    unsigned index = 0;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        if (AV_Descr[index - 1].Out_Of_Service != value) {
            AV_Descr[index - 1].Changed = true;
        }
        AV_Descr[index - 1].Out_Of_Service = value;
    }
}

float Analog_Value_Relinquish_Default(uint32_t object_instance)
{
    float value = AV_RELINQUISH_DEFAULT;
    unsigned index = 0;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        value = AV_Descr[index - 1].Relinquish_Default;
    }

    return value;
}

/**
 * Return the requested property of the analog value.
 *
 * @param rpdata  Property requested, see for BACNET_READ_PROPERTY_DATA details.
 *
 * @return apdu len, or BACNET_STATUS_ERROR on error
 */
int Analog_Value_Read_Property(BACNET_READ_PROPERTY_DATA *rpdata)
{
    int apdu_len = 0; /* return value */
    BACNET_BIT_STRING bit_string;
    BACNET_CHARACTER_STRING char_string;
    float real_value = (float)1.414;
    unsigned object_index = 0;
    bool state = false;
    uint8_t *apdu = NULL;
    ANALOG_VALUE_DESCR *CurrentAV;
#if defined(INTRINSIC_REPORTING)
    int len = 0;
    unsigned i = 0;
#endif

    /* Valid data? */
    if (rpdata == NULL) {
        return 0;
    }
    if ((rpdata->application_data == NULL) ||
        (rpdata->application_data_len == 0)) {
        return 0;
    }

    apdu = rpdata->application_data;

    object_index = Analog_Value_Instance_To_Index(rpdata->object_instance);
    if (object_index < 1 || object_index > Analog_Value_Instances) {
        rpdata->error_class = ERROR_CLASS_OBJECT;
        rpdata->error_code = ERROR_CODE_UNKNOWN_OBJECT;
        return BACNET_STATUS_ERROR;
    }

    CurrentAV = &AV_Descr[object_index - 1];

    switch (rpdata->object_property) {
        case PROP_OBJECT_IDENTIFIER:
            apdu_len = encode_application_object_id(
                &apdu[0], OBJECT_ANALOG_VALUE, rpdata->object_instance);
            break;

        case PROP_OBJECT_NAME:
        case PROP_DESCRIPTION:
            if (Analog_Value_Object_Name(
                    rpdata->object_instance, &char_string)) {
                apdu_len =
                    encode_application_character_string(&apdu[0], &char_string);
            }
            break;

        case PROP_OBJECT_TYPE:
            apdu_len =
                encode_application_enumerated(&apdu[0], OBJECT_ANALOG_VALUE);
            break;

        case PROP_PRESENT_VALUE:
            real_value = Analog_Value_Present_Value(rpdata->object_instance);
            apdu_len = encode_application_real(&apdu[0], real_value);
            break;

        case PROP_STATUS_FLAGS:
            bitstring_init(&bit_string);
            bitstring_set_bit(&bit_string, STATUS_FLAG_IN_ALARM,
                Analog_Value_Event_State(rpdata->object_instance) !=
                EVENT_STATE_NORMAL);
            bitstring_set_bit(&bit_string, STATUS_FLAG_FAULT, false);
            bitstring_set_bit(&bit_string, STATUS_FLAG_OVERRIDDEN, false);
            bitstring_set_bit(&bit_string, STATUS_FLAG_OUT_OF_SERVICE,
                CurrentAV->Out_Of_Service);

            apdu_len = encode_application_bitstring(&apdu[0], &bit_string);
            break;

        case PROP_EVENT_STATE:
#if defined(INTRINSIC_REPORTING)
            apdu_len =
                encode_application_enumerated(&apdu[0], CurrentAV->Event_State);
#else
            apdu_len =
                encode_application_enumerated(&apdu[0], EVENT_STATE_NORMAL);
#endif
            break;

        case PROP_OUT_OF_SERVICE:
            state = CurrentAV->Out_Of_Service;
            apdu_len = encode_application_boolean(&apdu[0], state);
            break;

        case PROP_UNITS:
            apdu_len =
                encode_application_enumerated(&apdu[0], CurrentAV->Units);
            break;

        case PROP_COV_INCREMENT:
            apdu_len =
                encode_application_real(&apdu[0], CurrentAV->COV_Increment);
            break;

#if defined(INTRINSIC_REPORTING)
        case PROP_TIME_DELAY:
            apdu_len =
                encode_application_unsigned(&apdu[0], CurrentAV->Time_Delay);
            break;

        case PROP_NOTIFICATION_CLASS:
            apdu_len = encode_application_unsigned(
                &apdu[0], CurrentAV->Notification_Class);
            break;

        case PROP_HIGH_LIMIT:
            apdu_len = encode_application_real(&apdu[0], CurrentAV->High_Limit);
            break;

        case PROP_LOW_LIMIT:
            apdu_len = encode_application_real(&apdu[0], CurrentAV->Low_Limit);
            break;

        case PROP_DEADBAND:
            apdu_len = encode_application_real(&apdu[0], CurrentAV->Deadband);
            break;

        case PROP_LIMIT_ENABLE:
            bitstring_init(&bit_string);
            bitstring_set_bit(&bit_string, 0,
                (CurrentAV->Limit_Enable & EVENT_LOW_LIMIT_ENABLE) ? true
                                                                   : false);
            bitstring_set_bit(&bit_string, 1,
                (CurrentAV->Limit_Enable & EVENT_HIGH_LIMIT_ENABLE) ? true
                                                                    : false);

            apdu_len = encode_application_bitstring(&apdu[0], &bit_string);
            break;

        case PROP_EVENT_ENABLE:
            bitstring_init(&bit_string);
            bitstring_set_bit(&bit_string, TRANSITION_TO_OFFNORMAL,
                (CurrentAV->Event_Enable & EVENT_ENABLE_TO_OFFNORMAL) ? true
                                                                      : false);
            bitstring_set_bit(&bit_string, TRANSITION_TO_FAULT,
                (CurrentAV->Event_Enable & EVENT_ENABLE_TO_FAULT) ? true
                                                                  : false);
            bitstring_set_bit(&bit_string, TRANSITION_TO_NORMAL,
                (CurrentAV->Event_Enable & EVENT_ENABLE_TO_NORMAL) ? true
                                                                   : false);

            apdu_len = encode_application_bitstring(&apdu[0], &bit_string);
            break;

        case PROP_ACKED_TRANSITIONS:
            bitstring_init(&bit_string);
            bitstring_set_bit(&bit_string, TRANSITION_TO_OFFNORMAL,
                CurrentAV->Acked_Transitions[TRANSITION_TO_OFFNORMAL].bIsAcked);
            bitstring_set_bit(&bit_string, TRANSITION_TO_FAULT,
                CurrentAV->Acked_Transitions[TRANSITION_TO_FAULT].bIsAcked);
            bitstring_set_bit(&bit_string, TRANSITION_TO_NORMAL,
                CurrentAV->Acked_Transitions[TRANSITION_TO_NORMAL].bIsAcked);

            apdu_len = encode_application_bitstring(&apdu[0], &bit_string);
            break;

        case PROP_NOTIFY_TYPE:
            apdu_len = encode_application_enumerated(
                &apdu[0], CurrentAV->Notify_Type ? NOTIFY_EVENT : NOTIFY_ALARM);
            break;

        case PROP_EVENT_TIME_STAMPS:
            /* Array element zero is the number of elements in the array */
            if (rpdata->array_index == 0)
                apdu_len = encode_application_unsigned(
                    &apdu[0], MAX_BACNET_EVENT_TRANSITION);
            /* if no index was specified, then try to encode the entire list */
            /* into one packet. */
            else if (rpdata->array_index == BACNET_ARRAY_ALL) {
                for (i = 0; i < MAX_BACNET_EVENT_TRANSITION; i++) {
                    len = encode_opening_tag(
                        &apdu[apdu_len], TIME_STAMP_DATETIME);
                    len += encode_application_date(&apdu[apdu_len + len],
                        &CurrentAV->Event_Time_Stamps[i].date);
                    len += encode_application_time(&apdu[apdu_len + len],
                        &CurrentAV->Event_Time_Stamps[i].time);
                    len += encode_closing_tag(
                        &apdu[apdu_len + len], TIME_STAMP_DATETIME);

                    /* add it if we have room */
                    if ((apdu_len + len) < MAX_APDU)
                        apdu_len += len;
                    else {
                        rpdata->error_code =
                            ERROR_CODE_ABORT_SEGMENTATION_NOT_SUPPORTED;
                        apdu_len = BACNET_STATUS_ABORT;
                        break;
                    }
                }
            } else if (rpdata->array_index <= MAX_BACNET_EVENT_TRANSITION) {
                apdu_len =
                    encode_opening_tag(&apdu[apdu_len], TIME_STAMP_DATETIME);
                apdu_len += encode_application_date(&apdu[apdu_len],
                    &CurrentAV->Event_Time_Stamps[rpdata->array_index].date);
                apdu_len += encode_application_time(&apdu[apdu_len],
                    &CurrentAV->Event_Time_Stamps[rpdata->array_index].time);
                apdu_len +=
                    encode_closing_tag(&apdu[apdu_len], TIME_STAMP_DATETIME);
            } else {
                rpdata->error_class = ERROR_CLASS_PROPERTY;
                rpdata->error_code = ERROR_CODE_INVALID_ARRAY_INDEX;
                apdu_len = BACNET_STATUS_ERROR;
            }
            break;
#endif

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
                    Analog_Value_Instance_To_Index(rpdata->object_instance);
                for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
                    /* FIXME: check if we have room before adding it to APDU */
                    if (AV_Descr[object_index - 1].Present_Value_Level[i] == AV_LEVEL_NULL) {
                        len = encode_application_null(&apdu[apdu_len]);
                    } else {
                        real_value =
                            AV_Descr[object_index - 1].Present_Value_Level[i];
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
                    Analog_Value_Instance_To_Index(rpdata->object_instance);
                if (rpdata->array_index <= BACNET_MAX_PRIORITY) {
                    if (AV_Descr[object_index - 1].Present_Value_Level[rpdata->array_index -
                            1] == AV_LEVEL_NULL) {
                        apdu_len = encode_application_null(&apdu[0]);
                    } else {
                        real_value =
                            AV_Descr[object_index - 1].Present_Value_Level
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
                Analog_Value_Relinquish_Default(rpdata->object_instance);
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
        (rpdata->object_property != PROP_EVENT_TIME_STAMPS) &&
        (rpdata->array_index != BACNET_ARRAY_ALL)) {
        rpdata->error_class = ERROR_CLASS_PROPERTY;
        rpdata->error_code = ERROR_CODE_PROPERTY_IS_NOT_AN_ARRAY;
        apdu_len = BACNET_STATUS_ERROR;
    }

    return apdu_len;
}

/*
 * Publish the values of priority array over mqtt
 */
void publish_av_priority_array(uint32_t object_instance, char *uuid)
{
    float value;
    char buf[1024] = {0};
    char *first = "";
    unsigned index = 0;
    unsigned i;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        strcpy(buf, "{");
        for (i = 0; i < BACNET_MAX_PRIORITY; i++) {
            value = AV_Descr[index - 1].Present_Value_Level[i];
            if (value == AV_LEVEL_NULL) {
                sprintf(&buf[strlen(buf)], "%sNull", first);
            } else {
                sprintf(&buf[strlen(buf)], "%s%.6f", first, value);
            }

            first = ",";
        }

        sprintf(&buf[strlen(buf)], "}");
        if (yaml_config_mqtt_enable()) {
            mqtt_publish_topic(OBJECT_ANALOG_VALUE, object_instance, PROP_PRIORITY_ARRAY,
                MQTT_TOPIC_VALUE_STRING, buf, uuid);
        }
    }
}

/* get the values of priority array */
void get_av_priority_array(uint32_t object_instance, float *pa, int pa_length)
{
    unsigned index = 0;
    unsigned i;
    unsigned max;

    index = Analog_Value_Instance_To_Index(object_instance);
    if (index > 0 && index <= Analog_Value_Instances) {
        max = (pa_length < BACNET_MAX_PRIORITY) ? pa_length : BACNET_MAX_PRIORITY;
        for (i = 0; i < max; i++) {
            pa[i] = AV_Descr[index - 1].Present_Value_Level[i];
        }
    }
}

/**
 * Set the requested property of the analog value.
 *
 * @param wp_data  Property requested, see for BACNET_WRITE_PROPERTY_DATA
 * details.
 *
 * @return true if successful
 */
bool Analog_Value_Write_Property(BACNET_WRITE_PROPERTY_DATA *wp_data)
{
    bool status = false; /* return value */
    unsigned int object_index = 0;
    int len = 0;
    BACNET_APPLICATION_DATA_VALUE value;
    ANALOG_VALUE_DESCR *CurrentAV;

    /* Valid data? */
    if (wp_data == NULL) {
        return false;
    }
    if (wp_data->application_data_len == 0) {
        return false;
    }

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
    if ((wp_data->object_property != PROP_PRIORITY_ARRAY) &&
        (wp_data->object_property != PROP_EVENT_TIME_STAMPS) &&
        (wp_data->array_index != BACNET_ARRAY_ALL)) {
        /*  only array properties can have array options */
        wp_data->error_class = ERROR_CLASS_PROPERTY;
        wp_data->error_code = ERROR_CODE_PROPERTY_IS_NOT_AN_ARRAY;
        return false;
    }

    /* Valid object? */
    object_index = Analog_Value_Instance_To_Index(wp_data->object_instance);
    if (object_index < 1 || object_index > Analog_Value_Instances) {
        wp_data->error_class = ERROR_CLASS_OBJECT;
        wp_data->error_code = ERROR_CODE_UNKNOWN_OBJECT;
        return false;
    }

    CurrentAV = &AV_Descr[object_index - 1];

    switch (wp_data->object_property) {
        case PROP_PRESENT_VALUE:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_REAL);
            if (status) {
                /* Command priority 6 is reserved for use by Minimum On/Off
                   algorithm and may not be used for other purposes in any
                   object. */
                status = Analog_Value_Present_Value_Set(wp_data->object_instance,
                        value.type.Real, wp_data->priority, NULL, true);
                if (wp_data->priority == 6) {
                    /* Command priority 6 is reserved for use by Minimum On/Off
                       algorithm and may not be used for other purposes in any
                       object. */
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_WRITE_ACCESS_DENIED;
                } else if (!status) {
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                }
            } else {
                status = write_property_type_valid(wp_data, &value,
                    BACNET_APPLICATION_TAG_NULL);
                if (status) {
                    status = Analog_Value_Present_Value_Set(wp_data->object_instance,
                        AV_LEVEL_NULL, wp_data->priority, NULL, true);
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
                CurrentAV->Out_Of_Service = value.type.Boolean;
            }
            break;

        case PROP_UNITS:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_ENUMERATED);
            if (status) {
                CurrentAV->Units = value.type.Enumerated;
            }
            break;

        case PROP_COV_INCREMENT:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_REAL);
            if (status) {
                if (value.type.Real >= 0.0) {
                    Analog_Value_COV_Increment_Set(
                        wp_data->object_instance, value.type.Real);
                } else {
                    status = false;
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                }
            }
            break;

#if defined(INTRINSIC_REPORTING)
        case PROP_TIME_DELAY:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_UNSIGNED_INT);
            if (status) {
                CurrentAV->Time_Delay = value.type.Unsigned_Int;
                CurrentAV->Remaining_Time_Delay = CurrentAV->Time_Delay;
            }
            break;

        case PROP_NOTIFICATION_CLASS:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_UNSIGNED_INT);
            if (status) {
                CurrentAV->Notification_Class = value.type.Unsigned_Int;
            }
            break;

        case PROP_HIGH_LIMIT:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_REAL);
            if (status) {
                CurrentAV->High_Limit = value.type.Real;
            }
            break;

        case PROP_LOW_LIMIT:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_REAL);
            if (status) {
                CurrentAV->Low_Limit = value.type.Real;
            }
            break;

        case PROP_DEADBAND:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_REAL);
            if (status) {
                CurrentAV->Deadband = value.type.Real;
            }
            break;

        case PROP_LIMIT_ENABLE:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_BIT_STRING);
            if (status) {
                if (value.type.Bit_String.bits_used == 2) {
                    CurrentAV->Limit_Enable = value.type.Bit_String.value[0];
                } else {
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                    status = false;
                }
            }
            break;

        case PROP_EVENT_ENABLE:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_BIT_STRING);
            if (status) {
                if (value.type.Bit_String.bits_used == 3) {
                    CurrentAV->Event_Enable = value.type.Bit_String.value[0];
                } else {
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                    status = false;
                }
            }
            break;

        case PROP_NOTIFY_TYPE:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_ENUMERATED);
            if (status) {
                switch ((BACNET_NOTIFY_TYPE)value.type.Enumerated) {
                    case NOTIFY_EVENT:
                        CurrentAV->Notify_Type = 1;
                        break;
                    case NOTIFY_ALARM:
                        CurrentAV->Notify_Type = 0;
                        break;
                    default:
                        wp_data->error_class = ERROR_CLASS_PROPERTY;
                        wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                        status = false;
                        break;
                }
            }
            break;
#endif
        case PROP_OBJECT_IDENTIFIER:
        case PROP_OBJECT_NAME:
            status = write_property_string_valid(wp_data, &value,
                characterstring_capacity(&Analog_Value_Instance_Names[0]));
            if (status) {
                Analog_Value_Set_Object_Name(wp_data->object_instance,
                    &value.type.Character_String, NULL, true);
            }
            break;
        case PROP_OBJECT_TYPE:
        case PROP_STATUS_FLAGS:
        case PROP_EVENT_STATE:
        case PROP_DESCRIPTION:
#if defined(INTRINSIC_REPORTING)
        case PROP_ACKED_TRANSITIONS:
        case PROP_EVENT_TIME_STAMPS:
#endif
            wp_data->error_class = ERROR_CLASS_PROPERTY;
            wp_data->error_code = ERROR_CODE_WRITE_ACCESS_DENIED;
            break;
        case PROP_RELINQUISH_DEFAULT:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_REAL);
            if (status) {
                float f_value;
                f_value = value.type.Real;
                object_index = Analog_Value_Instance_To_Index(
                    wp_data->object_instance);
                AV_Descr[object_index - 1].Relinquish_Default = f_value;
#if defined(MQTT)
                /* if (yaml_config_mqtt_enable()) {
                    mqtt_publish_topic(OBJECT_ANALOG_VALUE, wp_data->object_instance, PROP_RELINQUISH_DEFAULT,
                        MQTT_TOPIC_VALUE_FLOAT, &f_value, NULL);
                } */
#endif /* defined(MQTT) */
            }
            break;
        case PROP_PRIORITY_ARRAY:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_REAL);
            if (status) {
                if (wp_data->array_index > 0 && wp_data->array_index <= BACNET_MAX_PRIORITY) {
                    float f_value;
                    f_value = value.type.Real;

                    Analog_Value_Priority_Array_Set(wp_data->object_instance, f_value,
                        wp_data->array_index, NULL);
                } else {
                    status = false;
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_INVALID_ARRAY_INDEX;
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

void Analog_Value_Intrinsic_Reporting(uint32_t object_instance)
{
#if defined(INTRINSIC_REPORTING)
    BACNET_EVENT_NOTIFICATION_DATA event_data;
    BACNET_CHARACTER_STRING msgText;
    ANALOG_VALUE_DESCR *CurrentAV;
    unsigned int object_index;
    uint8_t FromState = 0;
    uint8_t ToState;
    float ExceededLimit = 0.0f;
    float PresentVal = 0.0f;
    bool SendNotify = false;

    object_index = Analog_Value_Instance_To_Index(object_instance);
    if (object_index > 0 && object_index <= Analog_Value_Instances)
        CurrentAV = &AV_Descr[object_index - 1];
    else
        return;

    /* check limits */
    if (!CurrentAV->Limit_Enable)
        return; /* limits are not configured */

    if (CurrentAV->Ack_notify_data.bSendAckNotify) {
        /* clean bSendAckNotify flag */
        CurrentAV->Ack_notify_data.bSendAckNotify = false;
        /* copy toState */
        ToState = CurrentAV->Ack_notify_data.EventState;

#if PRINT_ENABLED
        fprintf(stderr, "Send Acknotification for (%s,%u).\n",
            bactext_object_type_name(OBJECT_ANALOG_VALUE),
            (unsigned)object_instance);
#endif /* PRINT_ENABLED */

        characterstring_init_ansi(&msgText, "AckNotification");

        /* Notify Type */
        event_data.notifyType = NOTIFY_ACK_NOTIFICATION;

        /* Send EventNotification. */
        SendNotify = true;
    } else {
        /* actual Present_Value */
        PresentVal = Analog_Value_Present_Value(object_instance);
        FromState = CurrentAV->Event_State;
        switch (CurrentAV->Event_State) {
            case EVENT_STATE_NORMAL:
                /* A TO-OFFNORMAL event is generated under these conditions:
                   (a) the Present_Value must exceed the High_Limit for a
                   minimum period of time, specified in the Time_Delay property,
                   and (b) the HighLimitEnable flag must be set in the
                   Limit_Enable property, and
                   (c) the TO-OFFNORMAL flag must be set in the Event_Enable
                   property. */
                if ((PresentVal > CurrentAV->High_Limit) &&
                    ((CurrentAV->Limit_Enable & EVENT_HIGH_LIMIT_ENABLE) ==
                        EVENT_HIGH_LIMIT_ENABLE) &&
                    ((CurrentAV->Event_Enable & EVENT_ENABLE_TO_OFFNORMAL) ==
                        EVENT_ENABLE_TO_OFFNORMAL)) {
                    if (!CurrentAV->Remaining_Time_Delay)
                        CurrentAV->Event_State = EVENT_STATE_HIGH_LIMIT;
                    else
                        CurrentAV->Remaining_Time_Delay--;
                    break;
                }

                /* A TO-OFFNORMAL event is generated under these conditions:
                   (a) the Present_Value must exceed the Low_Limit plus the
                   Deadband for a minimum period of time, specified in the
                   Time_Delay property, and (b) the LowLimitEnable flag must be
                   set in the Limit_Enable property, and
                   (c) the TO-NORMAL flag must be set in the Event_Enable
                   property. */
                if ((PresentVal < CurrentAV->Low_Limit) &&
                    ((CurrentAV->Limit_Enable & EVENT_LOW_LIMIT_ENABLE) ==
                        EVENT_LOW_LIMIT_ENABLE) &&
                    ((CurrentAV->Event_Enable & EVENT_ENABLE_TO_OFFNORMAL) ==
                        EVENT_ENABLE_TO_OFFNORMAL)) {
                    if (!CurrentAV->Remaining_Time_Delay)
                        CurrentAV->Event_State = EVENT_STATE_LOW_LIMIT;
                    else
                        CurrentAV->Remaining_Time_Delay--;
                    break;
                }
                /* value of the object is still in the same event state */
                CurrentAV->Remaining_Time_Delay = CurrentAV->Time_Delay;
                break;

            case EVENT_STATE_HIGH_LIMIT:
                /* Once exceeded, the Present_Value must fall below the
                   High_Limit minus the Deadband before a TO-NORMAL event is
                   generated under these conditions: (a) the Present_Value must
                   fall below the High_Limit minus the Deadband for a minimum
                   period of time, specified in the Time_Delay property, and (b)
                   the HighLimitEnable flag must be set in the Limit_Enable
                   property, and (c) the TO-NORMAL flag must be set in the
                   Event_Enable property. */
                if ((PresentVal <
                        CurrentAV->High_Limit - CurrentAV->Deadband) &&
                    ((CurrentAV->Limit_Enable & EVENT_HIGH_LIMIT_ENABLE) ==
                        EVENT_HIGH_LIMIT_ENABLE) &&
                    ((CurrentAV->Event_Enable & EVENT_ENABLE_TO_NORMAL) ==
                        EVENT_ENABLE_TO_NORMAL)) {
                    if (!CurrentAV->Remaining_Time_Delay)
                        CurrentAV->Event_State = EVENT_STATE_NORMAL;
                    else
                        CurrentAV->Remaining_Time_Delay--;
                    break;
                }
                /* value of the object is still in the same event state */
                CurrentAV->Remaining_Time_Delay = CurrentAV->Time_Delay;
                break;

            case EVENT_STATE_LOW_LIMIT:
                /* Once the Present_Value has fallen below the Low_Limit,
                   the Present_Value must exceed the Low_Limit plus the Deadband
                   before a TO-NORMAL event is generated under these conditions:
                   (a) the Present_Value must exceed the Low_Limit plus the
                   Deadband for a minimum period of time, specified in the
                   Time_Delay property, and (b) the LowLimitEnable flag must be
                   set in the Limit_Enable property, and
                   (c) the TO-NORMAL flag must be set in the Event_Enable
                   property. */
                if ((PresentVal > CurrentAV->Low_Limit + CurrentAV->Deadband) &&
                    ((CurrentAV->Limit_Enable & EVENT_LOW_LIMIT_ENABLE) ==
                        EVENT_LOW_LIMIT_ENABLE) &&
                    ((CurrentAV->Event_Enable & EVENT_ENABLE_TO_NORMAL) ==
                        EVENT_ENABLE_TO_NORMAL)) {
                    if (!CurrentAV->Remaining_Time_Delay)
                        CurrentAV->Event_State = EVENT_STATE_NORMAL;
                    else
                        CurrentAV->Remaining_Time_Delay--;
                    break;
                }
                /* value of the object is still in the same event state */
                CurrentAV->Remaining_Time_Delay = CurrentAV->Time_Delay;
                break;

            default:
                return; /* shouldn't happen */
        } /* switch (FromState) */

        ToState = CurrentAV->Event_State;

        if (FromState != ToState) {
            /* Event_State has changed.
               Need to fill only the basic parameters of this type of event.
               Other parameters will be filled in common function. */

            switch (ToState) {
                case EVENT_STATE_HIGH_LIMIT:
                    ExceededLimit = CurrentAV->High_Limit;
                    characterstring_init_ansi(&msgText, "Goes to high limit");
                    break;

                case EVENT_STATE_LOW_LIMIT:
                    ExceededLimit = CurrentAV->Low_Limit;
                    characterstring_init_ansi(&msgText, "Goes to low limit");
                    break;

                case EVENT_STATE_NORMAL:
                    if (FromState == EVENT_STATE_HIGH_LIMIT) {
                        ExceededLimit = CurrentAV->High_Limit;
                        characterstring_init_ansi(
                            &msgText, "Back to normal state from high limit");
                    } else {
                        ExceededLimit = CurrentAV->Low_Limit;
                        characterstring_init_ansi(
                            &msgText, "Back to normal state from low limit");
                    }
                    break;

                default:
                    ExceededLimit = 0;
                    break;
            } /* switch (ToState) */

#if PRINT_ENABLED
            fprintf(stderr, "Event_State for (%s,%u) goes from %s to %s.\n",
                bactext_object_type_name(OBJECT_ANALOG_VALUE),
                (unsigned)object_instance,
                bactext_event_state_name(FromState),
                bactext_event_state_name(ToState));
#endif /* PRINT_ENABLED */

            /* Notify Type */
            event_data.notifyType = CurrentAV->Notify_Type;

            /* Send EventNotification. */
            SendNotify = true;
        }
    }

    if (SendNotify) {
        /* Event Object Identifier */
        event_data.eventObjectIdentifier.type = OBJECT_ANALOG_VALUE;
        event_data.eventObjectIdentifier.instance = object_instance;

        /* Time Stamp */
        event_data.timeStamp.tag = TIME_STAMP_DATETIME;
        Device_getCurrentDateTime(&event_data.timeStamp.value.dateTime);

        if (event_data.notifyType != NOTIFY_ACK_NOTIFICATION) {
            /* fill Event_Time_Stamps */
            switch (ToState) {
                case EVENT_STATE_HIGH_LIMIT:
                case EVENT_STATE_LOW_LIMIT:
                    CurrentAV->Event_Time_Stamps[TRANSITION_TO_OFFNORMAL] =
                        event_data.timeStamp.value.dateTime;
                    break;

                case EVENT_STATE_FAULT:
                    CurrentAV->Event_Time_Stamps[TRANSITION_TO_FAULT] =
                        event_data.timeStamp.value.dateTime;
                    break;

                case EVENT_STATE_NORMAL:
                    CurrentAV->Event_Time_Stamps[TRANSITION_TO_NORMAL] =
                        event_data.timeStamp.value.dateTime;
                    break;
            }
        }

        /* Notification Class */
        event_data.notificationClass = CurrentAV->Notification_Class;

        /* Event Type */
        event_data.eventType = EVENT_OUT_OF_RANGE;

        /* Message Text */
        event_data.messageText = &msgText;

        /* Notify Type */
        /* filled before */

        /* From State */
        if (event_data.notifyType != NOTIFY_ACK_NOTIFICATION)
            event_data.fromState = FromState;

        /* To State */
        event_data.toState = CurrentAV->Event_State;

        /* Event Values */
        if (event_data.notifyType != NOTIFY_ACK_NOTIFICATION) {
            /* Value that exceeded a limit. */
            event_data.notificationParams.outOfRange.exceedingValue =
                PresentVal;
            /* Status_Flags of the referenced object. */
            bitstring_init(
                &event_data.notificationParams.outOfRange.statusFlags);
            bitstring_set_bit(
                &event_data.notificationParams.outOfRange.statusFlags,
                STATUS_FLAG_IN_ALARM,
                CurrentAV->Event_State != EVENT_STATE_NORMAL);
            bitstring_set_bit(
                &event_data.notificationParams.outOfRange.statusFlags,
                STATUS_FLAG_FAULT, false);
            bitstring_set_bit(
                &event_data.notificationParams.outOfRange.statusFlags,
                STATUS_FLAG_OVERRIDDEN, false);
            bitstring_set_bit(
                &event_data.notificationParams.outOfRange.statusFlags,
                STATUS_FLAG_OUT_OF_SERVICE, CurrentAV->Out_Of_Service);
            /* Deadband used for limit checking. */
            event_data.notificationParams.outOfRange.deadband =
                CurrentAV->Deadband;
            /* Limit that was exceeded. */
            event_data.notificationParams.outOfRange.exceededLimit =
                ExceededLimit;
        }

        /* add data from notification class */
        Notification_Class_common_reporting_function(&event_data);

        /* Ack required */
        if ((event_data.notifyType != NOTIFY_ACK_NOTIFICATION) &&
            (event_data.ackRequired == true)) {
            switch (event_data.toState) {
                case EVENT_STATE_OFFNORMAL:
                case EVENT_STATE_HIGH_LIMIT:
                case EVENT_STATE_LOW_LIMIT:
                    CurrentAV->Acked_Transitions[TRANSITION_TO_OFFNORMAL]
                        .bIsAcked = false;
                    CurrentAV->Acked_Transitions[TRANSITION_TO_OFFNORMAL]
                        .Time_Stamp = event_data.timeStamp.value.dateTime;
                    break;

                case EVENT_STATE_FAULT:
                    CurrentAV->Acked_Transitions[TRANSITION_TO_FAULT].bIsAcked =
                        false;
                    CurrentAV->Acked_Transitions[TRANSITION_TO_FAULT]
                        .Time_Stamp = event_data.timeStamp.value.dateTime;
                    break;

                case EVENT_STATE_NORMAL:
                    CurrentAV->Acked_Transitions[TRANSITION_TO_NORMAL]
                        .bIsAcked = false;
                    CurrentAV->Acked_Transitions[TRANSITION_TO_NORMAL]
                        .Time_Stamp = event_data.timeStamp.value.dateTime;
                    break;

                default: /* shouldn't happen */
                    break;
            }
        }
    }
#endif /* defined(INTRINSIC_REPORTING) */
}

#if defined(INTRINSIC_REPORTING)
int Analog_Value_Event_Information(
    unsigned index, BACNET_GET_EVENT_INFORMATION_DATA *getevent_data)
{
    bool IsNotAckedTransitions;
    bool IsActiveEvent;
    int i;

    /* check index */
    if (index > 0 && index <= Analog_Value_Instances) {
        /* Event_State not equal to NORMAL */
        IsActiveEvent = (AV_Descr[index - 1].Event_State != EVENT_STATE_NORMAL);

        /* Acked_Transitions property, which has at least one of the bits
           (TO-OFFNORMAL, TO-FAULT, TONORMAL) set to FALSE. */
        IsNotAckedTransitions =
            (AV_Descr[index - 1]
                    .Acked_Transitions[TRANSITION_TO_OFFNORMAL]
                    .bIsAcked == false) |
            (AV_Descr[index - 1].Acked_Transitions[TRANSITION_TO_FAULT].bIsAcked ==
                false) |
            (AV_Descr[index - 1].Acked_Transitions[TRANSITION_TO_NORMAL].bIsAcked ==
                false);
    } else
        return -1; /* end of list  */

    if ((IsActiveEvent) || (IsNotAckedTransitions)) {
        /* Object Identifier */
        getevent_data->objectIdentifier.type = OBJECT_ANALOG_VALUE;
        getevent_data->objectIdentifier.instance =
            Analog_Value_Index_To_Instance(index - 1);
        /* Event State */
        getevent_data->eventState = AV_Descr[index - 1].Event_State;
        /* Acknowledged Transitions */
        bitstring_init(&getevent_data->acknowledgedTransitions);
        bitstring_set_bit(&getevent_data->acknowledgedTransitions,
            TRANSITION_TO_OFFNORMAL,
            AV_Descr[index - 1]
                .Acked_Transitions[TRANSITION_TO_OFFNORMAL]
                .bIsAcked);
        bitstring_set_bit(&getevent_data->acknowledgedTransitions,
            TRANSITION_TO_FAULT,
            AV_Descr[index - 1].Acked_Transitions[TRANSITION_TO_FAULT].bIsAcked);
        bitstring_set_bit(&getevent_data->acknowledgedTransitions,
            TRANSITION_TO_NORMAL,
            AV_Descr[index - 1].Acked_Transitions[TRANSITION_TO_NORMAL].bIsAcked);
        /* Event Time Stamps */
        for (i = 0; i < 3; i++) {
            getevent_data->eventTimeStamps[i].tag = TIME_STAMP_DATETIME;
            getevent_data->eventTimeStamps[i].value.dateTime =
                AV_Descr[index].Event_Time_Stamps[i];
        }
        /* Notify Type */
        getevent_data->notifyType = AV_Descr[index].Notify_Type;
        /* Event Enable */
        bitstring_init(&getevent_data->eventEnable);
        bitstring_set_bit(&getevent_data->eventEnable, TRANSITION_TO_OFFNORMAL,
            (AV_Descr[index - 1].Event_Enable & EVENT_ENABLE_TO_OFFNORMAL) ? true
                                                                       : false);
        bitstring_set_bit(&getevent_data->eventEnable, TRANSITION_TO_FAULT,
            (AV_Descr[index - 1].Event_Enable & EVENT_ENABLE_TO_FAULT) ? true
                                                                   : false);
        bitstring_set_bit(&getevent_data->eventEnable, TRANSITION_TO_NORMAL,
            (AV_Descr[index - 1].Event_Enable & EVENT_ENABLE_TO_NORMAL) ? true
                                                                    : false);
        /* Event Priorities */
        Notification_Class_Get_Priorities(
            AV_Descr[index - 1].Notification_Class, getevent_data->eventPriorities);

        return 1; /* active event */
    } else
        return 0; /* no active event at this index */
}

int Analog_Value_Alarm_Ack(
    BACNET_ALARM_ACK_DATA *alarmack_data, BACNET_ERROR_CODE *error_code)
{
    ANALOG_VALUE_DESCR *CurrentAV;
    unsigned int object_index;

    object_index = Analog_Value_Instance_To_Index(
        alarmack_data->eventObjectIdentifier.instance);

    if (object_index > 0 && object_index <= Analog_Value_Instances)
        CurrentAV = &AV_Descr[object_index - 1];
    else {
        *error_code = ERROR_CODE_UNKNOWN_OBJECT;
        return -1;
    }

    switch (alarmack_data->eventStateAcked) {
        case EVENT_STATE_OFFNORMAL:
        case EVENT_STATE_HIGH_LIMIT:
        case EVENT_STATE_LOW_LIMIT:
            if (CurrentAV->Acked_Transitions[TRANSITION_TO_OFFNORMAL]
                    .bIsAcked == false) {
                if (alarmack_data->eventTimeStamp.tag != TIME_STAMP_DATETIME) {
                    *error_code = ERROR_CODE_INVALID_TIME_STAMP;
                    return -1;
                }
                if (datetime_compare(
                        &CurrentAV->Acked_Transitions[TRANSITION_TO_OFFNORMAL]
                             .Time_Stamp,
                        &alarmack_data->eventTimeStamp.value.dateTime) > 0) {
                    *error_code = ERROR_CODE_INVALID_TIME_STAMP;
                    return -1;
                }

                /* Clean transitions flag. */
                CurrentAV->Acked_Transitions[TRANSITION_TO_OFFNORMAL].bIsAcked =
                    true;
            } else if (alarmack_data->eventStateAcked ==
                CurrentAV->Event_State) {
                /* Send ack notification */
            } else {
                *error_code = ERROR_CODE_INVALID_EVENT_STATE;
                return -1;
            }
            break;

        case EVENT_STATE_FAULT:
            if (CurrentAV->Acked_Transitions[TRANSITION_TO_NORMAL].bIsAcked ==
                false) {
                if (alarmack_data->eventTimeStamp.tag != TIME_STAMP_DATETIME) {
                    *error_code = ERROR_CODE_INVALID_TIME_STAMP;
                    return -1;
                }
                if (datetime_compare(
                        &CurrentAV->Acked_Transitions[TRANSITION_TO_NORMAL]
                             .Time_Stamp,
                        &alarmack_data->eventTimeStamp.value.dateTime) > 0) {
                    *error_code = ERROR_CODE_INVALID_TIME_STAMP;
                    return -1;
                }

                /* Clean transitions flag. */
                CurrentAV->Acked_Transitions[TRANSITION_TO_FAULT].bIsAcked =
                    true;
            } else if (alarmack_data->eventStateAcked ==
                CurrentAV->Event_State) {
                /* Send ack notification */
            } else {
                *error_code = ERROR_CODE_INVALID_EVENT_STATE;
                return -1;
            }
            break;

        case EVENT_STATE_NORMAL:
            if (CurrentAV->Acked_Transitions[TRANSITION_TO_FAULT].bIsAcked ==
                false) {
                if (alarmack_data->eventTimeStamp.tag != TIME_STAMP_DATETIME) {
                    *error_code = ERROR_CODE_INVALID_TIME_STAMP;
                    return -1;
                }
                if (datetime_compare(
                        &CurrentAV->Acked_Transitions[TRANSITION_TO_FAULT]
                             .Time_Stamp,
                        &alarmack_data->eventTimeStamp.value.dateTime) > 0) {
                    *error_code = ERROR_CODE_INVALID_TIME_STAMP;
                    return -1;
                }

                /* Clean transitions flag. */
                CurrentAV->Acked_Transitions[TRANSITION_TO_NORMAL].bIsAcked =
                    true;
            } else if (alarmack_data->eventStateAcked ==
                CurrentAV->Event_State) {
                /* Send ack notification */
            } else {
                *error_code = ERROR_CODE_INVALID_EVENT_STATE;
                return -1;
            }
            break;

        default:
            return -2;
    }

    /* Need to send AckNotification. */
    CurrentAV->Ack_notify_data.bSendAckNotify = true;
    CurrentAV->Ack_notify_data.EventState = alarmack_data->eventStateAcked;

    /* Return OK */
    return 1;
}

int Analog_Value_Alarm_Summary(
    unsigned index, BACNET_GET_ALARM_SUMMARY_DATA *getalarm_data)
{
    /* check index */
    if (index > 0 && index <= Analog_Value_Instances) {
        /* Event_State is not equal to NORMAL  and
           Notify_Type property value is ALARM */
        if ((AV_Descr[index - 1].Event_State != EVENT_STATE_NORMAL) &&
            (AV_Descr[index - 1].Notify_Type == NOTIFY_ALARM)) {
            /* Object Identifier */
            getalarm_data->objectIdentifier.type = OBJECT_ANALOG_VALUE;
            getalarm_data->objectIdentifier.instance =
                Analog_Value_Index_To_Instance(index - 1);
            /* Alarm State */
            getalarm_data->alarmState = AV_Descr[index - 1].Event_State;
            /* Acknowledged Transitions */
            bitstring_init(&getalarm_data->acknowledgedTransitions);
            bitstring_set_bit(&getalarm_data->acknowledgedTransitions,
                TRANSITION_TO_OFFNORMAL,
                AV_Descr[index - 1]
                    .Acked_Transitions[TRANSITION_TO_OFFNORMAL]
                    .bIsAcked);
            bitstring_set_bit(&getalarm_data->acknowledgedTransitions,
                TRANSITION_TO_FAULT,
                AV_Descr[index - 1]
                    .Acked_Transitions[TRANSITION_TO_FAULT]
                    .bIsAcked);
            bitstring_set_bit(&getalarm_data->acknowledgedTransitions,
                TRANSITION_TO_NORMAL,
                AV_Descr[index - 1]
                    .Acked_Transitions[TRANSITION_TO_NORMAL]
                    .bIsAcked);

            return 1; /* active alarm */
        } else
            return 0; /* no active alarm at this index */
    } else
        return -1; /* end of list  */
}
#endif /* defined(INTRINSIC_REPORTING) */
