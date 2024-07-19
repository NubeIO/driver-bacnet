/**************************************************************************
 *
 * Copyright (C) 2009 Steve Karg <skarg@users.sourceforge.net>
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

/* Multi-state Input Objects */

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
#include "bacnet/basic/object/ms-input.h"
#include "bacnet/basic/services.h"
#if defined(MQTT)
#include "MQTTClient.h"
#include "mqtt_client.h"
#endif /* defined(MQTT) */
#if defined(YAML_CONFIG)
#include "yaml_config.h"
#endif /* defined(YAML_CONFIG) */

/* number of demo objects */
#ifndef MAX_MULTISTATE_INPUTS
#define MAX_MULTISTATE_INPUTS 4
#endif

/* how many states? 1 to 254 states - 0 is not allowed. */
#ifndef MULTISTATE_NUMBER_OF_STATES
#define MULTISTATE_NUMBER_OF_STATES (254)
#endif

/* Run-time Multistate Input Instances */
static int Multistate_Input_Instances = 0;

/* Here is our Present Value */
static uint8_t *Present_Value = NULL;
/* Writable out-of-service allows others to manipulate our Present Value */
static bool *Out_Of_Service = NULL;
static char **Object_Name = NULL;
static char **Object_Description = NULL;
static char ***State_Text = NULL;

/* These three arrays are used by the ReadPropertyMultiple handler */
static const int Properties_Required[] = { PROP_OBJECT_IDENTIFIER,
    PROP_OBJECT_NAME, PROP_OBJECT_TYPE, PROP_PRESENT_VALUE, PROP_STATUS_FLAGS,
    PROP_EVENT_STATE, PROP_OUT_OF_SERVICE, PROP_NUMBER_OF_STATES, -1 };

static const int Properties_Optional[] = { PROP_DESCRIPTION, PROP_STATE_TEXT,
    -1 };

static const int Properties_Proprietary[] = { -1 };

void Multistate_Input_Property_Lists(
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

void Multistate_Input_Init(void)
{
    unsigned i;
    unsigned ii;
    char *pEnv;
    char **State_Text_a;

#if defined(YAML_CONFIG)
    Multistate_Input_Instances = yaml_config_msi_max();
    if (Multistate_Input_Instances == 0) {
#endif
    pEnv = getenv("MSI");
    if (pEnv) {
        Multistate_Input_Instances = atoi(pEnv);
    }
#if defined(YAML_CONFIG)
    }
#endif

    if (Multistate_Input_Instances > 0) {
        Present_Value = malloc(Multistate_Input_Instances * sizeof(uint8_t));
        Out_Of_Service = malloc(Multistate_Input_Instances * sizeof(bool));
        memset(Out_Of_Service, 0, Multistate_Input_Instances * sizeof(bool));
        Object_Name = malloc(Multistate_Input_Instances * sizeof(char *));
        Object_Description = malloc(Multistate_Input_Instances * sizeof(char *));
        State_Text = malloc(Multistate_Input_Instances * sizeof(char **));
    }

    /* initialize all the analog output priority arrays to NULL */
    for (i = 0; i < Multistate_Input_Instances; i++) {
        Present_Value[i] = 1;
        Object_Name[i] = malloc(64 * sizeof(char));
        sprintf(&Object_Name[i][0], "MULTISTATE INPUT %u", i + 1);
        Object_Description[i] = malloc(64 * sizeof(char));
        sprintf(&Object_Description[i][0], "MULTISTATE INPUT %u", i + 1);
        State_Text_a = malloc(MULTISTATE_NUMBER_OF_STATES * sizeof(char *));
        for (ii = 0; ii < MULTISTATE_NUMBER_OF_STATES; ii++) {
          State_Text_a[ii] = malloc(64 * sizeof(char));
          memset(&State_Text_a[ii][0], 0, 64 * sizeof(char));
        }

        State_Text[i] = State_Text_a;
    }

    return;
}

/* we simply have 0-n object instances.  Yours might be */
/* more complex, and then you need to return the index */
/* that correlates to the correct instance number */
unsigned Multistate_Input_Instance_To_Index(uint32_t object_instance)
{
    unsigned index = 0;

    if (object_instance > 0 && object_instance <= Multistate_Input_Instances) {
        index = object_instance;
    }

    return index;
}

/* we simply have 0-n object instances.  Yours might be */
/* more complex, and then you need to return the instance */
/* that correlates to the correct index */
uint32_t Multistate_Input_Index_To_Instance(unsigned index)
{
    return index;
}

/* we simply have 0-n object instances.  Yours might be */
/* more complex, and then count how many you have */
unsigned Multistate_Input_Count(void)
{
    return (Multistate_Input_Instances);
}

bool Multistate_Input_Valid_Instance(uint32_t object_instance)
{
    unsigned index = 0; /* offset from instance lookup */

    index = Multistate_Input_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Input_Instances) {
        return true;
    }

    return false;
}

static uint32_t Multistate_Input_Max_States(uint32_t instance)
{
    return MULTISTATE_NUMBER_OF_STATES;
}

uint32_t Multistate_Input_Present_Value(uint32_t object_instance)
{
    uint32_t value = 1;
    unsigned index = 0; /* offset from instance lookup */

    index = Multistate_Input_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Input_Instances) {
        value = Present_Value[index - 1];
    }

    return value;
}

bool Multistate_Input_Present_Value_Set(
    uint32_t object_instance, uint32_t value, char *uuid, int bacnet_client)
{
    bool status = false;
    unsigned index = 0; /* offset from instance lookup */

    index = Multistate_Input_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Input_Instances) {
        if ((value > 0) && (value <= MULTISTATE_NUMBER_OF_STATES)) {
            Present_Value[index - 1] = (uint8_t)value;
            status = true;
#if defined(MQTT)
            if (yaml_config_mqtt_enable() && !bacnet_client) {
                mqtt_publish_topic(OBJECT_MULTI_STATE_INPUT, object_instance, PROP_PRESENT_VALUE,
                    MQTT_TOPIC_VALUE_INTEGER, &value, uuid);
            }
#endif /* defined(MQTT) */
        }
    }

    return status;
}

bool Multistate_Input_Out_Of_Service(uint32_t object_instance)
{
    bool value = false;
    unsigned index = 0;

    index = Multistate_Input_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Input_Instances) {
        value = Out_Of_Service[index - 1];
    }

    return value;
}

void Multistate_Input_Out_Of_Service_Set(uint32_t object_instance, bool value)
{
    unsigned index = 0;

    index = Multistate_Input_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Input_Instances) {
        Out_Of_Service[index - 1] = value;
    }

    return;
}

char *Multistate_Input_Description(uint32_t object_instance)
{
    unsigned index = 0; /* offset from instance lookup */
    char *pName = NULL; /* return value */

    index = Multistate_Input_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Input_Instances) {
        pName = Object_Description[index - 1];
    }

    return pName;
}

bool Multistate_Input_Description_Set(uint32_t object_instance, char *new_name)
{
    unsigned index = 0; /* offset from instance lookup */
    size_t i = 0; /* loop counter */
    bool status = false; /* return value */
    size_t obj_desc_size = 64 * sizeof(char);

    index = Multistate_Input_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Input_Instances) {
        status = true;
        if (new_name) {
            for (i = 0; i < obj_desc_size; i++) {
                Object_Description[index - 1][i] = new_name[i];
                if (new_name[i] == 0) {
                    break;
                }
            }
        } else {
            for (i = 0; i < obj_desc_size; i++) {
                Object_Description[index - 1][i] = 0;
            }
        }
    }

    return status;
}

static bool Multistate_Input_Description_Write(uint32_t object_instance,
    BACNET_CHARACTER_STRING *char_string,
    BACNET_ERROR_CLASS *error_class,
    BACNET_ERROR_CODE *error_code)
{
    unsigned index = 0; /* offset from instance lookup */
    size_t length = 0;
    uint8_t encoding = 0;
    bool status = false; /* return value */
    size_t obj_desc_size = 64 * sizeof(char);

    index = Multistate_Input_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Input_Instances) {
        length = characterstring_length(char_string);
        if (length <= obj_desc_size) {
            encoding = characterstring_encoding(char_string);
            if (encoding == CHARACTER_UTF8) {
                status = characterstring_ansi_copy(Object_Description[index - 1],
                    obj_desc_size, char_string);
                if (!status) {
                    *error_class = ERROR_CLASS_PROPERTY;
                    *error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
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

bool Multistate_Input_Object_Name(
    uint32_t object_instance, BACNET_CHARACTER_STRING *object_name)
{
    unsigned index = 0; /* offset from instance lookup */
    bool status = false;

    index = Multistate_Input_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Input_Instances) {
        status = characterstring_init_ansi(object_name, Object_Name[index - 1]);
    }

    return status;
}

/* note: the object name must be unique within this device */
bool Multistate_Input_Name_Set(uint32_t object_instance, char *new_name)
{
    unsigned index = 0; /* offset from instance lookup */
    size_t i = 0; /* loop counter */
    bool status = false; /* return value */
    size_t obj_name_size = 64 * sizeof(char);

    index = Multistate_Input_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Input_Instances) {
        status = true;
        /* FIXME: check to see if there is a matching name */
        if (new_name) {
            for (i = 0; i < obj_name_size; i++) {
                Object_Name[index - 1][i] = new_name[i];
                if (new_name[i] == 0) {
                    break;
                }
            }
        } else {
            for (i = 0; i < obj_name_size; i++) {
                Object_Name[index - 1][i] = 0;
            }
        }
    }

    return status;
}

bool Multistate_Input_Object_Name_Set(uint32_t object_instance,
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

    index = Multistate_Input_Instance_To_Index(object_instance);
    if (index > 0 && index <= Multistate_Input_Instances) {
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
                    mqtt_publish_topic(OBJECT_MULTI_STATE_INPUT, object_instance, PROP_OBJECT_NAME,
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

char *Multistate_Input_State_Text(
    uint32_t object_instance, uint32_t state_index)
{
    unsigned index = 0; /* offset from instance lookup */
    char *pName = NULL; /* return value */

    index = Multistate_Input_Instance_To_Index(object_instance);
    if ((index > 0 && index <= Multistate_Input_Instances) && (state_index > 0) &&
        (state_index <= MULTISTATE_NUMBER_OF_STATES)) {
        state_index--;
        pName = State_Text[index - 1][state_index];
    }

    return pName;
}

/* note: the object name must be unique within this device */
bool Multistate_Input_State_Text_Set(
    uint32_t object_instance, uint32_t state_index, char *new_name)
{
    unsigned index = 0; /* offset from instance lookup */
    size_t i = 0; /* loop counter */
    bool status = false; /* return value */
    size_t state_text_size = 64 * sizeof(char);

    index = Multistate_Input_Instance_To_Index(object_instance);
    if ((index > 0 && index <= Multistate_Input_Instances) && (state_index > 0) &&
        (state_index <= MULTISTATE_NUMBER_OF_STATES)) {
        state_index--;
        status = true;
        if (new_name) {
            for (i = 0; i < state_text_size; i++) {
                State_Text[index - 1][state_index][i] = new_name[i];
                if (new_name[i] == 0) {
                    break;
                }
            }
        } else {
            for (i = 0; i < state_text_size; i++) {
                State_Text[index - 1][state_index][i] = 0;
            }
        }
    }

    return status;
    ;
}

static bool Multistate_Input_State_Text_Write(uint32_t object_instance,
    uint32_t state_index,
    BACNET_CHARACTER_STRING *char_string,
    BACNET_ERROR_CLASS *error_class,
    BACNET_ERROR_CODE *error_code)
{
    unsigned index = 0; /* offset from instance lookup */
    size_t length = 0;
    uint8_t encoding = 0;
    bool status = false; /* return value */
    size_t state_text_size = 64 * sizeof(char);

    index = Multistate_Input_Instance_To_Index(object_instance);
    if ((index > 0 && index < Multistate_Input_Instances) && (state_index > 0) &&
        (state_index <= Multistate_Input_Max_States(object_instance))) {
        state_index--;
        length = characterstring_length(char_string);
        if (length <= state_text_size) {
            encoding = characterstring_encoding(char_string);
            if (encoding == CHARACTER_UTF8) {
                status =
                    characterstring_ansi_copy(State_Text[index - 1][state_index],
                        state_text_size, char_string);
                if (!status) {
                    *error_class = ERROR_CLASS_PROPERTY;
                    *error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                }
            } else {
                *error_class = ERROR_CLASS_PROPERTY;
                *error_code = ERROR_CODE_CHARACTER_SET_NOT_SUPPORTED;
            }
        } else {
            *error_class = ERROR_CLASS_PROPERTY;
            *error_code = ERROR_CODE_NO_SPACE_TO_WRITE_PROPERTY;
        }
    } else {
        *error_class = ERROR_CLASS_PROPERTY;
        *error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
    }

    return status;
}

/* return apdu len, or BACNET_STATUS_ERROR on error */
int Multistate_Input_Read_Property(BACNET_READ_PROPERTY_DATA *rpdata)
{
    int len = 0;
    int apdu_len = 0; /* return value */
    BACNET_BIT_STRING bit_string;
    BACNET_CHARACTER_STRING char_string;
    uint32_t present_value = 0;
    unsigned i = 0;
    uint32_t max_states = 0;
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
                &apdu[0], OBJECT_MULTI_STATE_INPUT, rpdata->object_instance);
            break;
            /* note: Name and Description don't have to be the same.
               You could make Description writable and different */
        case PROP_OBJECT_NAME:
            Multistate_Input_Object_Name(rpdata->object_instance, &char_string);
            apdu_len =
                encode_application_character_string(&apdu[0], &char_string);
            break;
        case PROP_DESCRIPTION:
            characterstring_init_ansi(&char_string,
                Multistate_Input_Description(rpdata->object_instance));
            apdu_len =
                encode_application_character_string(&apdu[0], &char_string);
            break;
        case PROP_OBJECT_TYPE:
            apdu_len = encode_application_enumerated(
                &apdu[0], OBJECT_MULTI_STATE_INPUT);
            break;
        case PROP_PRESENT_VALUE:
            present_value =
                Multistate_Input_Present_Value(rpdata->object_instance);
            apdu_len = encode_application_unsigned(&apdu[0], present_value);
            break;
        case PROP_STATUS_FLAGS:
            /* note: see the details in the standard on how to use these */
            bitstring_init(&bit_string);
            bitstring_set_bit(&bit_string, STATUS_FLAG_IN_ALARM, false);
            bitstring_set_bit(&bit_string, STATUS_FLAG_FAULT, false);
            bitstring_set_bit(&bit_string, STATUS_FLAG_OVERRIDDEN, false);
            if (Multistate_Input_Out_Of_Service(rpdata->object_instance)) {
                bitstring_set_bit(
                    &bit_string, STATUS_FLAG_OUT_OF_SERVICE, true);
            } else {
                bitstring_set_bit(
                    &bit_string, STATUS_FLAG_OUT_OF_SERVICE, false);
            }
            apdu_len = encode_application_bitstring(&apdu[0], &bit_string);
            break;
        case PROP_EVENT_STATE:
            /* note: see the details in the standard on how to use this */
            apdu_len =
                encode_application_enumerated(&apdu[0], EVENT_STATE_NORMAL);
            break;
        case PROP_OUT_OF_SERVICE:
            state = Multistate_Input_Out_Of_Service(rpdata->object_instance);
            apdu_len = encode_application_boolean(&apdu[0], state);
            break;
        case PROP_NUMBER_OF_STATES:
            apdu_len = encode_application_unsigned(&apdu[apdu_len],
                Multistate_Input_Max_States(rpdata->object_instance));
            break;
        case PROP_STATE_TEXT:
            if (rpdata->array_index == 0) {
                /* Array element zero is the number of elements in the array */
                apdu_len = encode_application_unsigned(&apdu[0],
                    Multistate_Input_Max_States(rpdata->object_instance));
            } else if (rpdata->array_index == BACNET_ARRAY_ALL) {
                /* if no index was specified, then try to encode the entire list
                 */
                /* into one packet. */
                max_states =
                    Multistate_Input_Max_States(rpdata->object_instance);
                for (i = 1; i <= max_states; i++) {
                    characterstring_init_ansi(&char_string,
                        Multistate_Input_State_Text(
                            rpdata->object_instance, i));
                    /* FIXME: this might go beyond MAX_APDU length! */
                    len = encode_application_character_string(
                        &apdu[apdu_len], &char_string);
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
                max_states =
                    Multistate_Input_Max_States(rpdata->object_instance);
                if (rpdata->array_index <= max_states) {
                    characterstring_init_ansi(&char_string,
                        Multistate_Input_State_Text(
                            rpdata->object_instance, rpdata->array_index));
                    apdu_len = encode_application_character_string(
                        &apdu[0], &char_string);
                } else {
                    rpdata->error_class = ERROR_CLASS_PROPERTY;
                    rpdata->error_code = ERROR_CODE_INVALID_ARRAY_INDEX;
                    apdu_len = BACNET_STATUS_ERROR;
                }
            }
            break;
        default:
            rpdata->error_class = ERROR_CLASS_PROPERTY;
            rpdata->error_code = ERROR_CODE_UNKNOWN_PROPERTY;
            apdu_len = BACNET_STATUS_ERROR;
            break;
    }
    /*  only array properties can have array options */
    if ((apdu_len >= 0) && (rpdata->object_property != PROP_STATE_TEXT) &&
        (rpdata->array_index != BACNET_ARRAY_ALL)) {
        rpdata->error_class = ERROR_CLASS_PROPERTY;
        rpdata->error_code = ERROR_CODE_PROPERTY_IS_NOT_AN_ARRAY;
        apdu_len = BACNET_STATUS_ERROR;
    }

    return apdu_len;
}

/* returns true if successful */
bool Multistate_Input_Write_Property(BACNET_WRITE_PROPERTY_DATA *wp_data)
{
    bool status = false; /* return value */
    int len = 0;
    int element_len = 0;
    BACNET_APPLICATION_DATA_VALUE value;
    uint32_t max_states = 0;
    uint32_t array_index = 0;
    BACNET_OBJECT_TYPE object_type = OBJECT_NONE;
    uint32_t object_instance = 0;

    /* decode the first chunk of the request */
    len = bacapp_decode_application_data(
        wp_data->application_data, wp_data->application_data_len, &value);
    /* len < application_data_len: extra data for arrays only */
    if (len < 0) {
        /* error while decoding - a value larger than we can handle */
        wp_data->error_class = ERROR_CLASS_PROPERTY;
        wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
        return false;
    }
    if ((wp_data->object_property != PROP_STATE_TEXT) &&
        (wp_data->array_index != BACNET_ARRAY_ALL)) {
        /*  only array properties can have array options */
        wp_data->error_class = ERROR_CLASS_PROPERTY;
        wp_data->error_code = ERROR_CODE_PROPERTY_IS_NOT_AN_ARRAY;
        return false;
    }
    switch (wp_data->object_property) {
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
                    status = Multistate_Input_Object_Name_Set(
                        wp_data->object_instance, &value.type.Character_String,
                        &wp_data->error_class, &wp_data->error_code, NULL, false);
                }
            }
            break;
        case PROP_DESCRIPTION:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_CHARACTER_STRING);
            if (status) {
                status = Multistate_Input_Description_Write(
                    wp_data->object_instance, &value.type.Character_String,
                    &wp_data->error_class, &wp_data->error_code);
            }
            break;
        case PROP_PRESENT_VALUE:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_UNSIGNED_INT);
            if (status) {
                status = Multistate_Input_Present_Value_Set(
                    wp_data->object_instance, value.type.Unsigned_Int, NULL, false);
                if (!status) {
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                }
            }
            break;
        case PROP_OUT_OF_SERVICE:
            status = write_property_type_valid(wp_data, &value,
                BACNET_APPLICATION_TAG_BOOLEAN);
            if (status) {
                Multistate_Input_Out_Of_Service_Set(
                    wp_data->object_instance, value.type.Boolean);
            }
            break;
        case PROP_STATE_TEXT:
            if (wp_data->array_index == 0) {
                /* Array element zero is the number of
                   elements in the array.  We have a fixed
                   size array, so we are read-only. */
                wp_data->error_class = ERROR_CLASS_PROPERTY;
                wp_data->error_code = ERROR_CODE_WRITE_ACCESS_DENIED;
            } else if (wp_data->array_index == BACNET_ARRAY_ALL) {
                max_states =
                    Multistate_Input_Max_States(wp_data->object_instance);
                array_index = 1;
                element_len = len;
                do {
                    status = write_property_type_valid(wp_data, &value,
                        BACNET_APPLICATION_TAG_CHARACTER_STRING);
                    if (!status) {
                        break;
                    }
                    if (element_len) {
                        status = Multistate_Input_State_Text_Write(
                            wp_data->object_instance, array_index,
                            &value.type.Character_String,
                            &wp_data->error_class, &wp_data->error_code);
                    }
                    max_states--;
                    array_index++;
                    if (max_states) {
                        element_len = bacapp_decode_application_data(
                            &wp_data->application_data[len],
                            wp_data->application_data_len - len, &value);
                        if (element_len < 0) {
                            wp_data->error_class = ERROR_CLASS_PROPERTY;
                            wp_data->error_code =
                                ERROR_CODE_VALUE_OUT_OF_RANGE;
                            break;
                        }
                        len += element_len;
                    }
                } while (max_states);
            } else {
                max_states =
                    Multistate_Input_Max_States(wp_data->object_instance);
                if (wp_data->array_index <= max_states) {
                    status = write_property_type_valid(wp_data, &value,
                        BACNET_APPLICATION_TAG_CHARACTER_STRING);
                    if (status) {
                        status = Multistate_Input_State_Text_Write(
                            wp_data->object_instance, wp_data->array_index,
                            &value.type.Character_String, &wp_data->error_class,
                            &wp_data->error_code);
                    }
                } else {
                    wp_data->error_class = ERROR_CLASS_PROPERTY;
                    wp_data->error_code = ERROR_CODE_VALUE_OUT_OF_RANGE;
                }
            }
            break;
        case PROP_OBJECT_IDENTIFIER:
        case PROP_OBJECT_TYPE:
        case PROP_STATUS_FLAGS:
        case PROP_EVENT_STATE:
        case PROP_NUMBER_OF_STATES:
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
