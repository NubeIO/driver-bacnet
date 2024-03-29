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
#ifndef DLENV_H
#define DLENV_H
#include <stddef.h>
#include <stdint.h>
#include <stdint.h>
#include "bacnet/datalink/bvlc.h"

#include "bacnet/bacnet_stack_exports.h"

#if defined(YAML_CONFIG)
#include "bacnet/datalink/dlparams.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

    BACNET_STACK_EXPORT
    void dlenv_init(
        void);

#if defined(YAML_CONFIG)
    BACNET_STACK_EXPORT
    void dlenv_init_with_params(
        BACNET_DATALINK_PARAMS *params);
#endif

    BACNET_STACK_EXPORT
    int dlenv_register_as_foreign_device(
        void);

    BACNET_STACK_EXPORT
    void dlenv_maintenance_timer(
        uint16_t elapsed_seconds);

    BACNET_STACK_EXPORT
    void dlenv_bbmd_address_set(
        BACNET_IP_ADDRESS *address);

    BACNET_STACK_EXPORT
    void dlenv_bbmd_ttl_set(
        uint16_t ttl_secs);

    BACNET_STACK_EXPORT
    int dlenv_bbmd_result(
        void);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif
