#ifndef SYBUTILS_H
#define SYBUTILS_H


#include <ctpublic.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
/** The callback, use syslog to record client messages. */
CS_RETCODE clientmsg_cb(CS_CONTEXT *context, CS_CONNECTION *connection, CS_CLIENTMSG *errmsg);

/** The callback, use syslog to record server messages. */
CS_RETCODE servermsg_cb(CS_CONTEXT *context, CS_CONNECTION *connection, CS_SERVERMSG *srvmsg);
/** */
bool is_leap_year(int year);

/** Computes the total number of days starting from year 0*/
int date_to_days(int year, int month, int day);


#endif // SYBUTILS_H
