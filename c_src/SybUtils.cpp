#include "SybUtils.h"
#include "SysLogger.h"


CS_RETCODE clientmsg_cb(CS_CONTEXT *context,
        CS_CONNECTION *connection, CS_CLIENTMSG *errmsg)
{
    std::cout<<errmsg->msgstring;
    SysLogger::info(errmsg->msgstring);
    return CS_SUCCEED;
}

CS_RETCODE servermsg_cb(CS_CONTEXT *context,
        CS_CONNECTION *connection, CS_SERVERMSG *srvmsg)
{
    std::cout<<srvmsg->text;
    SysLogger::info(srvmsg->text);
    return CS_SUCCEED;
}

bool is_leap_year(int year)
{
    return  ((year % 4 ==0) && (year % 400 != 0))
            || (year % 400 == 0);
}

int dy(int year)
{
    int x;

    if (year == 0) {
        return 0;
    } else {
        x = year - 1;
        return (int)(x / 4) - (int)(x / 100) +
                (int)(x / 400) + x * 365 + 366;
    }
}

int dm(int month, bool is_leap)
{
    int days[12] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

    if (month > 2 && is_leap) {
        return days[month - 1] + 1;
    }

    return days[month - 1];
}

int date_to_days(int year, int month, int day)
{
    bool is_leap;
    int days;

    if (year < 0 || month < 1 || month > 12 || day < 1) {
        return -1;
    }
    is_leap = is_leap_year(year);
    days = dm(month, is_leap);
    if (month == 12) {
        if (day > 31) {
            return -1;
        }
    } else {
        if (days + day > dm(month + 1, is_leap)) {
            return -1;
        }
    }

    return dy(year) + days + day - 1;
}


