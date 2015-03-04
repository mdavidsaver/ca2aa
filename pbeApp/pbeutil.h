#ifndef PVEUTIL_H
#define PVEUTIL_H

#include <string>

#include <epicsTime.h>

extern const char *pvseps;
std::string pvpathname(const char* pvname);

void createDirs(const std::string& path);

void getYear(const epicsTimeStamp& t, int *year);
void getStartOfYear(int year, epicsTimeStamp* t);

std::ostream& operator<<(std::ostream& strm, const epicsTime& t);

#endif // PVEUTIL_H
