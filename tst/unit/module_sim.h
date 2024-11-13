//
// Simulate the Valkey Module Environment
//
#ifndef VALKEYJSONMODULE_TST_UNIT_MODULE_SIM_H_
#define VALKEYJSONMODULE_TST_UNIT_MODULE_SIM_H_

#include <cstddef>
#include <string>

extern size_t malloced;     // Total currently allocated memory
void setupValkeyModulePointers();
std::string test_getLogText();

#endif  // VALKEYJSONMODULE_TST_UNIT_MODULE_SIM_H_





