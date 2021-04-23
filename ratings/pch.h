// pch.h: This is a precompiled header file.
// Files listed below are compiled only once, improving build performance for future builds.
// This also affects IntelliSense performance, including code completion and many code browsing features.
// However, files listed here are ALL re-compiled if any one of them is updated between builds.
// Do not add files here that you will be updating frequently as this negates the performance advantage.

#ifndef PCH_H
#define PCH_H

#define _CRT_SECURE_NO_WARNINGS
#define NOMINMAX
#define VC_EXTRALEAN
#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <stdio.h>
#include <tchar.h>
#include <stdlib.h>
#include <conio.h>

#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <memory>
#include <functional>
#include <regex>

#include "sqlite3.h"

#ifdef _PREFAST_
#define Assert(x) __analysis_assume(!!(x))
#else
#define Assert(x) _ASSERT(x)
#endif

#endif //PCH_H
