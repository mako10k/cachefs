#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([cachefs], [0.2.0], [katsumata-m@t-axis.co.jp])
AC_CONFIG_SRCDIR([src/cachefs.c])
AC_CONFIG_HEADERS([config.h])
AM_INIT_AUTOMAKE([foreign])
AC_USE_SYSTEM_EXTENSIONS

LT_INIT()

# Checks for programs.
AC_PROG_CC

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h inttypes.h stddef.h stdlib.h string.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_STRUCT_ST_BLOCKS

# Checks for library functions.
AC_CHECK_FUNCS([ftruncate strerror clock_gettime fdatasync strrchr memfd_create])
AC_CONFIG_FILES([Makefile src/Makefile])
AC_CONFIG_MACRO_DIRS([m4])

AC_OUTPUT
