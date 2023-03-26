#define MYSQL_SERVER 1 // Not sure if this does anything anymore

#include <my_global.h>
#include <my_base.h>
#include <table.h>
#include <handler.h>
#include <my_dir.h>
#include <mysql_version.h>
#include <mysqld_error.h>
#include <sql_class.h>
#include <structs.h>
#include <sys/mman.h>
#include <thr_lock.h> /* THR_LOCK, THR_LOCK_DATA */
#include <field.h>
