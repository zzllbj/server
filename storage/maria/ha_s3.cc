/* Copyright (C) 2019 MariaDB Corppration AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the
   Free Software Foundation, Inc.
   51 Franklin Street, Fifth Floor, Boston, MA 02111-1301 USA
*/

/*
  Implementation of S3 storage engine.

  Storage format:

  The S3 engine is read only storage engine. The data is stored in
  same format as a non transactional Aria table in BLOCK_RECORD format.
  This makes it easy to cache both index and rows in the page cache.
  Data and index file are split into blocks of 's3_block_size', default
  4M.

  The table and it's associated files are stored in S3 into the following
  locations:

  frm file (for discovery):
  aws_bucket/database/table/frm

  First index block (contains description if the Aria file):
  aws_bucket/database/table/aria

  Rest of the index file:
  aws_bucket/database/table/index/block_number

  Data file:
  aws_bucket/database/table/data/block_number

  block_number is 6 digits decimal number, prefixed with 0
  (Can be larger than 6 numbers, the prefix is just for nice output)

  frm and base blocks are small (just the needed data).
  index and blocks are of size 's3_block_size'

  If compression is used, then original block size is s3_block_size
  but the stored block will be the size of the compressed block.

  Implementation:
  The s3 engine inherits from the ha_maria handler

  s3 will use it's own page cache to not interfere with normal Aria
  usage but also to ensure that the S3 page cache is large enough
  (with a 4M s3_block_size the engine will need a large cache to work,
  at least s3_block_size * 32. The default cache is 512M.
*/

#include "maria_def.h"
#include "sql_class.h"
#include <mysys_err.h>
C_MODE_START
#include <marias3.h>
C_MODE_END
#include "ha_s3.h"
#include "s3_func.h"
#include "aria_backup.h"

static PAGECACHE s3_pagecache;
static ulong s3_block_size;
static ulong s3_pagecache_division_limit, s3_pagecache_age_threshold;
static ulong s3_pagecache_file_hash_size;
static ulonglong s3_pagecache_buffer_size;
static char *s3_bucket, *s3_access_key, *s3_secret_key, *s3_region;

handlerton *s3_hton= 0;


/* Define system variables for S3 */

static MYSQL_SYSVAR_ULONG(block_size, s3_block_size,
       PLUGIN_VAR_RQCMDARG,
       "Block size for S3", 0, 0,
       4*1024*1024, 65536, 16*1024*1024, 8192);

static MYSQL_SYSVAR_ULONG(pagecache_age_threshold,
       s3_pagecache_age_threshold, PLUGIN_VAR_RQCMDARG,
       "This characterizes the number of hits a hot block has to be untouched "
       "until it is considered aged enough to be downgraded to a warm block. "
       "This specifies the percentage ratio of that number of hits to the "
       "total number of blocks in the page cache.", 0, 0,
       300, 100, ~ (ulong) 0L, 100);

static MYSQL_SYSVAR_ULONGLONG(pagecache_buffer_size, s3_pagecache_buffer_size,
       PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
       "The size of the buffer used for index blocks for Aria tables. "
       "Increase this to get better index handling (for all reads and "
       "multiple writes) to as much as you can afford.", 0, 0,
       KEY_CACHE_SIZE, 8192*16L, ~(ulonglong) 0, 1);

static MYSQL_SYSVAR_ULONG(pagecache_division_limit,
                          s3_pagecache_division_limit,
       PLUGIN_VAR_RQCMDARG,
       "The minimum percentage of warm blocks in key cache", 0, 0,
       100,  1, 100, 1);

static MYSQL_SYSVAR_ULONG(pagecache_file_hash_size,
                          s3_pagecache_file_hash_size,
       PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
       "Number of hash buckets for open files.  If you have a lot "
       "of S3 files open you should increase this for faster flush of "
       "changes. A good value is probably 1/10 of number of possible open "
       "S3 files.", 0,0, 512, 32, 16384, 1);

static MYSQL_SYSVAR_STR(bucket, s3_bucket,
       PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
      "AWS bucket",
       0, 0, "MariaDB");
static MYSQL_SYSVAR_STR(access_key, s3_access_key,
       PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
      "AWS access key",
       0, 0, "");
static MYSQL_SYSVAR_STR(secret_key, s3_secret_key,
       PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
      "AWS secret key",
       0, 0, "");
static MYSQL_SYSVAR_STR(region, s3_region,
       PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
      "AWS region",
       0, 0, "");


ha_create_table_option s3_table_option_list[]=
{
  /*
    one numeric option, with the default of UINT_MAX32, valid
    range of values 0..UINT_MAX32, and a "block size" of 10
    (any value must be divisible by 10).
  */
  HA_TOPTION_SYSVAR("s3_block_size", s3_block_size, block_size),
  HA_TOPTION_ENUM("compression_algorithm", compression_algorithm, "none,zlib",
                  0),
  HA_TOPTION_END
};


/*****************************************************************************
 S3 handler code
******************************************************************************/

/**
   Create S3 handler
*/


ha_s3::ha_s3(handlerton *hton, TABLE_SHARE *table_arg)
  :ha_maria(hton, table_arg), in_alter_table(0)
{
  /* Remove things that S3 doesn't support */
  int_table_flags&= ~(HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE |
                      HA_CAN_EXPORT);
  can_enable_indexes= 0;
}


/**
   Remember the handler to use for s3_block_read()

   @note
   In the future the ms3_st objects could be stored in
   a list in share. In this case we would however need a mutex
   to access the next free onw. By using st_my_thread_var we
   can avoid the mutex with the small cost of having to call
   register handler in all handler functions that will access
   the page cache
*/

void ha_s3::register_handler(MARIA_HA *file)
{
  struct st_my_thread_var *thread= my_thread_var;
  thread->keycache_file= (void*) file;
}


/**
   Write a row

   When generating the table as part of ALTER TABLE, writes are allowed.
   When table is moved to S3, writes are not allowed.
*/

int ha_s3::write_row(uchar *buf)
{
  if (in_alter_table)
    return ha_maria::write_row(buf);
  return HA_ERR_WRONG_COMMAND;
}

static void s3_info_init(S3_INFO *info)
{
  lex_string_set(&info->access_key, s3_access_key);
  lex_string_set(&info->secret_key, s3_secret_key);
  lex_string_set(&info->region,     s3_region);
  lex_string_set(&info->bucket,     s3_bucket);
}

/**
   Fill information in S3_INFO including paths to table and database
*/

static void s3_info_init(S3_INFO *s3_info, const char *path,
                         char *database_buff, size_t database_length)
{
  s3_info_init(s3_info);
  set_database_and_table_from_path(s3_info, path);
  /* Fix database as it's not \0 terminated */
  strmake(database_buff, s3_info->database.str,
          MY_MIN(database_length, s3_info->database.length));
  s3_info->database.str= database_buff;
}


/**
  Drop S3 table
*/

int ha_s3::delete_table(const char *name)
{
  ms3_st *s3_client;
  S3_INFO s3_info;
  int error;
  char database[NAME_LEN+1];

  s3_info_init(&s3_info, name, database, sizeof(database)-1);

  /* If internal on disk temporary table, let Aria take care of it */
  if (!strncmp(s3_info.table.str, "#sql-", 5))
    return ha_maria::delete_table(name);

  if (!(s3_client= s3_open_connection(&s3_info)))
    return HA_ERR_NO_SUCH_TABLE;
  error= aria_delete_from_s3(s3_client, s3_info.bucket.str,
                             s3_info.database.str,
                             s3_info.table.str,0);
  ms3_deinit(s3_client);
  return error;
}

/**
   Copy an Aria table to S3 or rename a table in S3

   The copy happens as part of the rename in ALTER TABLE when all data
   is in an Aria table and we now have to copy it to S3.

   If the table is an old table already in S3, we should just rename it.
*/

int ha_s3::rename_table(const char *from, const char *to)
{
  S3_INFO s3_info;
  char name[FN_REFLEN];
  ms3_st *s3_client;

  s3_info_init(&s3_info, to, name, NAME_LEN);
  if (!(s3_client= s3_open_connection(&s3_info)))
    return HA_ERR_NO_SUCH_TABLE;
  if (!strncmp(from + dirname_length(from), "#sql-", 5))
  {
    /*
      The table is a temporary table as part of ALTER TABLE.
      Copy the on disk temporary Aria table to S3.
    */
    int error;
    error= aria_copy_to_s3(s3_client, s3_info.bucket.str, from,
                           s3_info.database.str,
                           s3_info.table.str,
                           0, 0, 0, 0);
    if (!error)
    {
      /* Remove original files */
      fn_format(name, from, "", MARIA_NAME_DEXT,
                MY_APPEND_EXT|MY_UNPACK_FILENAME);
      my_delete(name, MYF(MY_WME | ME_WARNING));
      fn_format(name, from, "", MARIA_NAME_IEXT,
                MY_APPEND_EXT|MY_UNPACK_FILENAME);
      my_delete(name, MYF(MY_WME | ME_WARNING));
    }
    ms3_deinit(s3_client);
    return error;
  }
  ms3_deinit(s3_client);
  return HA_ERR_WRONG_COMMAND;
}

/**
   Create a s3 table.

   @notes
   One can only create an s3 table as part of ALTER TABLE
   The table is created as a non transactional Aria table with
   BLOCK_RECORD format
*/

int ha_s3::create(const char *name, TABLE *table_arg,
                  HA_CREATE_INFO *ha_create_info)
{
  DBUG_ENTER("ha_s3::create");

  if (!(ha_create_info->options & HA_CREATE_TMP_ALTER))
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);

  /* Force the table to a format suitable for S3 */
  ha_create_info->row_type= ROW_TYPE_PAGE;
  ha_create_info->transactional= HA_CHOICE_NO;
  DBUG_RETURN(ha_maria::create(name, table_arg, ha_create_info));
}

/**
   Open table


   @notes
   Table is read only, except if opened by ALTER as in this case we
   are creating the S3 table.
*/

int ha_s3::open(const char *name, int mode, uint open_flags)
{
  int res;
  S3_INFO s3_info;
  DBUG_ENTER("ha_s3:open");
  if (mode != O_RDONLY && !(open_flags & HA_OPEN_FOR_CREATE))
    DBUG_RETURN(EACCES);

  open_args= 0;
  if (!(open_flags & HA_OPEN_FOR_CREATE))
  {
    s3_info_init(&s3_info);
    /* Pass the above arguments to maria_open() */
    open_args= &s3_info;
  }

  if (!(res= ha_maria::open(name, mode, open_flags)))
  {
    if ((open_flags & HA_OPEN_FOR_CREATE))
      in_alter_table= 1;
    else
    {
      /*
        We have to modify the pagecache callbacks for the data file,
        index file and for bitmap handling
      */
      file->s->pagecache= &s3_pagecache;
      file->dfile.big_block_size= file->s->kfile.big_block_size=
        file->s->bitmap.file.big_block_size= file->s->base.s3_block_size;
      file->s->kfile.head_blocks= file->s->base.keystart / file->s->block_size;
    }
  }
  open_args= 0;
  DBUG_RETURN(res);
}


/******************************************************************************
 Storage engine handler definitions
******************************************************************************/

/**
   Free all resources for s3
*/

static handler *s3_create_handler(handlerton *hton,
                                  TABLE_SHARE * table,
                                  MEM_ROOT *mem_root)
{
  return new (mem_root) ha_s3(hton, table);
}


static int s3_hton_panic(handlerton *hton, ha_panic_function flag)
{
  if (flag == HA_PANIC_CLOSE && s3_hton)
  {
    end_pagecache(&s3_pagecache, TRUE);
    s3_deinit_library();
    s3_hton= 0;
  }
  return 0;
}


static int ha_s3_init(void *p)
{
  bool res;
  DBUG_ASSERT(maria_hton);

  s3_hton= (handlerton *)p;

  /* Use Aria engine as a base */
  memcpy(s3_hton, maria_hton, sizeof(*s3_hton));
  s3_hton->db_type= DB_TYPE_S3;
  s3_hton->create= s3_create_handler;
  s3_hton->panic=  s3_hton_panic;
  s3_hton->table_options= s3_table_option_list;
  s3_hton->commit= 0;
  s3_hton->rollback= 0;
  s3_hton->checkpoint_state= 0;
  s3_hton->flush_logs= 0;
  s3_hton->show_status= 0;
  s3_hton->prepare_for_backup= 0;
  s3_hton->end_backup= 0;
  s3_hton->flags= 0;

  if ((res= !init_pagecache(&s3_pagecache,
                            (size_t) s3_pagecache_buffer_size,
                            s3_pagecache_division_limit,
                            s3_pagecache_age_threshold, maria_block_size,
                            s3_pagecache_file_hash_size, 0)))
    s3_hton= 0;
  s3_pagecache.big_block_read= s3_block_read;
  s3_pagecache.big_block_free= s3_free;
  s3_init_library();
  return res ? HA_ERR_INITIALIZATION : 0;
}

static SHOW_VAR status_variables[]= {
  {"pagecache_blocks_not_flushed",
   (char*) &s3_pagecache.global_blocks_changed, SHOW_LONG},
  {"pagecache_blocks_unused",
   (char*) &s3_pagecache.blocks_unused, SHOW_LONG},
  {"pagecache_blocks_used",
   (char*) &s3_pagecache.blocks_used, SHOW_LONG},
  {"pagecache_read_requests",
   (char*) &s3_pagecache.global_cache_r_requests, SHOW_LONGLONG},
  {"pagecache_reads",
   (char*) &s3_pagecache.global_cache_read, SHOW_LONGLONG},
  {NullS, NullS, SHOW_LONG}
};


static struct st_mysql_sys_var* system_variables[]= {
  MYSQL_SYSVAR(block_size),
  MYSQL_SYSVAR(pagecache_age_threshold),
  MYSQL_SYSVAR(pagecache_buffer_size),
  MYSQL_SYSVAR(pagecache_division_limit),
  MYSQL_SYSVAR(pagecache_file_hash_size),
  MYSQL_SYSVAR(bucket),
  MYSQL_SYSVAR(access_key),
  MYSQL_SYSVAR(secret_key),
  MYSQL_SYSVAR(region),

  NULL
};

struct st_mysql_storage_engine s3_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

maria_declare_plugin(s3)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &s3_storage_engine,
  "S3",
  "MariaDB Corporation Ab",
  "Read only table stored in S3. Created by running "
  "ALTER TABLE table_name ENGINE=s3",
  PLUGIN_LICENSE_GPL,
  ha_s3_init,                   /* Plugin Init      */
  NULL,                         /* Plugin Deinit    */
  0x0100,                       /* 1.0              */
  status_variables,             /* status variables */
  system_variables,             /* system variables */
  "1.0",                        /* string version   */
  MariaDB_PLUGIN_MATURITY_ALPHA /* maturity         */
}
maria_declare_plugin_end;
