/* Copyright (C) 2019 MariaDB Corporation Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02111-1301 USA */

/*
  Interface function used by S3 storage engine and aria_copy_for_s3
*/

#include "maria_def.h"
#include "s3_func.h"
#include <aria_backup.h>
#include <mysqld_error.h>
#include <mysys_err.h>
#include <mysql_com.h>
#include <zlib.h>

/* number of '.' to print during a copy in verbose mode */
#define DISPLAY_WITH 79
#define AWS_PATH_LENGTH ((NAME_LEN)*3+3+10+6+11)

static void convert_index_to_s3_format(uchar *header, ulong block_size,
                                       int compression);
static void convert_index_to_disk_format(uchar *header);
static void convert_frm_to_s3_format(uchar *header);
static void convert_frm_to_disk_format(uchar *header);


/******************************************************************************
 Allocations handler for libmarias3
 To be removed when we do the init allocation in mysqld.cc
******************************************************************************/

static void *s3_wrap_malloc(size_t size)
{
  return my_malloc(size, MYF(MY_WME));
}

static void *s3_wrap_calloc(size_t nmemb, size_t size)
{
  return my_malloc(nmemb * size, MYF(MY_WME | MY_ZEROFILL));
}

static void *s3_wrap_realloc(void *ptr, size_t size)
{
  return my_realloc(ptr, size, MYF(MY_WME | MY_ALLOW_ZERO_PTR));
}

static char *s3_wrap_strdup(const char *str)
{
  return my_strdup(str, MYF(MY_WME));
}

static void s3_wrap_free(void *ptr)
{
  my_free(ptr);
}

void s3_init_library()
{
  ms3_library_init_malloc(s3_wrap_malloc, s3_wrap_free, s3_wrap_realloc,
                          s3_wrap_strdup, s3_wrap_calloc);
}

void s3_deinit_library()
{
  ms3_library_deinit();
}

/******************************************************************************
 Functions on S3_INFO and s3_block
******************************************************************************/

/*
  Free memory allocated by s3_get_object
*/

void s3_free(s3_block *data)
{
  my_free(data->alloc_ptr);
  data->alloc_ptr= 0;
}


/*
  Copy a S3_INFO structure
*/

S3_INFO *s3_info_copy(S3_INFO *old)
{
  S3_INFO *to, tmp;

  /* Copy lengths */
  memcpy(&tmp, &old, sizeof(tmp));
  /* Allocate new buffers */
  if (!my_multi_malloc(MY_WME, &to, sizeof(S3_INFO),
                       &tmp.access_key, old->access_key.length+1,
                       &tmp.secret_key, old->secret_key.length+1,
                       &tmp.region,     old->region.length+1,
                       &tmp.bucket,     old->bucket.length+1,
                       &tmp.database,   old->database.length,
                       &tmp.table,      old->table.length,
                       NullS))
    return 0;
  /* Copy lengths and new pointers to to */
  memcpy(to, &tmp, sizeof(tmp));
  /* Copy data */
  strmov((char*) to->access_key.str, old->access_key.str);
  strmov((char*) to->secret_key.str, old->secret_key.str);
  strmov((char*) to->region.str,     old->region.str);
  strmov((char*) to->bucket.str,     old->bucket.str);
  /* Database may not be null terminated */
  strmake((char*) to->database.str,  old->database.str, old->database.length);
  strmov((char*) to->table.str,      old->table.str);
  return to;
}

/**
   Open a connection to s3
*/

ms3_st *s3_open_connection(S3_INFO *s3)
{
  ms3_st *s3_client;
  if (!(s3_client= ms3_init(s3->access_key.str,
                            s3->secret_key.str,
                            s3->region.str,
                            NULL)))
  {
    my_printf_error(HA_ERR_NO_SUCH_TABLE,
                    "Can't open connection to S3, error: %d %s", MYF(0),
                    errno, ms3_error(errno));
    my_errno= HA_ERR_NO_SUCH_TABLE;
  }
  return s3_client;
}


/******************************************************************************
 High level functions to copy tables to and from S3
******************************************************************************/

/**
   Create suffix for object name
   @param to_end end of suffix (from previous call or 000000 at start)

   The suffix is a 6 length '0' prefixed number. If the number
   gets longer than 6, then it's extended to 7 and more digits.
*/

static void fix_suffix(char *to_end, ulong nr)
{
  char buff[11];
  uint length= (uint) (int10_to_str(nr, buff, 10) - buff);
  set_if_smaller(length, 6);
  strmov(to_end - length, buff);
}

/**
   Copy file to 'aws_path' in blocks of block_size

   @return 0   ok
   @return 1   error. Error message is printed to stderr

   Notes:
   file is always closed before return
*/

static my_bool copy_from_file(ms3_st *s3_client, const char *aws_bucket,
                              const char *aws_path,
                              File file, my_off_t start, my_off_t file_end,
                              uchar *block, size_t block_size,
                              my_bool compression, my_bool display)
{
  my_off_t pos;
  char *path_end= strend(aws_path);
  ulong bnr;
  my_bool print_done= 0;
  size_t length;

  for (pos= start, bnr=1 ; pos < file_end ; pos+= length, bnr++)
  {
    if ((length= my_pread(file, block, block_size, pos, MYF(MY_WME))) ==
        MY_FILE_ERROR)
      goto err;
    if (length == 0)
    {
      my_error(EE_EOFERR, MYF(0), my_filename(file), my_errno);
      goto err;
    }

    fix_suffix(path_end, bnr);
    if (s3_put_object(s3_client, aws_bucket, aws_path, block, length,
                      compression))
      goto err;

    /* Write up to DISPLAY_WITH number of '.' during copy */
    if (display &&
        ((pos + block_size) * DISPLAY_WITH / file_end) >
        (pos * DISPLAY_WITH/file_end))
    {
      fputc('.', stdout); fflush(stdout);
      print_done= 1;
    }
  }
  if (print_done)
  {
    fputc('\n', stdout); fflush(stdout);
  }
  my_close(file, MYF(MY_WME));
  return 0;

err:
  my_close(file, MYF(MY_WME));
  if (print_done)
  {
    fputc('\n', stdout); fflush(stdout);
  }
  return 1;
}


/**
   Copy an Aria table to S3
   @param s3_client    connection to S3
   @param aws_bucket   Aws bucket
   @param path         Path for Aria table (can be temp table)
   @param database     database name
   @param table_name   table name
   @param block_size   Block size in s3. If 0 then use block size
                       and compression as specified in the .MAI file as
                       specified as part of open.
   @param compression  Compression algorithm (0 = none, 1 = zip)
                       If block size is 0 then use .MAI file.
   @return 0  ok
   @return 1  error

   The table will be copied in S3 into the following locations:

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
*/

my_bool aria_copy_to_s3(ms3_st *s3_client, const char *aws_bucket,
                        const char *path,
                        const char *database, const char *table_name,
                        ulong block_size, my_bool compression,
                        my_bool force, my_bool display)
{
  ARIA_TABLE_CAPABILITIES cap;
  char aws_path[FN_REFLEN+100];
  char filename[FN_REFLEN];
  char *aws_path_end, *end;
  uchar *alloc_block= 0, *block;
  File file= -1;
  my_off_t file_size;
  int error;
  ms3_status_st status;
  DBUG_ENTER("aria_copy_to_s3");

  /* Index file name */
  fn_format(filename, path, "", ".MAI", MY_REPLACE_EXT);

  aws_path_end= strxmov(aws_path, database, "/", table_name, NullS);
  strmov(aws_path_end, "/aria");

  if (!ms3_status(s3_client, aws_bucket, aws_path, &status))
  {
    if (!force)
    {
      my_printf_error(EE_CANTCREATEFILE, "File %s exists in s3", MYF(0),
                      aws_path);
      DBUG_RETURN(1);
    }
    if (aria_delete_from_s3(s3_client, aws_bucket, database, table_name,
                            display))
      DBUG_RETURN(1);
  }

  if (display)
    printf("Copying aria table: %s.%s to s3\n", database, table_name);

  if ((file= my_open(filename,
                     O_RDONLY | O_SHARE | O_NOFOLLOW | O_CLOEXEC,
                     MYF(MY_WME))) < 0)
    DBUG_RETURN(1);
  if ((error= aria_get_capabilities(file, &cap)))
  {
    fprintf(stderr, "Got error %d when reading Aria header from %s\n",
            error, path);
    goto err;
  }
  if (cap.transactional || cap.data_file_type != BLOCK_RECORD)
  {
    fprintf(stderr,
            "Aria table %s doesn't match criteria to be copied to S3.\n"
            "It should be non-transactional and should have row_format page",
            path);
    goto err;
  }
  /*
    If block size is not specified, use the values specified as part of
    create
  */
  if (block_size == 0)
  {
    block_size=  cap.s3_block_size;
    compression= cap.compression;
  }

  /* Align s3_block size with table block size */
  block_size= (block_size/cap.block_size)*cap.block_size;

  /* Allocate block for data + flag for compress header */
  if (!(alloc_block= (uchar*) my_malloc(block_size+ALIGN_SIZE(1),
                                        MYF(MY_WME))))
    goto err;
  /* Read/write data here, but with prefix space for compression flag */
  block= alloc_block+ ALIGN_SIZE(1);

  if (my_pread(file, block, cap.header_size, 0, MYF(MY_WME | MY_FNABP)))
    goto err;

  strmov(aws_path_end, "/aria");

  if (display)
    printf("Creating aria table information %s\n", aws_path);

  convert_index_to_s3_format(block, block_size, compression);

  /*
    The first page is not compressed as we need it to know if the rest is
    compressed
  */
  if (s3_put_object(s3_client, aws_bucket, aws_path, block, cap.header_size,
                    0 /* no compression */ ))
    goto err;

  file_size= my_seek(file, 0L, MY_SEEK_END, MYF(0));

  end= strmov(aws_path_end,"/index");

  if (display)
    printf("Copying index information %s\n", aws_path);

  /* The 000000 will be update with block number by fix_suffix() */
  end= strmov(end, "/000000");

  error= copy_from_file(s3_client, aws_bucket, aws_path, file, cap.header_size,
                        file_size, block, block_size, compression, display);
  file= -1;
  if (error)
    goto err;

  /* Copy data file */
  fn_format(filename, path, "", ".MAD", MY_REPLACE_EXT);
  if ((file= my_open(filename,
                           O_RDONLY | O_SHARE | O_NOFOLLOW | O_CLOEXEC,
                           MYF(MY_WME))) < 0)
    DBUG_RETURN(1);

  file_size= my_seek(file, 0L, MY_SEEK_END, MYF(0));

  end= strmov(aws_path_end, "/data");

  if (display)
    printf("Copying data information %s\n", aws_path);

  /* The 000000 will be update with block number by fix_suffix() */
  end= strmov(end, "/000000");

  error= copy_from_file(s3_client, aws_bucket, aws_path, file, 0, file_size,
                        block, block_size, compression, display);
  file= -1;
  if (error)
    goto err;

  /* Copy frm file if it exists */
  fn_format(filename, path, "", ".frm", MY_REPLACE_EXT);
  if ((file= my_open(filename,
                     O_RDONLY | O_SHARE | O_NOFOLLOW | O_CLOEXEC,
                     MYF(0))) > 0)
  {
    if (display)
      printf("Copying frm file %s\n", filename);
    if ((file_size= my_seek(file, 0L, MY_SEEK_END, MYF(0))) >= block_size)
    {
      my_printf_error(HA_ERR_NOT_A_TABLE,
                      "Error: Size of %s is bigger than block size %lu",
                      MYF(0), filename, block_size);
    }
    else
    {
      if (my_pread(file, block, file_size, 0, MYF(MY_WME| MY_FNABP)))
        goto err;
      end= strmov(aws_path_end,"/frm");

      convert_frm_to_s3_format(block);

      if (s3_put_object(s3_client, aws_bucket, aws_path, block, file_size,
                        compression))
        goto err;
    }
    my_close(file, MYF(MY_WME));
    file= -1;
  }

  my_free(alloc_block);
  DBUG_RETURN(0);

err:
  if (file >= 0)
    my_close(file, MYF(0));
  my_free(alloc_block);
  DBUG_RETURN(1);
}


/**
   Copy file to 'aws_path' in blocks of block_size

   @return 0   ok
   @return 1   error. Error message is printed to stderr

   Notes:
   file is always closed before return
*/

static my_bool copy_to_file(ms3_st *s3_client, const char *aws_bucket,
                            char *aws_path, File file, my_off_t start,
                            my_off_t file_end, my_bool compression,
                            my_bool display)
{
  my_off_t pos;
  char *path_end= strend(aws_path);
  size_t error;
  ulong bnr;
  my_bool print_done= 0;
  s3_block block;
  DBUG_ENTER("copy_to_file");
  DBUG_PRINT("enter", ("path: %s  start: %llu  end: %llu",
                       aws_path, (ulonglong) start, (ulonglong) file_end));

  for (pos= start, bnr=1 ; pos < file_end ; pos+= block.length, bnr++)
  {
    fix_suffix(path_end, bnr);
    if (s3_get_object(s3_client, aws_bucket, aws_path, &block, compression, 1))
      goto err;

    error= my_write(file, block.str, block.length, MYF(MY_WME | MY_WME));
    s3_free(&block);
    if (error == MY_FILE_ERROR)
      goto err;

    /* Write up to DISPLAY_WITH number of '.' during copy */
    if (display &&
        ((pos + block.length) * DISPLAY_WITH /file_end) >
        (pos * DISPLAY_WITH/file_end))
    {
      fputc('.', stdout); fflush(stdout);
      print_done= 1;
    }
  }
  if (print_done)
  {
    fputc('\n', stdout); fflush(stdout);
  }
  my_close(file, MYF(MY_WME));
  DBUG_RETURN(0);

err:
  my_close(file, MYF(MY_WME));
  if (print_done)
  {
    fputc('\n', stdout); fflush(stdout);
  }
  DBUG_RETURN(1);
}


/**
   Copy a table from S3 to current directory
*/

my_bool aria_copy_from_s3(ms3_st *s3_client, const char *aws_bucket,
                          const char *path, const char *database,
                          my_bool compression, my_bool force, my_bool display)

{
  MARIA_STATE_INFO state;
  MY_STAT stat_info;
  char table_name[FN_REFLEN], aws_path[FN_REFLEN+100];
  char filename[FN_REFLEN];
  char *aws_path_end, *end;
  File file;
  s3_block block;
  my_off_t index_file_size, data_file_size;
  uint offset;
  int error;
  DBUG_ENTER("aria_copy_from_s3");

  /* Check if index file exists */
  fn_format(filename, path, "", ".MAI", MY_REPLACE_EXT);
  if (!force && my_stat(filename, &stat_info, MYF(0)))
  {
    my_printf_error(EE_CANTCREATEFILE, "Table %s already exists on disk",
                    MYF(0), filename);
    DBUG_RETURN(1);
  }

  fn_format(table_name, path, "", "", MY_REPLACE_DIR | MY_REPLACE_EXT);
  block.str= 0;

  aws_path_end= strxmov(aws_path, database, "/", table_name, NullS);
  strmov(aws_path_end, "/aria");

  if (s3_get_object(s3_client, aws_bucket, aws_path, &block, 0, 0))
  {
    my_printf_error(EE_FILENOTFOUND, "Table %s doesn't exist in s3", MYF(0),
                    filename);
    goto err;
  }
  if (block.length < MARIA_STATE_INFO_SIZE)
  {
    fprintf(stderr, "Wrong block length for first block: %lu\n",
            (ulong) block.length);
    goto err_with_free;
  }

  if (display)
    printf("Copying aria table: %s.%s from s3\n", database, table_name);

  /* For offset positions, check _ma_state_info_readlength() */
  offset= sizeof(state.header) + 4+ LSN_STORE_SIZE*3 + 8*5;
  index_file_size= mi_sizekorr(block.str + offset);
  data_file_size=  mi_sizekorr(block.str + offset+8);

  if ((file= my_create(filename, 0,
                       O_WRONLY | O_TRUNC | O_NOFOLLOW, MYF(MY_WME))) < 0)
    goto err_with_free;

  convert_index_to_disk_format(block.str);

  if (my_write(file, block.str, block.length, MYF(MY_WME | MY_FNABP)))
    goto err_with_free;

  if (display)
    printf("Copying index information %s\n", aws_path);

  end= strmov(aws_path_end,"/index/000000");

  error= copy_to_file(s3_client, aws_bucket, aws_path, file, block.length,
                      index_file_size, compression, display);
  file= -1;
  if (error)
    goto err_with_free;

  /* Copy data file */
  fn_format(filename, path, "", ".MAD", MY_REPLACE_EXT);
  if ((file= my_create(filename, 0,
                       O_WRONLY | O_TRUNC | O_NOFOLLOW, MYF(MY_WME))) < 0)
    DBUG_RETURN(1);

  end= strmov(aws_path_end, "/data");

  if (display)
    printf("Copying data information %s\n", aws_path);

  /* The 000000 will be update with block number by fix_suffix() */
  strmov(end, "/000000");

  error= copy_to_file(s3_client, aws_bucket, aws_path, file, 0, data_file_size,
                      compression, display);
  file= -1;
  s3_free(&block);
  block.str= 0;
  if (error)
    goto err;

  /* Copy frm file if it exists */
  strmov(aws_path_end, "/frm");
  if (!s3_get_object(s3_client, aws_bucket, aws_path, &block, compression, 0))
  {
    fn_format(filename, path, "", ".frm", MY_REPLACE_EXT);
    if ((file= my_create(filename, 0,
                         O_WRONLY | O_SHARE | O_NOFOLLOW | O_CLOEXEC,
                         MYF(0))) >= 0)
    {
      if (display)
        printf("Copying frm file %s\n", filename);

      convert_frm_to_disk_format(block.str);

      if (my_write(file, block.str, block.length, MYF(MY_WME | MY_FNABP)))
        goto err_with_free;
    }
    s3_free(&block);
    my_close(file, MYF(MY_WME));
    file= -1;
  }

  DBUG_RETURN(0);

err_with_free:
  s3_free(&block);
err:
  if (file >= 0)
    my_close(file, MYF(0));
  DBUG_RETURN(1);
}


/*
  Drop all files in a 'directory' in s3
*/

my_bool s3_delete_directory(ms3_st *s3_client, const char *aws_bucket,
                            const char *path)
{
  ms3_list_st *list, *org_list= 0;
  my_bool error= 0;
  DBUG_ENTER("delete_directory");

  if ((error= ms3_list(s3_client, aws_bucket, path, &org_list)))
  {
    my_printf_error(EE_FILENOTFOUND,
                    "Can't get list of files from %s. Error: %d %s", MYF(0),
                    path, error, ms3_server_error(s3_client));
    DBUG_RETURN(1);
  }

  for (list= org_list ; list ; list= list->next)
    if (s3_delete_object(s3_client, aws_bucket, list->key, 1))
      error= 1;
  if (org_list)
    ms3_list_free(org_list);
  DBUG_RETURN(error);
}


/**
   Drop all files related to a table from S3
*/

my_bool aria_delete_from_s3(ms3_st *s3_client, const char *aws_bucket,
                            const char *database, const char *table,
                            my_bool display)
{
  ms3_status_st status;
  char aws_path[FN_REFLEN+100];
  char *aws_path_end;
  int error;
  DBUG_ENTER("aria_delete_from_s3");

  aws_path_end= strxmov(aws_path, database, "/", table, NullS);
  strmov(aws_path_end, "/aria");

  if (ms3_status(s3_client, aws_bucket, aws_path, &status))
  {
    my_printf_error(HA_ERR_NO_SUCH_TABLE,
                    "Table %s.%s doesn't exist in s3", MYF(0), database, table);
    my_errno= HA_ERR_NO_SUCH_TABLE;
    DBUG_RETURN(1);
  }

  if (display)
    printf("Delete of aria table: %s.%s\n", database, table);

  strmov(aws_path_end,"/index");

  if (display)
    printf("Delete of index information %s\n", aws_path);

  error= s3_delete_directory(s3_client, aws_bucket, aws_path);

  strmov(aws_path_end,"/data");
  if (display)
    printf("Delete of data information %s\n", aws_path);

  error|= s3_delete_directory(s3_client, aws_bucket, aws_path);

  if (display)
    printf("Delete of base information and frm\n");

  strmov(aws_path_end,"/frm");
  /* Ignore error if .frm file doesn't exist */
  s3_delete_object(s3_client, aws_bucket, aws_path, 0);

  strmov(aws_path_end,"/aria");
  if (s3_delete_object(s3_client, aws_bucket, aws_path, 1))
    error= 1;

  DBUG_RETURN(error);
}


/**
   Create an object for index or data information

   Note that if compression is used, the data may be overwritten and
   there must be COMPRESS_HEADER length of free space before the data
*/

my_bool s3_put_object(ms3_st *s3_client, const char *aws_bucket,
                      const char *name, uchar *data, size_t length,
                      my_bool compression)
{
  uint8_t error;
  DBUG_ENTER("s3_put_object");
  DBUG_PRINT("enter", ("name: %s", name));

  if (compression)
  {
    size_t comp_len;

    data[-COMPRESS_HEADER]= 0;                  // No compression
    if (!my_compress(data, &length, &comp_len))
      data[-COMPRESS_HEADER]= 1;                // Compressed package
    data-=   COMPRESS_HEADER;
    length+= COMPRESS_HEADER;
    int3store(data+1, comp_len);               // Original length or 0
  }

  if (likely(!(error= ms3_put(s3_client, aws_bucket, name, data, length))))
    DBUG_RETURN(FALSE);

  my_printf_error(EE_WRITE, "Got error from put_object(%s): %d %s", MYF(0),
                  name, error, ms3_error(error));
  DBUG_RETURN(TRUE);
}


/******************************************************************************
 Low level functions interfacing with libmarias3
******************************************************************************/

/**
   Read an object for index or data information
*/

my_bool s3_get_object(ms3_st *s3_client, const char *aws_bucket,
                      const char *name, s3_block *block,
                      my_bool compression, my_bool print_error)
{
  uint8_t error;
  uchar *data;
  DBUG_ENTER("s3_get_object");
  DBUG_PRINT("enter", ("name: %s  compression: %d", name, compression));

  block->str= block->alloc_ptr= 0;
  if (likely(!(error= ms3_get(s3_client, aws_bucket, name,
                              (uint8_t**) &block->str,
                              &block->length))))
  {
    block->alloc_ptr= block->str;
    if (compression)
    {
      size_t length;

      /* If not compressed */
      if (!block->str[0])
      {
        block->length-= COMPRESS_HEADER;
        block->str+=    COMPRESS_HEADER;

        /* Simple check to ensure that it's a correct block */
        if (block->length % 1024)
        {
          s3_free(block);
          my_printf_error(HA_ERR_NOT_A_TABLE,
                          "Block '%s' is not compressed", MYF(0), name);
          DBUG_RETURN(TRUE);
        }
        DBUG_RETURN(FALSE);
      }

      if (((uchar*)block->str)[0] > 1)
      {
        s3_free(block);
        my_printf_error(HA_ERR_NOT_A_TABLE,
                        "Block '%s' is not compressed", MYF(0), name);
        DBUG_RETURN(TRUE);
      }

      length= uint3korr(block->str+1);

      if (!(data= (uchar*) my_malloc(length, MYF(MY_WME | MY_THREAD_SPECIFIC))))
      {
        s3_free(block);
        DBUG_RETURN(TRUE);
      }
      if (uncompress(data, &length, block->str + COMPRESS_HEADER,
                     block->length - COMPRESS_HEADER))
      {
        my_printf_error(ER_NET_UNCOMPRESS_ERROR,
                        "Got error uncompressing s3 packet", MYF(0));
        s3_free(block);
        my_free(data);
        DBUG_RETURN(TRUE);
      }
      s3_free(block);
      block->str= block->alloc_ptr= data;
      block->length= length;
    }
    DBUG_RETURN(FALSE);
  }
  if (print_error)
  {
    if (error == 9)
    {
      my_printf_error(EE_FILENOTFOUND, "Expected object '%s' didn't exists",
                      MYF(0), name);
      my_errno= EE_FILENOTFOUND;
    }
    else
    {
      my_printf_error(EE_READ, "Got error from get_object(%s): %d %s", MYF(0),
                      name, error, ms3_error(error));
      my_errno= EE_READ;
    }
  }
  DBUG_RETURN(TRUE);
}


my_bool s3_delete_object(ms3_st *s3_client, const char *aws_bucket,
                         const char *name, my_bool print_error)
{
  uint8_t error;
  DBUG_ENTER("s3_delete_object");
  DBUG_PRINT("enter", ("name: %s", name));

  if (likely(!(error= ms3_delete(s3_client, aws_bucket, name))))
    DBUG_RETURN(FALSE);

  if (print_error)
  {
    if (error == 9)
      my_printf_error(EE_FILENOTFOUND, "Expected object '%s' didn't exists",
                      MYF(0), name);
    else
      my_printf_error(EE_READ, "Got error from delete_object(%s): %d %s", MYF(0),
                      name, error, ms3_error(error));
  }
  DBUG_RETURN(TRUE);
}

/******************************************************************************
 Converting index and frm files to from S3 storage engine
******************************************************************************/

/**
  Change index information to be of type s3

  @param header      Copy of header in index file
  @param block_size  S3 block size
  @param compression Compression algorithm to use

  The position are from _ma_base_info_write()
*/

static void convert_index_to_s3_format(uchar *header, ulong block_size,
                                       int compression)
{
  MARIA_STATE_INFO state;
  uchar *base_pos;
  uint  base_offset;

  memcpy(state.header.file_version, header, sizeof(state.header));
  base_offset= mi_uint2korr(state.header.base_pos);
  base_pos= header + base_offset;

  base_pos[107]= (uchar) compression;
  mi_int3store(base_pos+119, block_size);
}


/**
   Change index information to be a normal disk based table
*/

static void convert_index_to_disk_format(uchar *header)
{
  MARIA_STATE_INFO state;
  uchar *base_pos;
  uint  base_offset;

  memcpy(state.header.file_version, header, sizeof(state.header));
  base_offset= mi_uint2korr(state.header.base_pos);
  base_pos= header + base_offset;

  base_pos[107]= 0;
  mi_int3store(base_pos+119, 0);
}

/**
  Change storage engine in the .frm file from Aria to s3

  For information about engine types, see legacy_db_type
*/

static void convert_frm_to_s3_format(uchar *header)
{
  DBUG_ASSERT(header[3] == 42 || header[3] == 41); /* Aria or S3 */
  header[3]= 41;                                   /* S3 */
}

/**
  Change storage engine in the .frm file from S3 to Aria

  For information about engine types, see legacy_db_type
*/

static void convert_frm_to_disk_format(uchar *header)
{
  DBUG_ASSERT(header[3] == 41);                 /* S3 */
  header[3]= 42;                                /* Aria */
}


/******************************************************************************
 Helper functions
******************************************************************************/

/*
  Set database and table name from path

  s3->database and s3->table_name will be pointed into path
  Note that s3->database will not be null terminated!
*/

my_bool set_database_and_table_from_path(S3_INFO *s3, const char *path)
{
  size_t org_length= dirname_length(path);
  size_t length= 0;

  if (!org_length)
    return 1;

  s3->table.str= path+org_length;
  s3->table.length= strlen(s3->table.str);
  for (length= --org_length; length > 0 ; length --)
  {
    if (path[length-1] == FN_LIBCHAR || path[length-1] == '/')
      break;
#ifdef FN_DEVCHAR
    if (path[length-1] == FN_DECVHAR)
      break;
#endif
  }
  if (length &&
      (path[length] != FN_CURLIB || org_length - length != 1))
  {
    s3->database.str= path + length;
    s3->database.length= org_length - length;
    return 0;
  }
  return 1;                                     /* Can't find database */
}

/******************************************************************************
 Reading blocks from index or data from S3
******************************************************************************/

/*
  Read the index header (first page) from the index file

  In case of error, my_error() is called
*/

my_bool read_index_header(ms3_st *client, S3_INFO *s3, s3_block *block)
{
  char aws_path[AWS_PATH_LENGTH];
  DBUG_ENTER("read_index_header");
  strxnmov(aws_path, sizeof(aws_path)-1, s3->database.str, "/", s3->table.str,
           "/aria", NullS);
  DBUG_RETURN(s3_get_object(client, s3->bucket.str, aws_path, block, 0, 1));
}

#ifdef FOR_FUTURE_IF_NEEDED_FOR_DEBUGGING_WITHOUT_S3
/**
   Read a big block from disk
*/

my_bool s3_block_read(struct st_pagecache *pagecache,
                             PAGECACHE_IO_HOOK_ARGS *args,
                             struct st_pagecache_file *file,
                             LEX_STRING *data)
{
  MARIA_SHARE *share= (MARIA_SHARE*) file->callback_data;
  my_bool datafile= file != &share->kfile;

  DBUG_ASSERT(file->big_block_size > 0);
  DBUG_ASSERT(((((my_off_t) args->pageno - file->head_blocks) <<
                pagecache->shift) %
               file->big_block_size) == 0);

  if (!(data->str= (char *) my_malloc(file->big_block_size, MYF(MY_WME))))
    return TRUE;

  data->length= mysql_file_pread(file->file,
                                 (unsigned char *)data->str,
                                 file->big_block_size,
                                 ((my_off_t) args->pageno << pagecache->shift),
                                 MYF(MY_WME));
  if (data->length == 0 || data->length == MY_FILE_ERROR)
  {
    if (data->length == 0)
    {
      LEX_STRING *file_name= (datafile ?
                              &share->data_file_name :
                              &share->index_file_name);
      my_error(EE_EOFERR, MYF(0), file_name->str, my_errno);
    }
    my_free(data->str);
    data->length= 0;
    data->str= 0;
    return TRUE;
  }
  return FALSE;
}
#endif


/**
   Read a block from S3 to page cache
*/

my_bool s3_block_read(struct st_pagecache *pagecache,
                      PAGECACHE_IO_HOOK_ARGS *args,
                      struct st_pagecache_file *file,
                      s3_block *block)
{
  char aws_path[AWS_PATH_LENGTH];
  MARIA_SHARE *share= (MARIA_SHARE*) file->callback_data;
  my_bool datafile= file->file != share->kfile.file;
  MARIA_HA *info= (MARIA_HA*) my_thread_var->keycache_file;
  ms3_st *client= info->s3;
  const char *path_suffix= datafile ? "/data/" : "/index/";
  char *end;
  S3_INFO *s3= share->s3_path;
  ulong block_number;
  DBUG_ENTER("s3_block_read");

  DBUG_ASSERT(file->big_block_size > 0);
  DBUG_ASSERT(((((my_off_t) args->pageno - file->head_blocks) <<
                pagecache->shift) %
               file->big_block_size) == 0);

  block_number= (((args->pageno - file->head_blocks) << pagecache->shift) /
                 file->big_block_size) + 1;

  end= strxnmov(aws_path, sizeof(aws_path)-12, s3->database.str, "/",
                s3->table.str, path_suffix, "000000", NullS);
  fix_suffix(end, block_number);

  DBUG_RETURN(s3_get_object(client, s3->bucket.str, aws_path, block,
                            share->base.compression_algorithm, 1));
}

/*
  Start file numbers from 1000 to more easily find bugs when the file number
  could be mistaken for a real file
*/
static volatile int32 unique_file_number= 1000;

int32 s3_unique_file_number()
{
  return my_atomic_add32_explicit(&unique_file_number, 1,
                                  MY_MEMORY_ORDER_RELAXED);
}
