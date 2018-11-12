/* Copyright (c) 2018, MariaDB Corporation
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "mariadb.h"
#include "sql_class.h"
#include "sql_base.h"                           // flush_tables
#include <my_sys.h>

static const char *stage_names[]=
{"START", "FLUSH", "WAIT_FOR_FLUSH", "LOCK_COMMIT", "END", 0};

TYPELIB backup_stage_names=
{ array_elements(stage_names)-1, "", stage_names, 0 };

static bool backup_running;
static MDL_ticket *backup_flush_ticket;

static bool backup_start(THD *thd);
static bool backup_flush(THD *thd);
static bool backup_wait_for_flush(THD *thd);
static bool backup_lock_commit(THD *thd);

/**
  Run next stage of backup
*/

void backup_init()
{
  backup_running= 0;
  backup_flush_ticket= 0;
}

bool run_backup_stage(THD *thd, backup_stages stage)
{
  backup_stages next_stage;
  DBUG_ENTER("run_backup_stage");

  if (thd->current_backup_stage == BACKUP_FINISHED)
  {
    if (stage != BACKUP_START)
    {
      my_error(ER_BACKUP_NOT_RUNNING, MYF(0));
      DBUG_RETURN(1);
    }
    next_stage= BACKUP_START;
  }
  else
  {
    if ((uint) thd->current_backup_stage >= (uint) stage)
    {
      my_error(ER_BACKUP_WRONG_STAGE, MYF(0), stage_names[stage],
               stage_names[thd->current_backup_stage]);
      DBUG_RETURN(1);
    }
    next_stage= (backup_stages) ((uint) thd->current_backup_stage + 1);
  }

  do
  {
    bool res;
    thd->current_backup_stage= next_stage;
    switch (next_stage) {
    case BACKUP_START:
      if (!(res= backup_start(thd)))
        break;
      /* Reset backup stage to start for next backup try */
      thd->current_backup_stage= BACKUP_FINISHED;
      break;
    case BACKUP_FLUSH:
      res= backup_flush(thd);
      break;
    case BACKUP_WAIT_FOR_FLUSH:
      res= backup_wait_for_flush(thd);
      break;
    case BACKUP_LOCK_COMMIT:
      res= backup_lock_commit(thd);
      break;
    case BACKUP_END:
      res= backup_end(thd);
      break;
    case BACKUP_FINISHED:
      DBUG_ASSERT(0);
      res= 0;
    }
    if (res)
    {
      my_error(ER_BACKUP_STAGE_FAILED, MYF(0), stage_names[(uint) stage]);
      DBUG_RETURN(1);
    }
    next_stage= (backup_stages) ((uint) next_stage + 1);
  } while (next_stage <= (uint) stage);

  DBUG_RETURN(0);
}


/**
  Start the backup

  - Wait for previous backup to stop running
  - Start service to log changed tables (TODO)
  - Block purge of redo files (Required at least for Aria)
  - An handler can optionally do a checkpoint of all tables,
    to speed up the recovery stage of the backup.
*/

static bool backup_start(THD *thd)
{
  DBUG_ENTER("backup_start");
  PSI_stage_info saved_stage= {0, "", 0};

  if (thd->global_read_lock.can_acquire_protection())
    DBUG_RETURN(1);

  if (thd->locked_tables_mode)
  {
    my_error(ER_LOCK_OR_ACTIVE_TRANSACTION, MYF(0));
    DBUG_RETURN(1);
  }

  mysql_mutex_lock(&LOCK_backup);
  thd->ENTER_COND(&COND_backup, &LOCK_backup, &stage_waiting_for_backup,
                  &saved_stage);
  while (backup_running && !thd->killed)
    mysql_cond_wait(&COND_backup, &LOCK_backup);

  if (thd->killed)
  {
    mysql_cond_signal(&COND_backup);
    thd->EXIT_COND(&saved_stage);
    DBUG_RETURN(1);
  }
  backup_running= 1;
  thd->EXIT_COND(&saved_stage);

  ha_prepare_for_backup();
  DBUG_RETURN(0);
}

/**
   backup_flush()

   - FLUSH all changes for not active non transactional tables, except
     for statistics and log tables. Close the tables, to ensure they
     are marked as closed after backup.

   - BLOCK all NEW write locks for all non transactional tables
     (except statistics and log tables).  Already granted locks are
     not affected (Running statements with non transaction tables will
     continue running).

   - The following DDL's doesn't have to be blocked as they can't set
     the table in a non consistent state:
     CREATE, RENAME, DROP
*/

static bool backup_flush(THD *thd)
{
  DBUG_ENTER("backup_flush");
  /*
    Lock all non transactional normal tables to be used in new DML's
    This is done with MDL_BACKUP_FLUSH in lock_global_read_lock.
  */
  if (thd->global_read_lock.lock_global_read_lock(thd, 0))
    DBUG_RETURN(1);

  purge_tables(false);  /* Flush unused tables and shares */

  DBUG_RETURN(0);
}

/**
  backup_wait_for_flush()

  - Wait for all statements using write locked non-transactional tables to end.

  - Mark all not used active non transactional tables (except
    statistics and log tables) to be closed with
    handler->extra(HA_EXTRA_FLUSH)

  - Block TRUNCATE TABLE, CREATE TABLE, DROP TABLE and RENAME
    TABLE. Block also start of a new ALTER TABLE and the final rename
    phase of ALTER TABLE.  Running ALTER TABLES are not blocked.  Both normal
    and inline ALTER TABLE'S should be blocked when copying is completed but
    before final renaming of the tables / new table is activated.
    This will probably require a callback from the InnoDB code.
*/

static bool backup_wait_for_flush(THD *thd)
{
  MDL_request mdl_request;
  DBUG_ENTER("backup_wait_for_flush");

  /* Wait until all all non trans statements has ended */
  mdl_request.init(MDL_key::BACKUP, "", "",
                   MDL_BACKUP_WAIT_FLUSH,
                   MDL_EXPLICIT);
  if (thd->mdl_context.acquire_lock(&mdl_request,
                                    thd->variables.lock_wait_timeout))
    DBUG_RETURN(1);
  backup_flush_ticket= mdl_request.ticket;

  /*
    Remove not used tables from the table share.  Flush all changes to
    non transaction tables and mark those that are not in use in write
    operations as closed. From backup purposes it's not critical if
    flush_tables() returns an error. It's ok to continue with next
    backup stage even if we got an error.
  */
  if (flush_tables(thd, FLUSH_NON_TRANS_TABLES))
    DBUG_RETURN(thd->is_error());

  /*
    block new DDL's
    We didn't do this lock above, as we wanted DDL's to be executed while
    we wait for non transactional tables (which may take a while).
 */
  if (thd->mdl_context.upgrade_shared_lock(backup_flush_ticket,
                                           MDL_BACKUP_WAIT_DDL,
                                           thd->variables.lock_wait_timeout))
    DBUG_RETURN(1);
  DBUG_RETURN(0);
}

/**
   backup_lock_commit()

   Block commits, writes to log and statistics tables and binary log
*/

static bool backup_lock_commit(THD *thd)
{
  DBUG_ENTER("backup_lock_commit");
  if (thd->mdl_context.upgrade_shared_lock(backup_flush_ticket,
                                           MDL_BACKUP_WAIT_COMMIT,
                                           thd->variables.lock_wait_timeout))
    DBUG_RETURN(1);
  flush_tables(thd, FLUSH_SYS_TABLES);
  DBUG_RETURN(0);
}

/**
   backup_end()

   safe to run, even if backup has not been run by this thread
*/

bool backup_end(THD *thd)
{
  DBUG_ENTER("backup_end");

  if (thd->current_backup_stage != BACKUP_FINISHED)
  {
    thd->current_backup_stage= BACKUP_FINISHED;
    if (backup_flush_ticket)
    {
      thd->mdl_context.release_lock(backup_flush_ticket);
      backup_flush_ticket= 0;
    }

    if (thd->global_read_lock.is_acquired())
      thd->global_read_lock.unlock_global_read_lock(thd);
    ha_end_backup();
    mysql_mutex_lock(&LOCK_backup);
    backup_running= 0;
    mysql_cond_signal(&COND_backup);
    mysql_mutex_unlock(&LOCK_backup);
  }
  DBUG_RETURN(0);
}


/**
   backup_set_alter_copy_lock()

   Upgrades the MDL_BACKUP_STMT lock to MDL_BACKUP_ALTER_COPY to allow
   copy of altered table to proceed under MDL_BACKUP_WAIT_DDL
*/

void backup_set_alter_copy_lock(THD *thd)
{
  MDL_ticket *ticket= thd->mdl_backup_ticket;

  /* Ticket maybe NULL in case of LOCK TABLES */
  if (ticket)
    ticket->downgrade_lock(MDL_BACKUP_ALTER_COPY);
}
