#include "mariadb.h"
#include "table.h"
#include "sql_class.h"
#include "opt_range.h"
#include "rowid_filter.h"
#include "sql_select.h"

inline
double Range_filter_cost_info::lookup_cost(
                               Rowid_filter_container_type cont_type)
{
  switch (cont_type) {
  case ORDERED_ARRAY_CONTAINER:
    return log(est_elements)*0.01;
  default:
    DBUG_ASSERT(0);
    return 0;
  }
}


inline
double Range_filter_cost_info::avg_access_and_eval_gain_per_row(
                        Rowid_filter_container_type cont_type)
{
  return (1+1.0/TIME_FOR_COMPARE) * (1 - selectivity) -
         lookup_cost(cont_type);
}

/**
  Sets information about filter with key_numb index.
  It sets a cardinality of filter, calculates its selectivity
  and gets slope and interscept values.
*/

void Range_filter_cost_info::init(Rowid_filter_container_type cont_type,
                                  TABLE *tab, uint idx)
{
  container_type= cont_type;
  table= tab;
  key_no= idx;
  est_elements= table->quick_rows[key_no];
  b= build_cost(container_type);
  selectivity= est_elements/((double) table->stat_records());
  a= avg_access_and_eval_gain_per_row(container_type);
  if (a > 0)
    cross_x= b/a;
  abs_independent.clear_all();
}

double
Range_filter_cost_info::build_cost(Rowid_filter_container_type container_type)
{
  double cost= 0;

  cost+= table->quick_index_only_costs[key_no];

  switch (container_type) {

  case ORDERED_ARRAY_CONTAINER:
    cost+= ARRAY_WRITE_COST * est_elements; /* cost filling the container */
    cost+= ARRAY_SORT_C * est_elements * log(est_elements); /* sorting cost */
    break;
  default:
    DBUG_ASSERT(0);
  }

  return cost;
}


static
int compare_range_filter_cost_info_by_a(Range_filter_cost_info **filter_ptr_1,
                                        Range_filter_cost_info **filter_ptr_2)
{
  double diff= (*filter_ptr_2)->a - (*filter_ptr_1)->a;
  return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
}

/**
  @brief

  @details
*/

void TABLE::prune_range_filters()
{
  uint i, j;

  Range_filter_cost_info **filter_ptr_1= range_filter_cost_info_ptr;
  for (i= 0; i < range_filter_cost_info_elems; i++, filter_ptr_1++)
  {
    uint key_no= (*filter_ptr_1)->key_no;
    Range_filter_cost_info **filter_ptr_2= filter_ptr_1 + 1;
    for (j= i+1; j < range_filter_cost_info_elems; j++, filter_ptr_2++)
    {
      key_map map= key_info[key_no].overlapped;
      map.intersect(key_info[(*filter_ptr_2)->key_no].overlapped);
      if (map.is_clear_all())
      {
        (*filter_ptr_1)->abs_independent.set_bit((*filter_ptr_2)->key_no);
        (*filter_ptr_2)->abs_independent.set_bit(key_no);
      }
    }
  }

  /* Sort the array range_filter_cost_info by 'a' */
  my_qsort(range_filter_cost_info_ptr,
           range_filter_cost_info_elems,
           sizeof(Range_filter_cost_info *),
           (qsort_cmp) compare_range_filter_cost_info_by_a);

  Range_filter_cost_info **cand_filter_ptr= range_filter_cost_info_ptr;
  for (i= 0; i < range_filter_cost_info_elems; i++, cand_filter_ptr++)
  {
    bool is_pruned= false;
    Range_filter_cost_info **usable_filter_ptr= range_filter_cost_info_ptr;
    key_map abs_indep;
    abs_indep.clear_all();
    for (uint j= 0; j < i; j++, usable_filter_ptr++)
    {
      if ((*cand_filter_ptr)->cross_x >= (*usable_filter_ptr)->cross_x)
      {
        if (abs_indep.is_set((*usable_filter_ptr)->key_no))
	{
	  is_pruned= true;
          break;
        }
        abs_indep.merge((*usable_filter_ptr)->abs_independent);
      }
      else
      {
        Range_filter_cost_info *moved= *cand_filter_ptr;
        memmove(usable_filter_ptr+1, usable_filter_ptr,
                sizeof(Range_filter_cost_info *) * (i-j-1));
        *usable_filter_ptr= moved;
      }
    }
    if (is_pruned)
    {
      memmove(cand_filter_ptr, cand_filter_ptr+1,
              sizeof(Range_filter_cost_info *) *
              (range_filter_cost_info_elems - 1 - i));
      range_filter_cost_info_elems--;
    }
  }
}


static uint
get_max_range_filter_elements_for_table(THD *thd, TABLE *tab,
                                        Rowid_filter_container_type cont_type)
{
  switch (cont_type) {
  case ORDERED_ARRAY_CONTAINER :
    return thd->variables.max_rowid_filter_size/tab->file->ref_length;
  default :
    DBUG_ASSERT(0);
    return 0;
  }
}

void TABLE::init_cost_info_for_usable_range_filters(THD *thd)
{
  uint key_no;
  key_map usable_range_filter_keys;
  usable_range_filter_keys.clear_all();
  key_map::Iterator it(quick_keys);
  while ((key_no= it++) != key_map::Iterator::BITMAP_END)
  {
    if (!(file->index_flags(key_no, 0, 1) & HA_DO_RANGE_FILTER_PUSHDOWN))
      continue;
    if (key_no == s->primary_key && file->primary_key_is_clustered())
      continue;
   if (quick_rows[key_no] >
       get_max_range_filter_elements_for_table(thd, this,
                                               ORDERED_ARRAY_CONTAINER))
      continue;
    usable_range_filter_keys.set_bit(key_no);
  }

  range_filter_cost_info_elems= usable_range_filter_keys.bits_set();
  if (!range_filter_cost_info_elems)
    return;

  range_filter_cost_info_ptr=
    (Range_filter_cost_info **) thd->calloc(sizeof(Range_filter_cost_info *) *
                                            range_filter_cost_info_elems);
  range_filter_cost_info=
    new (thd->mem_root) Range_filter_cost_info[range_filter_cost_info_elems];
  if (!range_filter_cost_info_ptr || !range_filter_cost_info)
  {
    range_filter_cost_info_elems= 0;
    return;
  }

  Range_filter_cost_info **curr_ptr= range_filter_cost_info_ptr;
  Range_filter_cost_info *curr_filter_cost_info= range_filter_cost_info;

  key_map::Iterator li(usable_range_filter_keys);
  while ((key_no= li++) != key_map::Iterator::BITMAP_END)
  {
    *curr_ptr= curr_filter_cost_info;
    curr_filter_cost_info->init(ORDERED_ARRAY_CONTAINER, this, key_no);
    curr_ptr++;
    curr_filter_cost_info++;
  }
  prune_range_filters();
}


Range_filter_cost_info *TABLE::best_filter_for_partial_join(uint access_key_no,
                                                            double records)
{
  if (!this || range_filter_cost_info_elems == 0 ||
      covering_keys.is_set(access_key_no))
    return 0;

  if (access_key_no == s->primary_key && file->primary_key_is_clustered())
    return 0;

  Range_filter_cost_info *best_filter= 0;
  double best_filter_gain= 0;

  key_map *overlapped= &key_info[access_key_no].overlapped;
  for (uint i= 0; i < range_filter_cost_info_elems ;  i++)
  {
    double curr_gain = 0;
    Range_filter_cost_info *filter= range_filter_cost_info_ptr[i];
    if ((filter->key_no == access_key_no) ||
        overlapped->is_set(filter->key_no))
      continue;
    if (records < filter->cross_x)
      break;
    curr_gain= filter->get_gain(records);
    if (best_filter_gain < curr_gain)
    {
      best_filter_gain= curr_gain;
      best_filter= filter;
    }
  }
  return best_filter;
}


bool Range_filter_ordered_array::fill()
{
  int rc= 0;
  handler *file= table->file;
  THD *thd= table->in_use;
  QUICK_RANGE_SELECT* quick= (QUICK_RANGE_SELECT*) select->quick;
  uint table_status_save= table->status;
  Item *pushed_idx_cond_save= file->pushed_idx_cond;
  uint pushed_idx_cond_keyno_save= file->pushed_idx_cond_keyno;
  bool in_range_check_pushed_down_save= file->in_range_check_pushed_down;

  table->status= 0;
  file->pushed_idx_cond= 0;
  file->pushed_idx_cond_keyno= MAX_KEY;
  file->in_range_check_pushed_down= false;

  /* We're going to just read rowids. */
  table->prepare_for_position();

  table->file->ha_start_keyread(quick->index);

  if (quick->init() || quick->reset())
    rc= 1;

  while (!rc)
  {
    rc= quick->get_next();
    if (thd->killed)
      rc= 1;
    if (!rc)
    {
      file->position(quick->record);
      if (refpos_container.add((char*) file->ref))
        rc= 1;
    }
  }

  quick->range_end();
  table->file->ha_end_keyread();
  table->status= table_status_save;
  file->pushed_idx_cond= pushed_idx_cond_save;
  file->pushed_idx_cond_keyno= pushed_idx_cond_keyno_save;
  file->in_range_check_pushed_down= in_range_check_pushed_down_save;
  if (rc != HA_ERR_END_OF_FILE)
    return 1;
  container_is_filled= true;
  table->file->rowid_filter_is_active= true;
  return 0;
}


bool Range_filter_ordered_array::sort()
{
  refpos_container.sort(refpos_order_cmp, (void *) (table->file));
  return false;
}


bool Range_filter_ordered_array::check(char *elem)
{
  int l= 0;
  int r= refpos_container.elements()-1;
  while (l <= r)
  {
    int m= (l + r) / 2;
    int cmp= refpos_order_cmp((void *) (table->file),
                              refpos_container.get_pos(m), elem);
    if (cmp == 0)
      return true;
    if (cmp < 0)
      l= m + 1;
    else
      r= m-1;
  }
  return false;
}


Range_filter_ordered_array::~Range_filter_ordered_array()
{
  if (select)
  {
    if (select->quick)
    {
      delete select->quick;
      select->quick= 0;
    }
    delete select;
    select= 0;
  }
}
