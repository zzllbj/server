#ifndef ROWID_FILTER_INCLUDED
#define ROWID_FILTER_INCLUDED


#include "mariadb.h"
#include "sql_array.h"

/**
  @class Rowid_filter

  What rowid / primary filters are
  --------------------------------
 
  Consider a join query Q of the form
    SELECT * FROM T1, ... , Tk WHERE P.

  For any of the table reference Ti(Q) from the from clause of Q different
  rowid / primary key filters (pk-filters for short) can be built.
  A pk-filter F built for Ti(Q) is a set of rowids / primary keys of Ti
  F= {pk1,...,pkN} such that for any row r=r1||...||rk from the result set of Q
  ri's rowid / primary key pk(ri) is contained in F.

  When pk-filters are useful
  --------------------------
  
  If building a pk-filter F for Ti(Q )is not too costly and its cardinality #F
  is much less than the cardinality of T - #T then using the pk-filter when
  executing Q might be quite beneficial.

  Let r be a random row from Ti. Let s(F) be the probability that pk(r)
  belongs to F. Let BC(F) be the cost of building F. 

  Suppose that the optimizer has chosen for Q a plan with this join order
  T1 => ... Tk and that the table Ti is accessed by a ref access using index I.
  Let K = {k1,...,kM} be the set of all rowid/primary keys values used to access
  rows of Ti when looking for matches in this table.to join Ti by index I. 
 
  Let's assume that two set sets K and F are uncorrelated.  With this assumption
  if before accessing data from Ti by the rowid / primary key k we first
  check whether k is in F then we can expect saving on M*(1-s(S)) accesses of
  data rows from Ti. If we can guarantee that test whether k is in F is 
  relatively cheap then we can gain a lot assuming that BC(F) is much less
  then the cost of fetching M*(1-s(S)) records from Ti and following
  evaluation of conditions pushed into Ti.

  Making pk-filter test cheap
  ---------------------------

  If the search structure to test whether an element is in F can be fully
  placed in RAM then this test is expected to be be much cheaper than a random
  access of a record from Ti. We'll consider two search structures for
  pk-filters: ordered array and bloom filter. Ordered array is easy to
  implement, but it's space consuming. If a filter contains primary keys
  then at least space for each primary key from the filter must be allocated
  in the search structure. On a the opposite a bloom filter requires a
  fixed number of bits and this number does not depend on the cardinality
  of the pk-filter (10 bits per element will serve pk-filter of any size). 
*/

class TABLE;
class SQL_SELECT;
class Rowid_filter_container;
class Range_rowid_filter_cost_info;

/* Cost to write rowid into array */
#define ARRAY_WRITE_COST      0.005
/* Factor used to calculate cost of sorting rowids in array */
#define ARRAY_SORT_C          0.01
/* Cost to evaluate condition */
#define COST_COND_EVAL  0.2

typedef enum
{
  SORTED_ARRAY_CONTAINER,
  BLOOM_FILTER_CONTAINER
} Rowid_filter_container_type;

class Rowid_filter_container : public Sql_alloc
{
public:
  virtual Rowid_filter_container_type get_type() = 0;
  virtual bool alloc() = 0;
  virtual bool add(void *ctxt, char *elem) = 0;
  virtual bool check(void *ctxt, char *elem) = 0;
  virtual ~Rowid_filter_container() {}
}; 


class Rowid_filter : public Sql_alloc
{
protected:
  Rowid_filter_container *container;
public:
  Rowid_filter(Rowid_filter_container *container_arg)
    : container(container_arg) {}
 
  virtual bool build() = 0;
  virtual bool check(char *elem) = 0;

  virtual ~Rowid_filter() {}

  Rowid_filter_container *get_container() { return container; }
};


class Range_rowid_filter: public Rowid_filter
{
  TABLE *table;
  SQL_SELECT *select;
  Range_rowid_filter_cost_info *cost_info;

public:
  Range_rowid_filter(TABLE *tab,
                     Range_rowid_filter_cost_info *cost_arg,
                     Rowid_filter_container *container_arg,
                     SQL_SELECT *sel)
    : Rowid_filter(container_arg), table(tab), select(sel), cost_info(cost_arg)
  {}

  ~Range_rowid_filter();

  bool build() { return fill(); }

  bool check(char *elem) { return container->check(table, elem); }

  bool fill(); 

  SQL_SELECT *get_select() { return select; }
};


class Refpos_container_sorted_array : public Sql_alloc
{
  uint max_elements;
  uint elem_size;
  Dynamic_array<char> *array;

public:

 Refpos_container_sorted_array(uint max_elems, uint elem_sz) 
    :  max_elements(max_elems), elem_size(elem_sz), array(0) {}

  ~Refpos_container_sorted_array()
  {
    delete array;
    array= 0;
  }

  bool alloc()
  {
    array= new Dynamic_array<char> (elem_size * max_elements,
                                    elem_size * max_elements/8 + 1);
    return array == NULL;
  }

  bool add(char *elem)
  {
    for (uint i= 0; i < elem_size; i++)
    {
      if (array->append(elem[i]))
	return true;
    }
    return false;
  }

  char *get_pos(uint n)
  {
    return array->get_pos(n * elem_size);
  }

  uint elements() { return array->elements() / elem_size; }

  void sort (int (*cmp) (void *ctxt, const void *el1, const void *el2),
                         void *cmp_arg)
  {
    my_qsort2(array->front(), array->elements()/elem_size,
              elem_size, (qsort2_cmp) cmp, cmp_arg);
  }
};

class Rowid_filter_sorted_array: public Rowid_filter_container
{
  Refpos_container_sorted_array refpos_container;
  bool is_checked;
  
public:
  Rowid_filter_sorted_array(uint elems, uint elem_size)
    : refpos_container(elems, elem_size), is_checked(false) {}

  Rowid_filter_container_type get_type()
  { return SORTED_ARRAY_CONTAINER; }

  bool alloc() { return refpos_container.alloc(); }

  bool add(void *ctxt, char *elem) { return refpos_container.add(elem); }

  bool check(void *ctxt, char *elem);
};


class Range_rowid_filter_cost_info : public Sql_alloc
{
public:
  Rowid_filter_container_type container_type;
  TABLE *table;
  uint key_no;
  double est_elements;
  double b;                         // intercept of the linear function
  double a;                         // slope of the linear function
  double selectivity;
  double cross_x;
  key_map abs_independent;

  /**
    Filter cost functions
  */

  Range_rowid_filter_cost_info() : table(0), key_no(0) {}

  void init(Rowid_filter_container_type cont_type,
            TABLE *tab, uint key_numb);

  double build_cost(Rowid_filter_container_type container_type);

  inline double lookup_cost(Rowid_filter_container_type cont_type);

  inline double
  avg_access_and_eval_gain_per_row(Rowid_filter_container_type cont_type);

  /**
    Get the gain that usage of filter promises for 'rows' key entries
  */
  inline double get_gain(double rows)
  {
    return rows * a - b;
  }

  inline double get_adjusted_gain(double rows, double worst_seeks)
  {
    return get_gain(rows) -
           (1 - selectivity) * (rows - MY_MIN(rows, worst_seeks));
  }

  inline double get_cmp_gain(double rows)
  {
    return rows * (1 - selectivity) / TIME_FOR_COMPARE;
  }

  Rowid_filter_container *create_container();

};


#endif /* ROWID_FILTER_INCLUDED */
