/* Copyright (C) 2000 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

/* subselect Item */

#ifdef __GNUC__
#pragma interface			/* gcc class implementation */
#endif

class st_select_lex;
class st_select_lex_unit;
class JOIN;
class select_subselect;
class subselect_engine;
class Item_bool_func2;

typedef Item_bool_func2* (*compare_func_creator)(Item*, Item*);

/* base class for subselects */

class Item_subselect :public Item_result_field
{
  my_bool engine_owner; /* Is this item owner of engine */
  my_bool value_assigned; /* value already assigned to subselect */
protected:
  /* engine that perform execution of subselect (single select or union) */
  subselect_engine *engine; 
  /* allowed number of columns (1 for single value subqueries) */
  uint max_columns;

public:
  Item_subselect();
  Item_subselect(Item_subselect *item)
  {
    null_value= item->null_value;
    decimals= item->decimals;
    max_columns= item->max_columns;
    engine= item->engine;
    engine_owner= 0;
    name= item->name;
  }

  /* 
     We need this method, because some compilers do not allow 'this'
     pointer in constructor initialization list, but we need pass pointer
     to subselect Item class to select_subselect classes constructor.
  */
  virtual void init (THD *thd, st_select_lex *select_lex, 
		     select_subselect *result);

  ~Item_subselect();
  virtual void assign_null() 
  {
    null_value= 1;
  }
  virtual void select_transformer(st_select_lex *select_lex);
  bool assigned() { return value_assigned; }
  void assigned(bool a) { value_assigned= a; }
  enum Type type() const;
  bool is_null() { return null_value; }
  void make_field (Send_field *);
  bool fix_fields(THD *thd, TABLE_LIST *tables, Item **ref);
  virtual void fix_length_and_dec();
  table_map used_tables() const;
  bool check_loop(uint id);

  friend class select_subselect;
};

/* single value subselect */

class Item_singleval_subselect :public Item_subselect
{
protected:
  longlong int_value; /* Here stored integer value of this item */
  double real_value; /* Here stored real value of this item */
  /* 
     Here stored string value of this item.
     (str_value used only as temporary buffer, because it can be changed 
     by Item::save_field)
  */
  String string_value; 
  enum Item_result res_type; /* type of results */
  
public:
  Item_singleval_subselect(THD *thd, st_select_lex *select_lex);
  Item_singleval_subselect(Item_singleval_subselect *item):
    Item_subselect(item)
  {
    int_value= item->int_value;
    real_value= item->real_value;
    string_value.set(item->string_value, 0, item->string_value.length());
    max_length= item->max_length;
    decimals= item->decimals;
    res_type= item->res_type;
  }
  virtual void assign_null() 
  {
    null_value= 1;
    int_value= 0;
    real_value= 0;
    max_length= 4;
    res_type= STRING_RESULT;
  }
  double val ();
  longlong val_int ();
  String *val_str (String *);
  Item *new_item() { return new Item_singleval_subselect(this); }
  enum Item_result result_type() const { return res_type; }
  void fix_length_and_dec();

  friend class select_singleval_subselect;
};

/* exists subselect */

class Item_exists_subselect :public Item_subselect
{
protected:
  longlong value; /* value of this item (boolean: exists/not-exists) */

public:
  Item_exists_subselect(THD *thd, st_select_lex *select_lex);
  Item_exists_subselect(Item_exists_subselect *item):
    Item_subselect(item)
  {
    value= item->value;
  }
  Item_exists_subselect(): Item_subselect() {}

  virtual void assign_null() 
  {
    value= 0;
  }

  Item *new_item() { return new Item_exists_subselect(this); }
  enum Item_result result_type() const { return INT_RESULT;}
  longlong val_int();
  double val();
  String *val_str(String*);
  void fix_length_and_dec();
  friend class select_exists_subselect;
};

/* IN subselect */

class Item_in_subselect :public Item_exists_subselect
{
protected:
  Item * left_expr;

public:
  Item_in_subselect(THD *thd, Item * left_expr, st_select_lex *select_lex);
  Item_in_subselect(Item_in_subselect *item);
  Item_in_subselect(): Item_exists_subselect() {}
  virtual void select_transformer(st_select_lex *select_lex);
  void single_value_transformer(st_select_lex *select_lex,
				Item *left_expr, compare_func_creator func);
};

/* ALL/ANY/SOME subselect */
class Item_allany_subselect :public Item_in_subselect
{
protected:
  compare_func_creator func;

public:
  Item_allany_subselect(THD *thd, Item * left_expr, compare_func_creator f,
		     st_select_lex *select_lex);
  Item_allany_subselect(Item_allany_subselect *item);
  virtual void select_transformer(st_select_lex *select_lex);
};

class subselect_engine
{
protected:
  select_subselect *result; /* results storage class */
  THD *thd; /* pointer to current THD */
  Item_subselect *item; /* item, that use this engine */
  enum Item_result res_type; /* type of results */
public:
  static void *operator new(size_t size)
  {
    return (void*) sql_alloc((uint) size);
  }
  static void operator delete(void *ptr, size_t size) {}

  subselect_engine(THD *thd, Item_subselect *si, select_subselect *res) 
  {
    result= res;
    item= si;
    this->thd= thd;
    res_type= STRING_RESULT;
  }

  virtual int prepare()= 0;
  virtual void fix_length_and_dec()= 0;
  virtual int exec()= 0;
  virtual uint cols()= 0; /* return number of columnss in select */
  virtual bool depended()= 0; /* depended from outer select */
  enum Item_result type() { return res_type; }
  virtual bool check_loop(uint id)= 0;
};

class subselect_single_select_engine: public subselect_engine
{
  my_bool prepared; /* simple subselect is prepared */
  my_bool optimized; /* simple subselect is optimized */
  my_bool executed; /* simple subselect is executed */
  st_select_lex *select_lex; /* corresponding select_lex */
  JOIN * join; /* corresponding JOIN structure */
public:
  subselect_single_select_engine(THD *thd, st_select_lex *select,
				 select_subselect *result,
				 Item_subselect *item);
  int prepare();
  void fix_length_and_dec();
  int exec();
  uint cols();
  bool depended();
  bool check_loop(uint id);
};

class subselect_union_engine: public subselect_engine
{
  st_select_lex_unit *unit;  /* corresponding unit structure */
public:
  subselect_union_engine(THD *thd,
			 st_select_lex_unit *u,
			 select_subselect *result,
			 Item_subselect *item);
  int prepare();
  void fix_length_and_dec();
  int exec();
  uint cols();
  bool depended();
  bool check_loop(uint id);
};
