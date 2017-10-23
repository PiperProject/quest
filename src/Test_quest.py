#!/usr/bin/env python

'''
Test_quest.py
'''


#############
#  IMPORTS  #
#############
# standard python packages
import inspect, logging, os, pickledb, sqlite3, sys, unittest
from StringIO import StringIO

import Quest


################
#  TEST QUEST  #
################
class Test_quest( unittest.TestCase ) :

  #logging.basicConfig( format='%(levelname)s:%(message)s', level=logging.DEBUG )
  logging.basicConfig( format='%(levelname)s:%(message)s', level=logging.INFO )


  ################
  #  EXAMPLE 20  #
  ################
  # tests a recursive query with negation
  def test_example20( self ) :

    test_id = "test_example20"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { "id0": [ { 0:1 }, [11,12] ], "0": [ 0, [ [1, 2], 111, 3 ] ] } )
    dbInst.set( "c", { "id1": [ { 0:1 }, [11,12] ], "1": [ 1, [ [1, 2], 222, 3 ] ] } )
    dbInst.set( "e", { "id2": [ { 50:51 }, [511,512] ], "1": [ 51, [ [51, 52], 5222, 53 ] ] } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query1 = "a(Z) :- d(Y,Z), b(_,Y,_,_) ;"
    q.setQuery( query1 )
    logging.debug( "  " + test_id + " : set query '" + query1 + "' to db instance." )

    query2 = "a(Z) :- c(_,Y,_,Z), notin b(_,_,Y,_) ;"
    q.setQuery( query2 )
    logging.debug( "  " + test_id + " : set query '" + query2 + "' to db instance." )

    query3 = "a(Z) :- e(_,_,Z,_) ;"
    q.setQuery( query3 )
    logging.debug( "  " + test_id + " : set query '" + query3 + "' to db instance." )

    query4 = "d(Y,Z) :- c(_,Y,_,Z) ;"
    q.setQuery( query4 )
    logging.debug( "  " + test_id + " : set query '" + query4 + "' to db instance." )

    schema = { "a":["int"], "b":["string","int","int","int"], "c":["string","int","int","int"], "e":["string","int","int","int"], "d":["int","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]
    
    expected_program       = ['define(a,{int});', \
                              'define(d,{int, int});', \
                              'define(b,{string, int, int, int});', \
                              'define(c,{string, int, int, int});', \
                              'define(e,{string, int, int, int});', \
                              'b("0",0,1,111);', \
                              'b("0",0,2,111);', \
                              'b("0",0,1,3);', \
                              'b("0",0,2,3);', \
                              'b("id0",0,1,11);', \
                              'b("id0",0,1,12);', \
                              'c("1",1,1,222);', \
                              'c("1",1,2,222);', \
                              'c("1",1,1,3);', \
                              'c("1",1,2,3);', \
                              'c("id1",0,1,11);', \
                              'c("id1",0,1,12);', \
                              'e("1",51,51,5222);', \
                              'e("1",51,52,5222);', \
                              'e("1",51,51,53);', \
                              'e("1",51,52,53);', \
                              'e("id2",50,51,511);', \
                              'e("id2",50,51,512);', \
                              query1, \
                              query2, \
                              query3, \
                              query4 ]
    expected_table_list    = ['a', 'd', 'b', 'c', 'e']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '11', \
                              '12', \
                              '51', \
                              '52', \
                              '---------------------------', \
                              'd', \
                              '1,3', \
                              '1,222', \
                              '0,11', \
                              '0,12', \
                              '---------------------------', \
                              'b', \
                              'id0,0,1,11', \
                              '0,0,1,3', \
                              'id0,0,1,12', \
                              '0,0,2,111', \
                              '0,0,2,3', \
                              '0,0,1,111', \
                              '---------------------------', \
                              'c', \
                              '1,1,1,222', \
                              '1,1,2,3', \
                              'id1,0,1,11', \
                              '1,1,2,222', \
                              'id1,0,1,12', \
                              '1,1,1,3', \
                              '---------------------------', \
                              'e', \
                              '1,51,51,5222', \
                              '1,51,52,5222', \
                              'id2,50,51,511', \
                              '1,51,51,53', \
                              '1,51,52,53', \
                              'id2,50,51,512' ]

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ################
  #  EXAMPLE 19  #
  ################
  # tests a recursive query
  def test_example19( self ) :

    test_id = "test_example19"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { "id0": [ { 0:1 }, [11,12] ], "0": [ 0, [ [1, 2], 111, 3 ] ] } )
    dbInst.set( "c", { "id1": [ { 0:1 }, [11,12] ], "1": [ 1, [ [1, 2], 222, 3 ] ] } )
    dbInst.set( "e", { "id2": [ { 50:51 }, [511,512] ], "1": [ 51, [ [51, 52], 5222, 53 ] ] } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query1 = "a(Z) :- d(Y,Z), b(_,Y,_,_) ;"
    q.setQuery( query1 )
    logging.debug( "  " + test_id + " : set query '" + query1 + "' to db instance." )

    query2 = "a(Z) :- b(_,_,Y,_), c(_,Y,_,Z) ;"
    q.setQuery( query2 )
    logging.debug( "  " + test_id + " : set query '" + query2 + "' to db instance." )

    query3 = "a(Z) :- e(_,_,Z,_) ;"
    q.setQuery( query3 )
    logging.debug( "  " + test_id + " : set query '" + query3 + "' to db instance." )

    query4 = "d(Y,Z) :- c(_,Y,_,Z) ;"
    q.setQuery( query4 )
    logging.debug( "  " + test_id + " : set query '" + query4 + "' to db instance." )

    schema = { "a":["int"], "b":["string","int","int","int"], "c":["string","int","int","int"], "e":["string","int","int","int"], "d":["int","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]
    
    expected_program       = ['define(a,{int});', \
                              'define(d,{int, int});', \
                              'define(b,{string, int, int, int});', \
                              'define(c,{string, int, int, int});', \
                              'define(e,{string, int, int, int});', \
                              'b("0",0,1,111);', \
                              'b("0",0,2,111);', \
                              'b("0",0,1,3);', \
                              'b("0",0,2,3);', \
                              'b("id0",0,1,11);', \
                              'b("id0",0,1,12);', \
                              'c("1",1,1,222);', \
                              'c("1",1,2,222);', \
                              'c("1",1,1,3);', \
                              'c("1",1,2,3);', \
                              'c("id1",0,1,11);', \
                              'c("id1",0,1,12);', \
                              'e("1",51,51,5222);', \
                              'e("1",51,52,5222);', \
                              'e("1",51,51,53);', \
                              'e("1",51,52,53);', \
                              'e("id2",50,51,511);', \
                              'e("id2",50,51,512);', \
                              query1, \
                              query2, \
                              query3, \
                              query4 ]
    expected_table_list    = ['a', 'd', 'b', 'c', 'e']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '11', \
                              '51', \
                              '52', \
                              '12', \
                              '3', \
                              '222', \
                              '---------------------------', \
                              'd', \
                              '1,3', \
                              '1,222', \
                              '0,11', \
                              '0,12', \
                              '---------------------------', \
                              'b', \
                              'id0,0,1,11', \
                              '0,0,1,3', \
                              'id0,0,1,12', \
                              '0,0,2,111', \
                              '0,0,2,3', \
                              '0,0,1,111', \
                              '---------------------------', \
                              'c', \
                              '1,1,1,222', \
                              '1,1,2,3', \
                              'id1,0,1,11', \
                              '1,1,2,222', \
                              'id1,0,1,12', \
                              '1,1,1,3', \
                              '---------------------------', \
                              'e', \
                              '1,51,51,5222', \
                              '1,51,52,5222', \
                              'id2,50,51,511', \
                              '1,51,51,53', \
                              '1,51,52,53', \
                              'id2,50,51,512' ]

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ################
  #  EXAMPLE 18  #
  ################
  # tests incorrect edb input for a subgoal. adding column.
  def test_example18( self ) :

    test_id = "test_example18"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", [ "str", { "id0": [ { 0:1 }, [11,12] ], "0": [ 0, [ [1, 2], 111, 3 ] ] } ] )
    dbInst.set( "c", { "id1": [ { 0:1 }, [11,12] ], "1": [ 1, [ [1, 2], 222, 3 ] ] } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query1 = "a(Z) :- d(Y,Z), b(_,Y,_,_) ;"
    q.setQuery( query1 )
    logging.debug( "  " + test_id + " : set query '" + query1 + "' to db instance." )

    query2 = "a(Z) :- b(_,_,Y,_), c(_,Y,_,Z) ;"
    q.setQuery( query2 )
    logging.debug( "  " + test_id + " : set query '" + query2 + "' to db instance." )

    query3 = "d(Y,Z) :- c(_,Y,_,Z) ;"
    q.setQuery( query3 )
    logging.debug( "  " + test_id + " : set query '" + query3 + "' to db instance." )

    schema = { "a":["int"], "b":["string","int","int","int"],"c":["string","int","int","int"], "d":["int","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )

    with self.assertRaises(SystemExit) as cm:
      allProgramData = q.run()
    self.assertEqual( cm.exception.code, "  FORMAT_EDB_STATEMENTS : ERROR : table 'b' has edb definition inconsistent with length of table schema : edb = ['str', '0', 0, 1, 111], table schema = ['string', 'int', 'int', 'int']" )

    # ---------------------------- #
    dbInst.deldb()


  ################
  #  EXAMPLE 17  #
  ################
  # tests incorrect schema input in body
  def test_example17( self ) :

    test_id = "test_example17"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { "id0": [ { 0:1 }, [11,12] ], "0": [ 0, [ [1, 2], 111, 3 ] ] } )
    dbInst.set( "c", { "id1": [ { 0:1 }, [11,12] ], "1": [ 1, [ [1, 2], 222, 3 ] ] } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query1 = "a(Z) :- d(Y,Z), b(_,Y,_,_) ;"
    q.setQuery( query1 )
    logging.debug( "  " + test_id + " : set query '" + query1 + "' to db instance." )

    query2 = "a(Z) :- b(_,_,Y,_), c(_,Y,_,Z) ;"
    q.setQuery( query2 )
    logging.debug( "  " + test_id + " : set query '" + query2 + "' to db instance." )

    query3 = "d(Y,Z) :- c(_,Y,_,Z) ;"
    q.setQuery( query3 )
    logging.debug( "  " + test_id + " : set query '" + query3 + "' to db instance." )

    schema = { "a":["int"], "b":["int","int","int"],"c":["string","int","int","int"], "d":["int","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )

    with self.assertRaises(SystemExit) as cm:
      allProgramData = q.run()
    self.assertEqual( cm.exception.code, "  FORMAT_EDB_STATEMENTS : ERROR : table 'b' has edb definition inconsistent with length of table schema : edb = ['0', 0, 1, 111], table schema = ['int', 'int', 'int']" )

    # ---------------------------- #
    dbInst.deldb()


  ################
  #  EXAMPLE 16  #
  ################
  # tests incorrect schema input in head
  def test_example16( self ) :

    test_id = "test_example16"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { "id0": [ { 0:1 }, [11,12] ], "0": [ 0, [ [1, 2], 111, 3 ] ] } )
    dbInst.set( "c", { "id1": [ { 0:1 }, [11,12] ], "1": [ 1, [ [1, 2], 222, 3 ] ] } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query1 = "a(Z) :- d(Y,Z), b(_,Y,_,_) ;"
    q.setQuery( query1 )
    logging.debug( "  " + test_id + " : set query '" + query1 + "' to db instance." )

    query2 = "a(Z) :- b(_,_,Y,_), c(_,Y,_,Z) ;"
    q.setQuery( query2 )
    logging.debug( "  " + test_id + " : set query '" + query2 + "' to db instance." )

    query3 = "d(Y,Z) :- c(_,Y,_,Z) ;"
    q.setQuery( query3 )
    logging.debug( "  " + test_id + " : set query '" + query3 + "' to db instance." )

    schema = { "a":["int","string"], "b":["string","int","int","int"],"c":["string","int","int","int"], "d":["int","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )

    with self.assertRaises(SystemExit) as cm:
      allProgramData = q.run()
    self.assertEqual(cm.exception.code, "ERROR : table 'a' has inconsitent arities: define_arity = 2, queryList_arity = 1")

    # ---------------------------- #
    dbInst.deldb()


  ################
  #  EXAMPLE 15  #
  ################
  # tests three input queries, including a set of different IDBs
  def test_example15( self ) :

    test_id = "test_example15"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { "id0": [ { 0:1 }, [11,12] ], "0": [ 0, [ [1, 2], 111, 3 ] ] } )
    dbInst.set( "c", { "id1": [ { 0:1 }, [11,12] ], "1": [ 1, [ [1, 2], 222, 3 ] ] } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query1 = "a(Z) :- d(Y,Z), b(_,Y,_,_) ;"
    q.setQuery( query1 )
    logging.debug( "  " + test_id + " : set query '" + query1 + "' to db instance." )

    query2 = "a(Z) :- b(_,_,Y,_), c(_,Y,_,Z) ;"
    q.setQuery( query2 )
    logging.debug( "  " + test_id + " : set query '" + query2 + "' to db instance." )

    query3 = "d(Y,Z) :- c(_,Y,_,Z) ;"
    q.setQuery( query3 )
    logging.debug( "  " + test_id + " : set query '" + query3 + "' to db instance." )

    schema = { "a":["int"], "b":["string","int","int","int"],"c":["string","int","int","int"], "d":["int","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]
    
    expected_program       = ['define(a,{int});', \
                              'define(d,{int, int});', \
                              'define(b,{string, int, int, int});', \
                              'define(c,{string, int, int, int});', \
                              'b("0",0,1,111);', \
                              'b("0",0,2,111);', \
                              'b("0",0,1,3);', \
                              'b("0",0,2,3);', \
                              'b("id0",0,1,11);', \
                              'b("id0",0,1,12);', \
                              'c("1",1,1,222);', \
                              'c("1",1,2,222);', \
                              'c("1",1,1,3);', \
                              'c("1",1,2,3);', \
                              'c("id1",0,1,11);', \
                              'c("id1",0,1,12);', \
                              query1, \
                              query2, \
                              query3 ]
    expected_table_list    = ['a', 'd', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '11', \
                              '12', \
                              '3', \
                              '222', \
                              '---------------------------', \
                              'd', \
                              '1,3', \
                              '1,222', \
                              '0,11', \
                              '0,12', \
                              '---------------------------', \
                              'b', \
                              'id0,0,1,11', \
                              '0,0,1,3', \
                              'id0,0,1,12', \
                              '0,0,2,111', \
                              '0,0,2,3', \
                              '0,0,1,111', \
                              '---------------------------', \
                              'c', \
                              '1,1,1,222', \
                              '1,1,2,3', \
                              'id1,0,1,11', \
                              '1,1,2,222', \
                              'id1,0,1,12', \
                              '1,1,1,3' ]

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ################
  #  EXAMPLE 14  #
  ################
  # tests two input queries for same IDB
  def test_example14( self ) :

    test_id = "test_example14"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { "id0": [ { 0:1 }, [11,12] ], "0": [ 0, [ [1, 2], 111, 3 ] ] } )
    dbInst.set( "c", { "id1": [ { 0:1 }, [11,12] ], "1": [ 1, [ [1, 2], 222, 3 ] ] } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query1 = "a(Z) :- b(_,Y,_,_), c(_,Y,_,Z) ;"
    q.setQuery( query1 )
    logging.debug( "  " + test_id + " : set query '" + query1 + "' to db instance." )

    query2 = "a(Z) :- b(_,_,Y,_), c(_,Y,_,Z) ;"
    q.setQuery( query2 )
    logging.debug( "  " + test_id + " : set query '" + query2 + "' to db instance." )

    schema = { "a":["int"], "b":["string","int","int","int"],"c":["string","int","int","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]
    
    expected_program       = ['define(a,{int});', \
                              'define(b,{string, int, int, int});', \
                              'define(c,{string, int, int, int});', \
                              'b("0",0,1,111);', \
                              'b("0",0,2,111);', \
                              'b("0",0,1,3);', \
                              'b("0",0,2,3);', \
                              'b("id0",0,1,11);', \
                              'b("id0",0,1,12);', \
                              'c("1",1,1,222);', \
                              'c("1",1,2,222);', \
                              'c("1",1,1,3);', \
                              'c("1",1,2,3);', \
                              'c("id1",0,1,11);', \
                              'c("id1",0,1,12);', \
                              query1, \
                              query2 ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '11', \
                              '12', \
                              '3', \
                              '222', \
                              '---------------------------', \
                              'b', \
                              'id0,0,1,11', \
                              '0,0,1,3', \
                              'id0,0,1,12', \
                              '0,0,2,111', \
                              '0,0,2,3', \
                              '0,0,1,111', \
                              '---------------------------', \
                              'c', \
                              '1,1,1,222', \
                              '1,1,2,3', \
                              'id1,0,1,11', \
                              '1,1,2,222', \
                              'id1,0,1,12', \
                              '1,1,1,3' ]

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ################
  #  EXAMPLE 13  #
  ################
  # tests dictionaries with nested arrays, dictionaries and primitives
  def test_example13( self ) :

    test_id = "test_example13"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { "id0": [ { 0:1 }, [11,12] ], "0": [ 0, [ [1, 2], 111, 3 ] ] } )
    dbInst.set( "c", { "id1": [ { 0:1 }, [11,12] ], "1": [ 1, [ [1, 2], 222, 3 ] ] } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query = "a(Z) :- b(_,Y,_,_), c(_,Y,_,Z) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["int"], "b":["string","int","int","int"],"c":["string","int","int","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]
    
    expected_program       = ['define(a,{int});', \
                              'define(b,{string, int, int, int});', \
                              'define(c,{string, int, int, int});', \
                              'b("0",0,1,111);', \
                              'b("0",0,2,111);', \
                              'b("0",0,1,3);', \
                              'b("0",0,2,3);', \
                              'b("id0",0,1,11);', \
                              'b("id0",0,1,12);', \
                              'c("1",1,1,222);', \
                              'c("1",1,2,222);', \
                              'c("1",1,1,3);', \
                              'c("1",1,2,3);', \
                              'c("id1",0,1,11);', \
                              'c("id1",0,1,12);', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '11', \
                              '12', \
                              '---------------------------', \
                              'b', \
                              'id0,0,1,11', \
                              '0,0,1,3', \
                              'id0,0,1,12', \
                              '0,0,2,111', \
                              '0,0,2,3', \
                              '0,0,1,111', \
                              '---------------------------', \
                              'c', \
                              '1,1,1,222', \
                              '1,1,2,3', \
                              'id1,0,1,11', \
                              '1,1,2,222', \
                              'id1,0,1,12', \
                              '1,1,1,3' ]

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ################
  #  EXAMPLE 12  #
  ################
  # tests dictionaries with nested arrays mixed with primitives
  def test_example12( self ) :

    test_id = "test_example12"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { "id0": [ [ 0, 1 ], [11,12] ], "0": [ [1, 2], 3 ] } )
    dbInst.set( "c", { "id1": [ [ 0 ], [11,12,13] ], "1": [ [11, 2], 13 ] } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query = "a(X,Z) :- b(X,Y,_), c(Z,Y,_) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["string","string"], "b":["string","int","int"],"c":["string","int","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]

    expected_program       = ['define(a,{string, string});', \
                              'define(b,{string, int, int});', \
                              'define(c,{string, int, int});', \
                              'b("0",1,3);', \
                              'b("0",2,3);', \
                              'b("id0",0,11);', \
                              'b("id0",1,11);', \
                              'b("id0",0,12);', \
                              'b("id0",1,12);', \
                              'c("1",11,13);', \
                              'c("1",2,13);', \
                              'c("id1",0,11);', \
                              'c("id1",0,12);', \
                              'c("id1",0,13);', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '0,1', \
                              'id0,id1', \
                              '---------------------------', \
                              'b', \
                              '0,2,3', \
                              'id0,0,11', \
                              'id0,0,12', \
                              'id0,1,11', \
                              '0,1,3', \
                              'id0,1,12', \
                              '---------------------------', \
                              'c', \
                              '1,11,13', \
                              'id1,0,13', \
                              '1,2,13', \
                              'id1,0,11', \
                              'id1,0,12' ]


    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ################
  #  EXAMPLE 11  #
  ################
  # tests dictionaries with nested binary arrays
  def test_example11( self ) :

    test_id = "test_example11"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { "id0": [ 0, 1 ], "0":2 } )
    dbInst.set( "c", { "id1": [ 0, 1 ], "1":2 } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query = "a(X) :- b(X,Y), c(_,Y) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["string"], "b":["string","int"],"c":["string","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]

    expected_program       = ['define(a,{string});', \
                              'define(b,{string, int});', \
                              'define(c,{string, int});', \
                              'b("0",2);', \
                              'b("id0",0);', \
                              'b("id0",1);', \
                              'c("1",2);', \
                              'c("id1",0);', \
                              'c("id1",1);', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '0', \
                              'id0', \
                              '---------------------------', \
                              'b', \
                              'id0,0', \
                              '0,2', \
                              'id0,1', \
                              '---------------------------', \
                              'c', \
                              'id1,1', \
                              '1,2', \
                              'id1,0']

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ################
  #  EXAMPLE 10  #
  ################
  # tests binary dictionaries with nested arrays and primitives
  def test_example10( self ) :

    test_id = "test_example10"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { "id0": [ 0 ], "0":10 } )
    dbInst.set( "c", { 0: [ 1 ], 1:11 } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query = "a(Z) :- b(X,Y), c(Y,Z) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["int"], "b":["string","int"],"c":["int","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]

    expected_program       = ['define(a,{int});', \
                              'define(b,{string, int});', \
                              'define(c,{int, int});', \
                              'b("0",10);', \
                              'b("id0",0);', \
                              'c(0,1);', \
                              'c(1,11);', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '1', \
                              '---------------------------', \
                              'b', \
                              'id0,0', \
                              '0,10', \
                              '---------------------------', \
                              'c', \
                              '1,11', \
                              '0,1']

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ###############
  #  EXAMPLE 9  #
  ###############
  # tests unary dictionaries mapping to arrays of primitives
  def test_example9( self ) :

    test_id = "test_example9"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { "id0": [ "id1" ] } )
    dbInst.set( "c", { "id1": [ 1 ] } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query = "a(Z) :- b(X,Y), c(Y,Z) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["int"], "b":["string","string"],"c":["string","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]

    expected_program       = ['define(a,{int});', \
                              'define(b,{string, string});', \
                              'define(c,{string, int});', \
                              'b("id0","id1");', \
                              'c("id1",1);', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '1', \
                              '---------------------------', \
                              'b', \
                              'id0,id1', \
                              '---------------------------', \
                              'c', \
                              'id1,1']

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ###############
  #  EXAMPLE 8  #
  ###############
  # tests unary dictionaries mapping to primitives
  def test_example8( self ) :

    test_id = "test_example8"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { "id0": "id1" } )
    dbInst.set( "c", { "id1": 1 } )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query = "a(Z) :- b(X,Y), c(Y,Z) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["int"], "b":["string","string"],"c":["string","int"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]

    expected_program       = ['define(a,{int});', \
                              'define(b,{string, string});', \
                              'define(c,{string, int});', \
                              'b("id0","id1");', \
                              'c("id1",1);', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '1', \
                              '---------------------------', \
                              'b', \
                              'id0,id1', \
                              '---------------------------', \
                              'c', \
                              'id1,1']

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ###############
  #  EXAMPLE 7  #
  ###############
  # test multiple nested arrays mixed with strings
  def test_example7( self ) :

    test_id = "test_example7"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + " : initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", [ ["strA","strAA"], "thing1", [["wazzup?","str10"],"str11"] ] )
    dbInst.set( "c", [ ["strB","strA"], "thing1", [["wazzup?","str20"],"str21"] ] )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  test_example7 : instantiated Quest instance '" + str( q ) )

    query = "a(X,Y) :- b(X,Y,_,_), c(X,Y,_,_) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["string","string"], "b":["string","string","string","string"],"c":["string","string","string","string"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]

    expected_program       = ['define(a,{string, string});', \
                              'define(b,{string, string, string, string});', \
                              'define(c,{string, string, string, string});', \
                              'b("strA","thing1","wazzup?","str11");', \
                              'b("strAA","thing1","wazzup?","str11");', \
                              'b("strA","thing1","str10","str11");', \
                              'b("strAA","thing1","str10","str11");', \
                              'c("strB","thing1","wazzup?","str21");', \
                              'c("strA","thing1","wazzup?","str21");', \
                              'c("strB","thing1","str20","str21");', \
                              'c("strA","thing1","str20","str21");', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              'strA,thing1', \
                              '---------------------------', \
                              'b', \
                              'strA,thing1,str10,str11', \
                              'strA,thing1,wazzup?,str11', \
                              'strAA,thing1,str10,str11', \
                              'strAA,thing1,wazzup?,str11', \
                              '---------------------------', \
                              'c', \
                              'strB,thing1,wazzup?,str21', \
                              'strA,thing1,wazzup?,str21', \
                              'strA,thing1,str20,str21', \
                              'strB,thing1,str20,str21']

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ###############
  #  EXAMPLE 6  #
  ###############
  # tests nested arrays with only arrays of strings
  def test_example6( self ) :

    test_id = "test_example6"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + " : initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", [ ["strA","strAA"], ["strB","str11"] ] )
    dbInst.set( "c", [ ["strB","strBB"], ["strA","str21"] ] )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query = "a(X) :- b(X,Y), c(Y,X) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["string"], "b":["string","string"],"c":["string","string"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]


    expected_program       = ['define(a,{string});', \
                              'define(b,{string, string});', \
                              'define(c,{string, string});', \
                              'b("strA","strB");', \
                              'b("strAA","strB");', \
                              'b("strA","str11");', \
                              'b("strAA","str11");', \
                              'c("strB","strA");', \
                              'c("strBB","strA");', \
                              'c("strB","str21");', \
                              'c("strBB","str21");', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              'strA', \
                              '---------------------------', \
                              'b', \
                              'strA,strB', \
                              'strAA,strB', \
                              'strAA,str11', \
                              'strA,str11', \
                              '---------------------------', \
                              'c', \
                              'strB,strA', \
                              'strBB,strA', \
                              'strB,str21', \
                              'strBB,str21' ]

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ###############
  #  EXAMPLE 5  #
  ###############
  # tests nested arrays of strings with nesting in first column
  def test_example5( self ) :

    test_id = "test_example5"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", [ ["strA","strAA"], "strB" ] )
    dbInst.set( "c", [ ["strB","strBB"], "strA" ] )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query = "a(X) :- b(X,Y), c(Y,X) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["string"], "b":["string","string"],"c":["string","string"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]


    expected_program       = ['define(a,{string});', \
                              'define(b,{string, string});', \
                              'define(c,{string, string});', \
                              'b("strA","strB");', \
                              'b("strAA","strB");', \
                              'c("strB","strA");', \
                              'c("strBB","strA");', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              'strA', \
                              '---------------------------', \
                              'b', \
                              'strA,strB', \
                              'strAA,strB', \
                              '---------------------------', \
                              'c', \
                              'strB,strA', \
                              'strBB,strA']

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ###############
  #  EXAMPLE 4  #
  ###############
  # tests nested arrays of strings
  def test_example4( self ) :

    test_id = "test_example4"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", [ "strA", ["str10","strB"] ] )
    dbInst.set( "c", [ "strB", ["strA","str21"] ] )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query = "a(X) :- b(X,Y), c(Y,X) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["string"], "b":["string","string"],"c":["string","string"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]


    expected_program       = ['define(a,{string});', \
                              'define(b,{string, string});', \
                              'define(c,{string, string});', \
                              'b("strA","str10");', \
                              'b("strA","strB");', \
                              'c("strB","strA");', \
                              'c("strB","str21");', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              'strA', \
                              '---------------------------', \
                              'b', \
                              'strA,strB', \
                              'strA,str10', \
                              '---------------------------', \
                              'c', \
                              'strB,strA', \
                              'strB,str21']

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ###############
  #  EXAMPLE 3  #
  ###############
  # tests arrays of multiple strings
  def test_example3( self ) :

    test_id = "test_example3"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", ["str_10","str_11"] )
    dbInst.set( "c", ["str_11","str21"] )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query = "a(X) :- b(X), c(X) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["string"], "b":["string"],"c":["string"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]


    expected_program       = ['define(a,{string});', \
                              'define(b,{string});', \
                              'define(c,{string});', \
                              'b("str_10");', \
                              'b("str_11");', \
                              'c("str_11");', \
                              'c("str21");', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              'str_11', \
                              '---------------------------', \
                              'b', \
                              'str_10', \
                              'str_11', \
                              '---------------------------', \
                              'c', \
                              'str_11', \
                              'str21']

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ###############
  #  EXAMPLE 2  #
  ###############
  # tests arrays of single strings
  def test_example2( self ) :

    test_id = "test_example2"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", ["str10"] )
    dbInst.set( "c", ["str10"] )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query = "a(X) :- b(X), c(X) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["string"], "b":["string"],"c":["string"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]


    expected_program       = ['define(a,{string});', \
                              'define(b,{string});', \
                              'define(c,{string});', \
                              'b("str10");', \
                              'c("str10");', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              'str10', \
                              '---------------------------', \
                              'b', \
                              'str10', \
                              '---------------------------', \
                              'c', \
                              'str10']

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


  ###############
  #  EXAMPLE 1  #
  ###############
  # tests single strings
  def test_example1( self ) :

    test_id = "test_example1"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_quest.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", "str10" )
    dbInst.set( "c", "str10" )

    # --------------------------------------------------------------- #
    q = Quest.Quest( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated Quest instance '" + str( q ) )

    query = "a(X) :- b(X), c(X) ;"
    q.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["string"], "b":["string"],"c":["string"] }

    for rel in schema :
      q.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    logging.debug( "  " + test_id + " : calling 'run' on Quest instance." )
    allProgramData = q.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]


    expected_program       = ['define(a,{string});', \
                              'define(b,{string});', \
                              'define(c,{string});', \
                              'b("str10");', \
                              'c("str10");', \
                              query ]
    expected_table_list    = ['a', 'b', 'c']
    expected_results_array = ['---------------------------', \
                              'a', \
                              'str10', \
                              '---------------------------', \
                              'b', \
                              'str10', \
                              '---------------------------', \
                              'c', \
                              'str10']

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # ---------------------------- #
    dbInst.deldb()


#########
#  EOF  #
#########
