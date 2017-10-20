#/usr/bin/env python

# based on https://github.com/KDahlgren/pyLDFI/blob/master/src/wrappers/c4/C4Wrapper.py

#############
#  IMPORTS  #
#############
# standard python packages
import inspect, os, string, sys, time
from ctypes import *
from types  import *

# ------------------------------------------------------ #
# ------------------------------------------------------ #

class C4Wrapper( object ) :


  ##########
  #  INIT  #
  ##########
  def __init__( self ) :

    c4_lib_loc                     = os.path.abspath( __file__ + '/../../../lib/c4/build/src/libc4/libc4.dylib' )
    self.lib                       = cdll.LoadLibrary( c4_lib_loc )
    self.lib.c4_make.restype       = POINTER(c_char)
    self.lib.c4_dump_table.restype = c_char_p   # c4_dump_table returns a char*


  #########
  #  RUN  #
  #########
  # fullprog is a string of concatenated overlog commands.
  def run( self, allProgramData ) :

    allProgramLines = allProgramData[0] # := list of every code line in the generated C4 program.
    tableList       = allProgramData[1] # := list of all tables in generated C4 program.

    completeProg    = "".join( allProgramLines )

    # ----------------------------------------- #
    print "PRINTING LEGIBLE INPUT PROG"
    for line in fullprog :
      line = line.split( ";" )
      for statement in line :
        statement = statement.rstrip()
        if not statement == "" :
          statement = statement + ";"
          print statement

    # ----------------------------------------- #
    # initialize c4 instance
    self.lib.c4_initialize()
    self.c4_obj = self.lib.c4_make( None, 0 )

    # ---------------------------------------- #
    # load program
    print "SUBMITTING SUBPROG : "
    c_prog = bytes( completeProg )
    self.lib.c4_install_str( self.c4_obj, c_prog )

    # ---------------------------------------- #
    # dump program results to file
    results_array = self.saveC4Results_toArray( tableList )

    # ---------------------------------------- #
    # close c4 program
    self.lib.c4_destroy( self.c4_obj )
    self.lib.c4_terminate( )

    # ---------------------------------------- #
    return results_array


  ##############################
  #  SAVE C4 RESULTS TO ARRAY  #
  ##############################
  # save c4 results to array
  def saveC4Results_toArray( self, tableList ) :

    # save new contents
    results_array = []

    for table in tableList :

      # output to stdout
      if DEBUG :
        print "---------------------------"
        print table
        print self.lib.c4_dump_table( self.c4_obj, table )

      # save in array
      results_array.append( "---------------------------" )
      results_array.append( table )

      table_results_str   = self.lib.c4_dump_table( self.c4_obj, table )
      table_results_array = table_results_str.split( '\n' )
      results_array.extend( table_results_array[:-1] ) # don't add the last empty space

    return results_array


#########
#  EOF  #
#########
