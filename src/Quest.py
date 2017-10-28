#/usr/bin/env python

# based on https://github.com/KDahlgren/pyLDFI/blob/master/src/wrappers/c4/C4Wrapper.py

##########################################################################
# Quest usage notes:
#
# 1. Quest does not yet support schema derivation on arbitrary queries.
#    Users must provide the schemas of IDBs/EDBs as input.
#    Ex: for a query a(X):-b(X,Y),C(Y,X),
#        suppose a is a unary relation of strings and b is a binary
#        relation pairing strings and ints and c is a binary relation
#        pairing ints and strings.
#        Then the user must also supply the appropriate schema
#        for Quest to work properly :
#        { "a":["string"], "b":["string","int"], "c":["int","string"] }
#
##########################################################################

#############
#  IMPORTS  #
#############
# standard python packages
import copy, logging, math, os, pickledb, string, sys, unittest
import C4Wrapper

# ------------------------------------------------------ #

# import sibling packages HERE!!!
adaptersPath  = os.path.abspath( __file__ + "/../../../../adapters" )
if not adaptersPath in sys.path :
  sys.path.append( adaptersPath )
import Adapter

# settings dir
settingsPath  = os.path.abspath( __file__ + "/../../core" )
if not settingsPath in sys.path :
  sys.path.append( settingsPath )
import settings

# ------------------------------------------------------ #

DEBUG = settings.DEBUG

class Quest( object ) :

  ################
  #  ATTRIBUTES  #
  ################
  nosql_type = None   # the type of nosql database under consideration
  dbcursor   = None   # pointer to target database instance

  queryList  = []     # list of query strings
  schema     = {}     # dictionary mapping each EDB and IDB relation name 
                      # to an array listing the datatypes for each attribute.

  ##########
  #  INIT  #
  ##########
  def __init__( self, nosql_type, dbcursor ) :
    self.dbcursor   = dbcursor
    self.nosql_type = nosql_type


  ################
  #  DESTRUCTOR  #
  ################
  def destructor( self ) :
    self.nosql_type = None   # the type of nosql database under consideration
    self.dbcursor.deldb()    # pointer to target database instance

    self.queryList  = []     # list of query strings
    self.schema     = {}     # dictionary mapping each EDB and IDB relation name 
                             # to an array listing the datatypes for each attribute.

  #########
  #  RUN  #
  #########
  # execute the queries over the target database and 
  # use c4 to complete the evaluation process.
  def run( self ) :

    # --------------------------------------- #
    # get the table list
    table_list = self.getTableList()

    # --------------------------------------- #
    # get define statements
    c4_define_statements = self.getDefineStatements( table_list )

    # --------------------------------------- #
    # get EDBs
    c4_edb_statements = self.getEDBs( table_list )

    # --------------------------------------- #
    # sanity checks
    self.verifyArities( c4_define_statements, c4_edb_statements, self.queryList, table_list )
    self.verifyDataTypes( c4_define_statements, c4_edb_statements, self.queryList, table_list )

    # --------------------------------------- #
    # format results
    formatted_statements = c4_define_statements + c4_edb_statements + self.queryList

    # --------------------------------------- #
    # run c4 program evaluation
    w             = C4Wrapper.C4Wrapper( ) # initializes c4 wrapper instance
    results_array = w.run( [ formatted_statements, table_list ] )

    # --------------------------------------- #
    logging.debug( "  RUN : formatted_statements = " + str( formatted_statements ) )
    logging.debug( "  RUN : table_list           = " + str( table_list ) )
    logging.debug( "  RUN : results_array        = " + str( results_array ) )

    return [ formatted_statements, table_list, results_array ]


  #######################
  #  VERIFY DATA TYPES  #
  #######################
  # make sure the relation primitive data types match across relevant 
  # define, edb, and sub/goal statements.
  # supported primitive data types are : strings, ints, floats, bools
  def verifyDataTypes( self, c4_define_statements, c4_edb_statements, queryList, table_list ) :
    return None


  ####################
  #  VERIFY ARITIES  #
  ####################
  # make sure the relation arities match across relevant 
  # define, edb, and sub/goal statements.
  def verifyArities( self, c4_define_statements, c4_edb_statements, queryList, table_list  ) :

    #print "c4_define_statements : " + str( c4_define_statements )
    #print "c4_edb_statements    : " + str( c4_edb_statements )
    #print "queryList            : " + str( queryList )
    #print "table_list           : " + str( table_list )

    for table in table_list :
      define_arity    = self.getDefineArity( c4_define_statements, table )
      edb_arity       = self.getEDBArity( c4_edb_statements, table )
      queryList_arity = self.getQueryListArity( queryList, table )

      #print ">> table = " + table,
      #print "define_arity    : " + str( define_arity )
      #print "edb_arity       : " + str( edb_arity )
      #print "queryList_arity : " + str( queryList_arity )

      # make sure every table has a define statement and appears in a query
      if define_arity < 0 :
        sys.exit( "ERROR : table '" + str( table ) + "' has no define statement. aborting..." )
      elif edb_arity < 0 and queryList_arity < 0 :
        sys.exit( "ERROR : table '" + str( table ) + "' does not appear in any query or edb fact. aborting..." )

      # make sure define_arity agrees with edb_arity
      if edb_arity > 0 and not define_arity == edb_arity :
        sys.exit( "ERROR : table '" + str( table ) + "' has inconsitent arities: define_arity = " + str( define_arity ) + ", edb_arity = " + str( edb_arity ) )

      # make sure define_arity agrees with queryList_arities
      if queryList_arity > 0 and not define_arity == queryList_arity :
        sys.exit( "ERROR : table '" + str( table ) + "' has inconsitent arities: define_arity = " + str( define_arity ) + ", queryList_arity = " + str( queryList_arity ) )

      # make sure edb_arities agree with queryList_arities
      if edb_arity > 0 and queryList_arity > 0 and not queryList_arity == edb_arity :
        sys.exit( "ERROR : table '" + str( table ) + "' has inconsitent arities: define_arity = " + str( define_arity ) + ", queryList_arity = " + str( queryList_arity ) )


  ##########################
  #  GET QUERY LIST ARITY  #
  ##########################
  # grab the arities of the given table across all relevant query statements
  # make sure the arities are identical
  # return the arity as an integer
  def getQueryListArity( self, queryList, table ) :

    #print "==========================="
    #print "queryList = " + str( queryList )
    #print "table     = " + str( table )

    arity     = -1
    arityList = []

    for q in queryList :

      searchStr = table + "("

      if searchStr in q :

        q    = q.translate( None, string.whitespace )
        q    = q.split( ":-" )
        goal = q[0]
        body = q[1]

        #print "------------------------"
        #print "searchStr = " + str( searchStr )
        #print "q         = " + str( q )
        #print "goal      = " + str( goal )
        #print "body      = " + str( body )

        # check query goal for table instances
        if searchStr in goal :

          openParenIndex = goal.index( "(" )
          closParenIndex = goal.index( ")" )

          contents = goal[ openParenIndex+1 : closParenIndex-1 ]
          contents = contents.split( "," )

          arity    = len( contents )
          arityList.append( arity )

        # check query body for table instances
        # this is an 'if' condition because the goal could appear in the body
        # in recursive queries.
        if searchStr in body :

          for i in range( 0, len( body ) ) :

            startContentsIndex = body.find( searchStr, i )

            if startContentsIndex > 0 :
              startContentsIndex = startContentsIndex + len( searchStr ) - 1 # first index of contents
              endContentsIndex   = body.find( ")", startContentsIndex )
              contents           = body[ startContentsIndex : endContentsIndex ]
              contents           = contents.split( "," )

              arity              = len( contents )
              arityList.append( arity )
              currStartIndex = endContentsIndex

    # make sure all arities are identical
    if len( arityList ) > 0 :
      if all( a == arityList[0] for a in arityList ) :
        arity = arityList[0]
      else :
        sys.exit( "ERROR : edb definitions for table '" + str( table ) + "' have inconsistent arities : " + str( arityList ) )

    # make sure arity is an integer
    if not arity == math.floor( arity ) :
      sys.exit( "ERROR : table '" + str( table ) + "' has arity " + str( arity ) + ". relation arities must be positive integers." )

    return arity


  ###################
  #  GET EDB ARITY  #
  ###################
  # grab the arities of the given table across all relevant edb statements
  # make sure the arities are identical
  # return the arity as an integer
  # a negative integer indicates table is IDB
  def getEDBArity( self, c4_edb_statements, table ) :

    arity = -1
    arityList = []
    for stmt in c4_edb_statements :
      stmt = stmt.replace( ";", "" )
      stmt = stmt.split( "(" )
      relationName = stmt[0]

      if relationName == table :
        dataList = stmt[1]
        dataList = dataList.replace( ")", "" )
        dataList = dataList.split( "," )
        arityList.append( len( dataList ) )

    # make sure all arities are identical
    if len( arityList ) > 0 :
      if all( a == arityList[0] for a in arityList ) :
        arity = arityList[0]
      else :
        sys.exit( "ERROR : edb definitions for table '" + str( table ) + "' have inconsistent arities : " + str( arityList ) )

    # make sure arity is an integer
    if not arity == math.floor( arity ) :
      sys.exit( "ERROR : table '" + str( table ) + "' has arity " + str( arity ) + ". relation arities must be positive integers." )

    return arity


  ########################
  #  GET DEFINE ARITIES  #
  ########################
  # given a list of c4 define statements and a table.
  # get the define statement for the table.
  # extract the list of data types.
  # return the arity of the list.
  def getDefineArity( self, c4_define_statements, table ) :

    arity = 0

    for stmt in c4_define_statements :
      stmt = stmt.replace( "define(","" )
      stmt = stmt.replace( ");","" )
      stmt = stmt.split( ",{" )
      relationName = stmt[0]
      typeList = stmt[1]
      typeList = typeList.replace( "}", "" )
      typeList = typeList.split( "," )

      #print relationName,
      #print "typeList : " + str( typeList )

      if relationName == table :
        arity = len( typeList )
        break

    # make sure arity is greater than zero and integer
    if arity <= 0 or not arity == math.floor( arity ) :
      sys.exit( "ERROR : table '" + str( table ) + "' has arity " + str( arity ) + ". relation arities must be positive integers." )

    return arity


  ##############
  #  GET EDBS  #
  ##############
  # grab EDB data from the target database instance
  def getEDBs( self, table_list ) :

    #print "table_list : " + str( table_list )

    c4_edb_statements = []

    ad = Adapter.Adapter( self.nosql_type )

    for relationName in table_list :
      try :
        raw_value    = ad.get( relationName, self.dbcursor )
        relationData = self.getRelationData( relationName, raw_value )

        logging.debug( "  GETEDBS : submitting relationName '" + str( relationName ) + "' and relationData '" + str( relationData ) + "' to format_ebd_statements" )

        c4_edb_statements.extend( self.format_edb_statements( relationName, relationData ) )
      except SystemExit :
        raise
      except :
        pass

    #sys.exit( "c4_edb_statements : " + str( c4_edb_statements ) )
    return c4_edb_statements


  ###########################
  #  FORMAT EDB STATEMENTS  #
  ###########################
  # build c4 edb statements per relation name, given all data for that relation.
  def format_edb_statements( self, relationName, relationData ) :

    logging.debug( "  FORMAT_EDB_STATEMENTS : relation name is '" + str( relationName ) + "'"  )
    logging.debug( "  FORMAT_EDB_STATEMENTS : relationData  is '" + str( relationData ) + "'"  )

    statements = []
    for row in relationData :

      currStatement = []
      currStatement.append( relationName + "(" )

      logging.debug( "  FORMAT_EDB_STATEMENTS : self.schema['" + str( relationName ) + "'] = " + str( self.schema[ relationName ] ) )
      logging.debug( "  FORMAT_EDB_STATEMENTS : '" + str( relationName ) + "' data : " + str( row ) )

      # make sure schema aligns with number of data items in table rows
      if not len( self.schema[ relationName ] ) == len( row ) :
        sys.exit( "  FORMAT_EDB_STATEMENTS : ERROR : table '" + str( relationName ) + "' has edb definition inconsistent with length of table schema : edb = " + str( row ) + ", table schema = " + str( self.schema[ relationName ] ) )

      for i in range(0,len(row)) :

        # last value
        if i == len(row) - 1 :
          if self.schema[ relationName ][i] == "string" : # string values need quotes
            currStatement.append( '"' +  row[i] + '");' )
          else :
            currStatement.append( str( row[i] ) + ");" )

        # middle value
        else :
          if self.schema[ relationName ][i] == "string" : # string values need quotes
            currStatement.append( '"' +  row[i] + '",' )
          else :
            currStatement.append( str( row[i] ) + "," )

      statements.append( "".join( currStatement ) )

    logging.debug( "  FORMAT_EDB_STATEMENTS : statements = " + str( statements ) )
    return statements


  #######################
  #  GET RELATION DATA  #
  #######################
  # input relation name and value
  # observe relation value could be a string, an array of strings, arrays, or dictionarys, or a dictionary
  # of strings, arrays, or dictionaries.
  # other objects are not currently supported.
  # return an array of arrays. each inner array represents a tuple of data derived from the raw value.
  def getRelationData( self, relationName, raw_value ) :

    logging.debug( "----------------------------------------------------" )
    logging.debug( "  GETRELATIONDATA : relationName : " + str( relationName ) )
    logging.debug( "  GETRELATIONDATA : raw_value    : " + str( raw_value ) )

    # ---------------------------------------------------------- #
    # PRE PROCESS raw_value -> CONVERT STRINGS AND INTS TO ARRAYS

    if type( raw_value ) is str or type( raw_value ) is int :
      raw_value = [ raw_value ]

    # ---------------------------------------------------------- #
    # BASE CASE : empty raw_value
    if len( raw_value ) < 1 :
      logging.debug( "  GETRELATIONDATA : returning relationData as " + str( [] ) )
      return []

    # ---------------------------------------------------------- #
    # BASE CASE : list of primatives
    elif self.isPrimativeList( raw_value ) :
      relationData = [ [ x ] for x in raw_value  ] # transform data into tuples (i.e. unary arrays)
      logging.debug( "  GETRELATIONDATA : returning relationData as " + str( relationData ) )
      return relationData

    # ---------------------------------------------------------- #
    # BASE CASE : dictionary of primatives (both keys and valus are primatives)
    elif self.isPrimativeDict( raw_value ) :

      relationData = []
      for key in raw_value :
        currTup = []

        if type( raw_value[ key ] ) is list :
          for val in raw_value[ key ] :
            relationData.append( [ key, val ] )

        else :
          currTup = [ key, raw_value[key] ]
          relationData.append( currTup )

      logging.debug( "  GETRELATIONDATA : returning relationData as " + str( relationData ) )
      return relationData


    # ---------------------------------------------------------- #
    # RECURSIVE CASE : still stuff to process in raw_value
    elif self.isUnaryDict( raw_value ) :
      key = raw_value.keys()
      key = key[0]
      return self.getRelationData( relationName, [key] + raw_value[key] )

    # ---------------------------------------------------------- #
    # RECURSIVE CASE : still stuff to process in raw_value
    else :

      ##########################################
      # CASE : raw_value is a list
      if type( raw_value ) is list :

        # set left_subRelation
        logging.debug( "  GETRELATIONDATA : launching left_subRelation on " + str( raw_value[0] ) )
        left_subRelation = self.getRelationData( relationName, raw_value[0] )

        # set right_subRelation
        try :
          logging.debug( "  GETRELATIONDATA : launching right_subRelation on " + str( raw_value[1:] ) )
          right_subRelation = self.getRelationData( relationName, raw_value[1:] )
        except IndexError :
          right_subrelation = []

        # merge sub relations
        logging.debug( "  GETRELATIONDATA : left_subRelation  is " + str( left_subRelation ) )
        logging.debug( "  GETRELATIONDATA : right_subRelation is " + str( right_subRelation ) )
  
        if left_subRelation == [] and not right_subRelation == [] :
          relationData = right_subRelation
  
        elif not left_subRelation == [] and right_subRelation == [] :
          relationData = left_subRelation
  
        else :
          relationData = []
          row          = []
          for row1 in right_subRelation :
            for row2 in left_subRelation :
              row.extend( row2 )
              row.extend( row1 )
              relationData.append( row )
              row = []
              logging.debug( "  GETRELATIONDATA : added " + str( row ) + " to relationData" )
  
        logging.debug( "  GETRELATIONDATA : returning relationData as " + str( relationData ) )
        return relationData

      ##########################################
      # CASE : raw_value is a dict
      elif type( raw_value ) is dict :

        # set left_subRelation
        allKeys = raw_value.keys() # should never be empty
        logging.debug( "  GETRELATIONDATA : launching left_subRelation on " + str( { allKeys[0] : raw_value[allKeys[0]] } ) )
        left_subRelation = self.getRelationData( relationName, { allKeys[0] : raw_value.pop( allKeys[0] ) } )

        # set right_subRelation
        try :
          logging.debug( "  GETRELATIONDATA : launching right_subRelation on " + str( raw_value ) )
          right_subRelation = self.getRelationData( relationName, raw_value )
        except IndexError :
          right_subrelation = []

        # merge sub relations
        logging.debug( "  GETRELATIONDATA : left_subRelation  is " + str( left_subRelation ) )
        logging.debug( "  GETRELATIONDATA : right_subRelation is " + str( right_subRelation ) )

        if left_subRelation == [] and not right_subRelation == [] :
          relationData = right_subRelation

        elif not left_subRelation == [] and right_subRelation == [] :
          relationData = left_subRelation

        else :
          relationData = left_subRelation + right_subRelation

        logging.debug( "  GETRELATIONDATA : returning relationData as " + str( relationData ) )
        return relationData

      ##########################################


  ###################
  #  IS UNARY DICT  #
  ###################
  # check if dictionary only contains one kv pair
  def isUnaryDict( self, raw_value ) :

    if type( raw_value ) is dict and len(raw_value) == 1 :
      return True

    else :
      return False


  ###########################
  #  CONTAINS UNRECOGNIZED  #
  ###########################
  # check if the structure contains unrecognized types
  # structure will be a list or a dictionary
  # return boolean
  def containsUnrecognized( self, raw_value ) :

    logging.debug( "  CONTAINSUNRECGONIZED : checking raw_value for unrecognized types : " + str( raw_value ) )

    assert( type( raw_value ) is list or type( raw_value ) is dict )

    flag = False

    # -------------------------------------------- #
    # LIST
    if type( raw_value ) is list :

      logging.debug( "  CONTAINSUNRECGONIZED : raw_value is a list" )

      for item in raw_value :
        for d in raw_value :
          if type( d ) is str or type( d ) is int or type( d ) is float or type( d ) is bool :
            pass
          else :
            sys.exit( "ERROR : raw_value '" + str( raw_value ) + " contains unrecognized type " + str(type(d)) + " for element '" + d + "'" )

    # -------------------------------------------- #
    # DICTIONARY
    elif type( raw_value ) is dict :

      logging.debug( "  CONTAINSUNRECGONIZED : raw_value is a dict" )

      for key in sorted( raw_value.iterkeys() ):

        # check key types
        if type( key ) is str or type( key ) is int or type( key ) is float or type( key ) is bool :
          pass
        else :
          sys.exit( "ERROR : raw_value '" + str( raw_value ) + " contains unrecognized type " + str(type(d)) + " for element '" + d + "'" )

        # check value types
        if type( raw_value[key] ) is str or type( raw_value[key] ) is int or type( raw_value[key] ) is float or type( raw_value[key] ) is bool :
          pass
        else :
          sys.exit( "ERROR : raw_value '" + str( raw_value ) + " contains unrecognized type " + str(type(d)) + " for element '" + d + "'" )

    return flag


  #######################
  #  IS PRIMATIVE LIST  #
  #######################
  # check if the list contains only primative types
  # return boolean
  def isPrimativeList( self, raw_value ) :

    if type( raw_value ) is list :

      flag = True
      for d in raw_value :
        if type( d ) is str or type( d ) is int or type( d ) is float or type( d ) is bool :
          pass
        else :
          flag = False

      return flag

    else :
      return False


  #######################
  #  IS PRIMATIVE DICT  #
  #######################
  # check if the dict contains only primative types
  # return boolean
  def isPrimativeDict( self, raw_value ) :

    if type( raw_value ) is dict :

      flag = True
      for key in raw_value :

        # ------------------------------------- #
        # check key types
        if type( key ) is str or type( key ) is int or type( key ) is float or type( key ) is bool :
          pass
        else :
          flag = False

        # ------------------------------------- #
        # check value types
        if self.isPrimativeList( raw_value[ key ] ) :
          pass

        elif type( raw_value[key] ) is str or type( raw_value[key] ) is int or type( raw_value[key] ) is float or type( raw_value[key] ) is bool :
          pass

        else :
          flag = False

      return flag

    else :
      return False


  ###########################
  #  GET DEFINE STATEMENTS  #
  ###########################
  def getDefineStatements( self, table_list ) :

    #print "schema     : " + str( self.schema )
    #print "table_list : " + str( table_list )

    c4_define_statements = []

    for relationName in table_list :

      relSchema = None

      try :
        relSchema = self.schema[ relationName ]

      except :
        sys.exit( "ERROR : getDefineStatements : schema does not support relation name '" + str( relationName ) + "'. aborting..." )

      if relSchema :
        relSchema = str( relSchema )
        relSchema = relSchema.replace( "'", "" )
        relSchema = relSchema.replace( '"', '' )
        relSchema = relSchema.replace( "[", "{" )
        relSchema = relSchema.replace( "]", "}" )
        statement = "define(" + relationName + "," + relSchema + ");"
        c4_define_statements.append( statement )

    #sys.exit( "c4_define_statements : " + str( c4_define_statements ) )
    return c4_define_statements


  ###############
  #  SET QUERY  #
  ###############
  # add a query to the query list
  def setQuery( self, queryStr ) :
    self.queryList.append( queryStr )


  ################
  #  SET SCHEMA  #
  ################
  # define schema for input relation
  def setSchema( self, relationName, typeList ) :
    self.schema[ relationName ] = typeList


  ################
  #  GET SCHEMA  #
  ################
  # given a relation name, grab the associated schema info, if it exists
  def getSchema( self, relationName ) :

    try :
      return self.schema[ relationName ]
    except KeyError :
      return "KeyError : relation name '" + str( relationName ) + "' has no saved schema."


  ####################
  #  GET TABLE LIST  #
  ####################
  # examine all queries and grab all edb and idb tables.
  def getTableList( self ) :

    table_list = []

    for q in self.queryList :

      q = q.replace( "notin", "" )

      # remove whitespace
      q = q.translate( None, string.whitespace )

      # get the tables in this query
      tables = self.getTables( q )

      for t in tables :
        if not t in table_list :
          table_list.append( t )

    #sys.exit( "table_list : " + str( table_list ) )
    return table_list


  ################
  #  GET TABLES  #
  ################
  # get all tables referenced in a query statment.
  def getTables( self, queryLine ) :

    # initialize return structure to empty
    tables = []

    # remove all whitespace
    queryLine = queryLine.translate( None, string.whitespace )

    # create array on open paren
    queryLine = queryLine.split( "(" )

    # examine each component for a valid table name
    for component in queryLine :

      for i in range( 0, len( component ) ) :

        c = component[i]

        if self.isOp( c ) or self.isComma( c ) :

          try :
            remainingStr = component[i+1:]
            if self.hasSymbols( remainingStr ) :
              pass
            else :
              tables.append( component[i+1:] )
              break

          except :
            break

        elif i == len(component) - 1 and not self.hasSymbols( component ) :
          tables.append( component )

        else :
          pass

    #print "GET TABLES"
    #print "queryLine : " + str( queryLine )
    #print "tables    : " + str( tables )
    #sys.exit( "GET TABLES" )

    return tables


  ###########
  #  IS OP  #
  ###########
  # check if input character is the "-" in ":-"
  def isOp( self, c ) :
    if c == "-" :
      return True
    else :
      return False


  ##############
  #  IS COMMA  #
  ##############
  # check if input character is a comma
  def isComma( self, c ) :
    if c == "," :
      return True
    else :
      return False


  #################
  #  HAS SYMBOLS  #
  #################
  # check if input string contains any of the query language symbols.
  def hasSymbols( self, cstr ) :

    symbols = [ ":-", "(", ")", ",", ";" ]

    for s in symbols :
      if s in cstr :
        return True

    return False


#########
#  EOF  #
#########
