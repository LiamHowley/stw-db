STW-DB is a context oriented Object Relational Model, that uses CONTEXTL and CL-POSTGRES to dynamically generate environmentally constrained stored procedures and query functions. Procedure/function generation is cached, and the appropriate procedure to be called is determined by the presence of slot values in the queried object. This is a beta version. The API is subject to change.


# To Load

Use quickload to call (ql:quickload :stw-db). If not found in the quickload repositories, add to your local-projects directory.


# Connecting to a database

Connections are made by passing an environment variable to the DB-CONNECT macro, along with a layered context. DB-CONNECT provides a  database connection `*DB*` that can be captured and called by database querying functions. Defining an environment variable is relatively straightforward: 

`(define-db-environment db
    (connection-params #P "<DATABASE PARAMETER FILE>"))

(db-connect db (db-layer)
	    <-- do something here -->)`
	    

## Database parameters file

The database parameter file must contain the name of the database, the USER, PASSWORD and HOST. Optional parameters include PORT, POOLED-P, USE-SSL, USE-BINARY, SERVICE and APPLICATION-NAME. A template file can be found at source. If used it should be moved to a location outside of an application's home directory.

`
DATABASE="<db-name>"
USER="<db-user>"
PASSWORD="<db-password>"
HOST="<db-host>"
#PORT=5432
#POOLED-P=t
#USE-SSL=no
#USE-BINARY=no
#SERVICE="postgres"
#APPLICATION-NAME=""
`


## Connections

By default STW-DB uses connection pools. Each connection pool is tied to an environment. For any specific environment a connection limit can be set either directly when defining the environment, or by use of the SET-CONNECTION-LIMIT macro.

Connection limits are set with a positive numeric value: a negative value indicates no limit and 0 a deactivated pool. The default is no limits. NOTE: Connection limits are enironment specific. I.e. a value of 10 for each of ten defined database environments means 100 potential connections." This is useful for establishing database access to more than one database, and for throttling/prioritizing database access for specific types of queries.

Clearing a connection pool is achieved by passing a defined environment to the CLEAR-CONNECTION-POOL macro.

Access to the connection pool is limited to the layered functions MAKE-CONNECTION and CLOSE-CONNECTION both of which are called automatically by the DB-CONNECT macro. Both make best attempts to use atomic procedures, otherwise a lock is used to protect pool access during push/pop operations.


# STW-META and CONTEXTL

STW-DB uses STW-META, a thin wrapper around CONTEXTL. For further information about the protocol STW-DB uses to define table classes or interface nodes please see the documentation for STW-META. With that said, the section on class definition initialization is repeated here:

> 8. Initializing the class definition - important!
>
> As layered classes are subclasses of STANDARD-CLASS, initialization protocols proceed as per normal. As such, context specific initialization procedures should not be placed within initialize-instance, reinitialize-instance or shared-initialize methods. To put it simply, they are not thread safe. Instead the layered function INITIALIZE-IN-CONTEXT is called from the auxiliary :around method of shared-initialize, and after the call to call-next-method. Context and class specific initialization procedures should be placed in specialized instances of this layered functoin. It is for this reason that layered classes of type STW-BASE-CLASS are defined within their layer context.


## Context Layers

There are three basic layers. The most primitive is DB-TABLE-LAYER, which as the name suggests is for direct mapping with database tables. The next is a higher level DB-INTERFACE-LAYER which interfaces with layered application objects. Both of the above inherit from DB-LAYER, which creates an environment within which it's sub-layers can nest and alternate.

Inheriting from DB-TABLE-LAYER is INSERT-TABLE and DELETE-TABLE. Inheriting from DB-INTERFACE-LAYER is INSERT-NODE, DELETE-NODE, UPDATE-NODE and RETRIEVE-NODE. By establishing database operations as layered contexts, and through the use of layered functions, we're able to establish a relatively uniform protocol for database operations.



# Table Definitions

Three macros are provided for database table definitions: DEFINE-KEY-TABLE, DEFINE-DB-TABLE and DEFINE-INTERFACE-NODE.

DEFINE-KEY-TABLE defines a key table of type DB-KEY-TABLE in the DB-TABLE-LAYER context. It is a singleton, and has the sole purpose of defining a single column table of autoincrementing values.

(define-key-table user-base () id)


## DB-TABLE-CLASS

DEFINE-DB-TABLE defines a key table of type DB-TABLE-CLASS in the DB-TABLE-LAYER context.

A table layer is also a singleton, and maps directly to database table/relation. Using the DB-TABLE-CLASS metaclass, each slot is defined as type DB-COLUMN-SLOT-DEFINITION. Relevant initargs include: :schema, :col-type, :primary-key, :root-key, :foreign-key, :unique, :check, :default, :index, and :not-null. As the example below shows, many slots do not require initial arguments, as values are largely determined and slots bound during initialization. Schema is a case in point, where schema is passed as an argument to the metaclass DB-TABLE-CLASS, and is then assigned using reflective techniques to all relevant slots or classes as required.

`(define-db-table user-account ()
  ((id :col-type :integer
       :primary-key t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))
   (password :col-type :text
	     :not-null t)
   (created-on :col-type :timestamptz
	       :default (now))
   (created-by :col-type :integer
	       :not-null t
	       :foreign-key (:table user-base
			     :column id
			     :on-delete :cascade
			     :on-update :cascade
			     :no-join t))
   (validated :col-type :boolean
	      :default nil)))


(define-db-table user-site ()
  ((id :col-type :integer
       :primary-key t
       :not-null t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))
   (site :primary-key t
	 :not-null t
	 :col-type :text)))`


## DB-INTERFACE-NODE

DEFINE-INTERFACE-NODE defines a layered class of type DB-INTERFACE-CLASS. An interface node, it inherits slots from one or more table classes, and can in turn be inherited itself. The type of DB-INTERFACE-CLASS slots is DB-AGGREGATE-SLOT-DEFINITION and is used to aggregate the multiple values of a one to many relation. It does so by mapping a slot of type DB-COLUMN-SLOT-DEFINITION from a class of type DB-TABLE-CLASS. For this end, DB-AGGREGATE-SLOT-DEFINITION provides initargs for :maps-table, :maps-column and :type. 

`(define-interface-node account
  (user-base user-account)
  ((sites :maps-table user-site :maps-column site :type list)))`

In essence, an interface node is a tree of relations mapped onto a database. A list of relevant tables is aggregated during compilation including both table classes listed as superclasses and tables mapped by slots of type DB-AGGREGATE-SLOT-DEFINITION. This list of tables is stored in the class slot TABLES, and sorted so that column slots referenced by foreign keys always precede the referring slot. The first table in the list TABLES after sorting is the root table, which as it is used throughout database operations, must be referenced by all other tables in the TABLES slot, either directly or indirectly. Specifically, the primary keys of the root table act as the primary keys of the interface node.



# Procedures and Functions

Procedures and functions are created by the layered function GENERATE-PROCEDURE, which is called by the layered function SYNC. SYNC takes an interface node and component, and various keyword arguments, and caches the resulting procedure/function.



# Refreshing the Cache

To refresh the cached procedure simply set the refresh-cache keyword when invoking SYNC.


# DB Queries and Operations

##Inserting a record:

`(db-connect db (insert-node) (sync (make-instance 'account) nil))`

When inserting, the procedure generated is determined by the slots with assigned values, the presence or absence of the not-null attribute or the presence of a default value. Any autogenerated values such as those of type serial, are returned and the class is updated.

To insert into one table only provide the additional component (table) class, and change the context to insert-table.

`(db-connect db (insert-table) (sync (make-instance 'account) (find-class 'user-site)))`


## Updating a record:

`(db-connect db (update-node) (sync <old> <new>))`

In this instance <old> refers to the original application object that we wish to update where appropriate with the values in <new>. Once again the procedure generated accords to the presence of values in <new> that differ from the values in <old>. A necessary condition to updating an object is that the key-column value in <old> corresponds to the key-column value in <new>. Otherwise the objects will be assumed to belong to different records and will result in an error. Updated columns are returned and their values assigned to the relevant slots in class <old>


## Deleting a record:

`(db-connect db (insert-node) (sync <object> nil))`

To delete an object ensure that the key-column is present. The generated procedure will call delete on the relevant record in the key column's table. Whether the delete propogates through the foreign key chain is determined by the setting :on-delete in the foreign key constraint. The choices are :cascade :restrict and :no-action.

For more on the merits and uses for each option, please see [the relevant page](https://www.postgresql.org/docs/14/ddl-constraints.html) in [Postgresql Documentation](https://www.postgresql.org/docs/14).


To delete from one table only provide the additional component (table) class, and change the context to delete-table.

`(db-connect db (delete-table) (sync (make-instance 'account) (find-class 'user-site)))`


## Retrieving a record:

`(db-connect db (retrieve-node) (sync <object> nil))`

In retrieving a node the values assigned to slots are used in the "where" clause of the select statement. Joins are by default inner-joins, however tables pushed to the key parameter :OPTIONAL-JOIN of the layered function SYNC are reordered and joined using a left outer join. Tables pushed to the :UNION-QUERY or UNION-ALL-QUERIES key parameters, are also queried by means of a nested select expression in the "from" clause.



## Setting up:

The simplest procedure for setting up is to attempt to insert a record and let the insert fail. If a schema, table, type, or procedure is unknown, a database-error is returned and the necessary resource created recursively. Otherwise, a slew of layered functions is available to: CREATE-SCHEMA, SET-SCHEMA, SET-PRIVILEGED-USER, CREATE-TABLE-STATEMENT, FOREIGN-KEY-STATEMENTS, CREATE-PG-COMPOSITE, CREATE-TYPED-DOMAIN.

Note: All foreign keys unless already indexed are automatically indexed.



## Tearing down:

TRUNCATE-TABLE, DROP-TABLE and DROP-SCHEMA, all require user confirmation to proceed.



# Conditions and Restarts

## NULL-KEY-ERROR

Should the primary keys of the root table be unbound or null during an update or delete operation, an error of NULL-KEY-ERROR is invoked. As deleting / updating multiple rows according to specific column values may well have been the goal of the operation, a restart NOT-AND-ERROR is provided, which mimics the behaviour of the CONTINUE restart.


## UPDATE-KEY-VALUE-ERROR

On copying an interface node object: If the original node has a root table primary key value while the clone has either no value, is unbound, or a value that does not match the original node, a correctable UPDATE-KEY-VALUE-ERROR is invoked, and the restart USE-EXPECTED-VALUE is provided. USE-EXPECTED-VALUE takes the value assigned to the primary keys of the original node, and assigns them to the respective slots of the clone, before continuing on its merry way. Thus a normal update may proceed. This is useful if updating a bunch of different nodes to fit a template of values. Say for example to reset values to some default. However, in the event that an update of values is desired, select CONTINUE. It should be noted though, that while an update will occur, the final operation will be a delete operation, where the root key record(s) being derived from select statement using supplied values are then deleted. As such any keys defaulting to a cascade, on deletion, will be lost.


# TO DO

- Add support for additional options for select queries: aggregate functions such as count, max, etc, with having and group-by. The latter could be done by adding an additional interface type/context???
- Add additional transaction support for procedures such as commit, rollback, and savepoint.
- Revisit dates, and look at intervals.
