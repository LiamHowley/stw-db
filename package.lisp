(in-package :cl-user)

(defpackage :stw.db
  (:use :cl)
  (:import-from :stw.util
		:aif
		:awhen
		:self
		:scase
		:ensure-list
		:explode-string
		:with-gensyms
		:map-tree-depth-first
		:ordered-plist-values)
  (:import-from :stw.meta
		:with-context
		:define-base-class
		:stw-base-layer
		:serialize
		:base-class
		:stw-base-class
		:stw-direct-slot-definition
		:stw-layer-context
		:direct-slot-class
		:find-slot-definition
		:filter-slots-by-type
		:filter-precedents-by-type
		:object-to-plist)
  (:import-from :contextl
		:defdynamic
		:dynamic
		:dlet
		:capture-dynamic-environment
		:with-dynamic-environment
		:deflayer
		:with-active-layers
		:define-layered-class
		:define-layered-function
		:define-layered-method
		:call-next-layered-method
		:singleton-class
		:partial-class
		:partial-class-base-initargs
		:remove-layer
		:adjoin-layer-using-class)
  (:import-from :closer-mop
		:slot-definition-name
		:slot-definition-initargs)
  (:import-from :cl-postgres
		:database-connection
		:open-database
		:database-open-p
		:close-database
		:exec-query
		:get-postgresql-version
		:database-error
		:database-error-code)
  (:import-from :atomics
		:implementation-not-supported
		:atomic-pop
		:atomic-push)
  (:import-from :fare-memoization
		:memoize
		:unmemoize)
  (:import-from :bordeaux-threads
  		:make-lock
  		:with-lock-held)
  (:export :define-db-table
	   :define-key-table
	   :define-interface-node

	   :db-interface-layer
	   :db-table-layer
	   :db-layer

	   :db-interface
	   :db-table
	   :db-connect
	   :connection-pool
	   :clear-connection-pool
	   
	   :db-base-column-definition
	   :db-column-slot-definition
	   :db-aggregate-slot-definition

	   ;;;; schema
	   :create-schema
	   :set-schema
	   :set-privileged-user

	   ;;;; statement functions
	   :create-statement
	   :foreign-keys-statements
	   :index-statement

	   ;;;; utils

	   ;; syntax
	   :db-syntax-prep
	   :sql-op
	   :set-sql-name
	   :date/time-p

	   ;; formatting
	   :infill-column))

(in-package :stw.db)
