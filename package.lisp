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
		:mappend
		:map-tree-depth-first
		:ordered-plist-values
		:ensure-list
		:flatten
		:reverse-flatten
		:number-range
		:array-to-list)
  (:import-from :stw.meta
		:with-context
		:delete-context
		:define-base-class
		:stw-base-layer
		:serialize
		:serialized-p
		:base-class
		:stw-base-class
		:stw-direct-slot-definition
		:stw-layer-context
		:slot-definition-class
		:initialize-in-context
		:find-slot-definition
		:map-filtered-slots
		:filter-slots-by-type
		:filter-precedents-by-type
		:object-to-plist
		:slots-with-values
		:clone-object
		:equality)
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
		:partial-class-base-initargs
		:remove-layer
		:adjoin-layer-using-class)
  (:import-from :closer-mop
		:slot-definition-name
		:slot-definition-type
		:slot-definition-initargs)
  (:import-from :cl-postgres
		:database-connection
		:open-database
		:database-open-p
		:close-database
		:exec-query
		:get-postgresql-version
		:database-error
		:database-error-code
		:row-reader
		:ignore-row-reader
		:next-row
		:next-field
		:field-name)
  (:import-from :atomics
		:implementation-not-supported
		:atomic-pop
		:atomic-push)
  (:import-from :bordeaux-threads
  		:make-lock
  		:with-lock-held)
  (:export :define-db-table
	   :define-key-table
	   :define-interface-node

	   :db-interface-class
	   :db-table-class
	   :db-key-table

	   :db-layer
	   :db-interface-layer
	   :db-table-layer
	   :insert-node
	   :update-node
	   :retrieve-node
	   :delete-node
	   :insert-table
	   :delete-table

	   :define-db-environment
	   :delete-db-environment
	   :connection-params
	   :db-connect
	   :connection-pool
	   :clear-connection-pool
	   :set-connection-limit

	   :db-base-column-definition
	   :db-column-slot-definition
	   :db-aggregate-slot-definition

	   ;; ops
	   :execute
	   :read-row-to-class

	   ;; template dispatch and caching
	   :get-key
	   :db-template-register
	   :proc-template
	   :dispatcher
	   :dispatch-statement
	   :update-op-dispatch-statement

	   ;;; procedures / functions
	   :generate-procedure
	   :generate-components
	   :generate-component

	   ;;; schema
	   :create-schema
	   :set-schema
	   :set-privileged-user

	   ;;;; statement functions
	   :create-table-statement
	   :foreign-keys-statements
	   :index-statement

	   ;;; utils

	   ;; syntax
	   :db-syntax-prep
	   :sql-op
	   :set-sql-name
	   :date/time-p

	   ;; formatting
	   :infill-column
	   :infix-constraint

	   ;; errors
	   :invalid-operator-error))

(in-package :stw.db)
