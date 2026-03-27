(in-package :cl-user)

(defpackage :stw.db
  (:use :cl :iterate)

  (:import-from
   :stw.util
   :trie
   :make-trie
   :insert-word
   :walk-branch
   :trie-leaf
   :insert-word
   :find-and-replace)

  (:import-from
   :stw.util
   :aif
   :awhen
   :self
   :iterate-extend
   :for=
   :with=
   :collect-into
   :collect-all
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

  (:import-from
   :cl-comp
   :with-context
   :delete-context
   :define-base-class
   :serialize
   :serialized-p
   :base-class
   :comp-base-layer
   :comp-base-class
   :comp-direct-slot-definition
   :comp-layer-context
   :slot-definition-class
   :initialize-in-context
   :find-slot-definition
   :map-filtered-slots
   :filter-slots-by-type
   :filter-precedents-by-type
   :find-class-precedent
   :slots-with-values
   :object-to-plist
   :clone-object
   :equality)

  (:import-from
   :contextl
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

  (:import-from
   :closer-mop
   :slot-definition-name
   :slot-definition-type
   :slot-definition-initargs)

  (:import-from
   :cl-postgres
   :database-connection
   :open-database
   :database-open-p
   :close-database
   :exec-query
   :get-postgresql-version
   :row-reader
   :ignore-row-reader
   :next-row
   :next-field
   :field-name
   :to-sql-string
   :database-error
   :database-error-code
   :database-error-message)

  (:import-from
   :atomics
   :implementation-not-supported
   :atomic-pop
   :atomic-push)

  (:import-from
   :bordeaux-threads
   :make-lock
   :with-lock-held)

  (:import-from
   :local-time
   :now
   :universal-to-timestamp
   :set-local-time-cl-postgres-readers)

  (:import-from
   :uuid
   :make-v3-uuid
   :+namespace-oid+)

  (:export
   :define-db-table
   :define-key-table
   :define-root-table
   :define-interface-node

   :db-interface-class
   :db-table-class
   :db-key-table
   :db-root-table

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
   :*db*
   :connection-pool
   :clear-connection-pool
   :set-connection-limit

   :db-base-column-definition
   :db-column-slot-definition
   :enumerated-column-slot-definition
   :date/time-column-slot-definition
   :db-aggregate-slot-definition

   ;; ops
   :sync
   :read-row-to-class
   :parse-result

   ;; template dispatch and caching
   :get-key
   :db-template-register
   :proc-template
   :dispatcher
   :dispatch-statement
   :update-op-dispatch-statement

   ;; procedures / functions
   :generate-procedure
   :generate-components
   :generate-component

   ;; schema
   :*schema*
   :schema
   :nodes
   :create-schema
   :set-schema
   :set-privileged-user
   :initialize-schema

   ;; statement functions
   :create-table-statement
   :create-enumerated-types-statement
   :foreign-keys-statements
   :index-statement

   ;; tearing-down
   :drop-schema
   :drop-table
   :truncate-table

   ;; utils

   ;; syntax
   :db-syntax-prep
   :sql-op
   :set-sql-name
   :date/time-p

   ;; formatting
   :infill-column
   :infix-constraint

   ;; conditions and restarts
   :invalid-operator-error
   :null-key-error
   :null-value-error
   :update-key-value-error
   :not-an-error
   :use-expected-value
   :reserved-keyword-error
   :reserved-function/type-name-error))

(in-package :stw.db)

;;; setting date/time readers to use local-time
(set-local-time-cl-postgres-readers)
