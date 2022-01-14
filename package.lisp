(in-package :cl-user)

(defpackage :stw.db
  (:use :cl)
  (:import-from :stw.util
		:ensure-list
		:with-gensyms)
  (:import-from :stw.meta
		:define-base-class
		:stw-base-layer
		:base-class
		:stw-base-class
		:stw-direct-slot-definition
		:stw-layer-context
		:direct-slot-class
		:filter-slots-by-type)
  (:import-from :contextl
		:deflayer
		:define-layered-class
		:define-layered-method
		:call-next-layered-method
		:partial-class
		:partial-class-base-initargs
		:remove-layer
		:adjoin-layer-using-class)
  (:import-from :closer-mop
		:slot-definition-name)
  (:export :define-db-table
	   :define-interface-node

	   :db-interface-layer
	   :db-table-layer

	   :db-column-slot-definition


	   ;;;; utils

	   ;; syntax
	   :db-syntax-prep
	   :sql-op
	   :set-sql-name
	   :date/time-p

	   ;; formatting
	   :infill-column))

(in-package :stw.db)
