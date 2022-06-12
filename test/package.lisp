(defpackage stw.db.test
  (:use :cl :parachute :stw.db)
  (:import-from
   :contextl
   :with-active-layers)
  (:import-from
   :stw.meta
   :clone-object
   :find-slot-definition)
  (:import-from
   :cl-postgres
   :database-connection)
  (:shadow
   :define-db-table
   :define-key-table)
  (:export :stw-db))

(in-package stw.db.test)

(define-test stw-db)

(defparameter *schema* "stw_test_schema")
