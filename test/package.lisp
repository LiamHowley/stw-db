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
  (:import-from
   :local-time
   :timestamp)
  (:shadow
   :define-db-table
   :define-key-table)
  (:export :run-tests))

(in-package stw.db.test)

(define-test stw-db)

(defmacro run-tests ()
  `(prog1
       (test 'stw-db)
     (db-connect db (db-layer)
       (drop-schema *schema* t))))


(defparameter *schema* "stw_test_schema")
