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
  (:export :live-tests
	   :run-tests))

(in-package stw.db.test)

(define-test stw-db)

(defmacro run-tests (&optional include-live-tests)
  `(if ,include-live-tests
       (progn 
	(test 'stw-db)
	(live-tests))
       (test 'stw-db)))

(defparameter *schema* "stw_test_schema")
