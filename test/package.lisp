(defpackage stw.db.test
  (:use :cl
	:parachute
	:stw.db)
  (:import-from :contextl
   :with-active-layers)
  (:shadow
   :define-db-table
   :define-key-table)
  (:export :stw-db))

(in-package stw.db.test)

(define-test stw-db)

(defparameter *schema* "stw")
