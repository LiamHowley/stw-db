(in-package stw.db.test)

(defparameter *file* nil)

(defmacro live-tests ()
  `(handler-case
       (progn (test 'live-test)
	      (db-connect db (db-layer)
		(drop-schema *schema* t)))
     (error () (db-connect db (db-layer)
		  (when (cl-postgres:database-open-p stw.db::*db*)
		    (drop-schema *schema* t))))))

(define-test live-test)
  
(define-test params-file...
  :parent live-test
  (princ "Please specify a file with connection params.")
  (terpri)
  (setf *file* (read))
  (unless (stringp *file*)
    (setf *file* (string *file*)))
  (true (probe-file *file*))
  (with-active-layers (db-layer)
    (of-type list (connection-params *file*))))

(define-test connecting...
  :parent live-test
  :depends-on (params-file...)
  (define-db-environment db
      (connection-params *file*))
  (db-connect db (db-layer)
    (of-type cl-postgres:database-connection stw.db::*db*)))
