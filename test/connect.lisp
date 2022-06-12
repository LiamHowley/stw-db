(in-package stw.db.test)

(defparameter *file* nil)

(define-test params-file...
  :parent stw-db
  (princ "Please specify a file with connection params.")
  (terpri)
  (setf *file* (read))
  (unless (stringp *file*)
    (setf *file* (string *file*)))
  (true (probe-file *file*))
  (with-active-layers (db-layer)
    (of-type list (connection-params *file*))))

(define-test connecting...
  :parent stw-db
  :depends-on (params-file...)
  (define-db-environment db
      (connection-params *file*))
  (db-connect db (db-layer)
    (of-type cl-postgres:database-connection stw.db::*db*)))
