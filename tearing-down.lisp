(in-package stw.db)

(defmacro safety-first (warning &body body)
  `(when
       (loop
	 for input = (progn
		       ,warning
		       (princ "Do you wish to proceed? Yes or No? ")
		       (read))
	 while input
	 until (or (string-equal input :yes)
		   (string-equal input :no))
	 finally (return (if (string-equal input "yes") t nil)))
     ,@body))


;;; schema

(define-layered-function drop-schema (schema)
  (:method
      :in db-layer (schema)
    (safety-first
      (warn "Schema ~a is about to be dropped." schema)
      (exec-query *db* (format nil "DROP SCHEMA ~(~a~)" schema)))))


;;; tables

(defmacro define-table-op (op format-control error-control &rest error-args)
  (with-gensyms (table-name table confirmed)
    `(define-layered-function ,op (class &optional ,confirmed)
       (:method
	   :in db-interface-layer ((class db-interface-class) &optional ,confirmed)
	 (safety-first
	   (unless ,confirmed
	     (warn ,error-control ,@error-args))
	   (with-slots (tables) class
	     (loop
	       for ,table in tables
	       do (with-active-layers (db-table-layer)
		    (,op (find-class ,table) t))))))
       (:method
	   :in db-table-layer ((class db-table-class) &optional ,confirmed)
	 (safety-first
	   (unless ,confirmed
	     (warn ,error-control ,@error-args))
	   (with-slots (schema table) class
	     (let ((,table-name (set-sql-name schema table)))
	       (exec-query *db* (format nil ,format-control ,table-name)))))))))


(define-table-op drop-table
  "DROP TABLE ~a;"
  "You are about to drop the table(s): ~a."
  (class-name class))

(define-table-op truncate-table
  "TRUNCATE TABLE ~a RESTART IDENTITY CASCADE;"
  "You are about to delete all data from the table(s): ~a."
  (class-name class))
