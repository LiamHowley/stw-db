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

(define-layered-function drop-schema (schema &optional cascade)
  (:method
      :in db-layer (schema &optional cascade)
    (safety-first
      (warn "Schema ~a is about to be dropped." schema)
      (restart-case
	  (exec-query *db* (format nil "DROP SCHEMA IF EXISTS ~(~a~)~@[ cascade~]" schema cascade))
	(cascade () (drop-schema schema t))))))
	  


;;; tables

(defmacro define-table-op (op format-control error-control &rest error-args)
  (with-gensyms (table-name key-table confirmed)
    `(define-layered-function ,op (class &optional ,confirmed)
       (:method
	   :in db-interface-layer ((class db-interface-class) &optional ,confirmed)
	 (unless ,confirmed
	   (warn ,error-control ,@error-args))
	 (with-slots (root-key) class
	   (let ((,key-table (slot-value root-key 'table)))
	     (with-active-layers (db-table-layer)
	       (,op ,key-table t)))))
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
  (db-syntax-prep (class-name class)))

(define-table-op truncate-table
  "TRUNCATE TABLE ~a RESTART IDENTITY CASCADE;"
  "You are about to delete all data from the table(s): ~a."
  (db-syntax-prep (class-name class)))
