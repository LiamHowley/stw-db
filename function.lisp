(in-package stw.db)

(define-layered-class db-function
  :in db-layer (procedure)
  ((sql-query :initarg :sql-query :initform nil :reader sql-query)))


(define-layered-method statement
  :in-layer db-layer ((class db-function))
  (with-slots (schema name args vars sql-query sql-statement) class
    (setf sql-statement
	  (format nil "CREATE OR REPLACE FUNCTION ~a (~a)~%RETURNS TABLE (~a)~%AS $BODY$~%BEGIN~%RETURN QUERY ~a~%END;~%$BODY$~%LANGUAGE plpgsql;"
		  (set-sql-name schema name)
		  (format nil "~@[~{~{~a ~a~@[ ~a~]~}~^, ~}~]" args)
		  (format nil "~{~{~a ~a~}~^,~%~}" vars)
		  sql-query))
    class))

(define-layered-method set-control
  :in retrieve-node ((function db-function))
  (with-slots (schema name p-control p-controls) function
    (setf p-control (format nil "SELECT * FROM ~a.~a (~@[~{~a~^, ~}~])"
			    schema name (mapcar #'(lambda (control)
						    (cond ((and control (car control))
							   (car control))
							  (control "~a")
							  (t "null")))
						p-controls)))))
