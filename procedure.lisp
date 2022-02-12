(in-package stw.db)


(define-layered-class procedure
  :in db-layer ()
  ((schema :initarg :schema :accessor schema)
   (name :initarg :name :accessor name)
   (args :initarg :args :initform nil :accessor args)
   (p-values :initarg :values :initform nil :accessor p-values)
   (vars :initarg :vars :initform nil :accessor vars)
   (sql-list :initarg :sql-list :initform nil :accessor sql-list)
   (sql-statement :initarg :sql-statement :initform nil :reader sql-statement)))


(define-layered-method statement
  :in-layer db-interface-layer ((class procedure))
  (with-slots (schema name args vars sql-list sql-statement) class
    (setf sql-statement
	  (format nil "CREATE OR REPLACE PROCEDURE ~a (~a)~%LANGUAGE plpgsql~%AS $BODY$~%~a~%BEGIN~%~a~%END;~%$BODY$;"
		  (set-sql-name schema name)
		  (format nil "~@[~{~{~a~@[ ~a~]~}~^, ~}~]" args)
		  (format nil "~@[DECLARE~%~{~{~a ~a~@[ := ~a~];~}~%~}~]" vars)
		  (format nil "~{~a~^~%~}" sql-list)))
    class))

(defstruct component
  (sql nil :type string)
  (declarations () :type list)
  (params)
  (param-controls))


;;; pg composite arrays - used in passing values to insert procedure calls

(define-layered-function sql-typed-array (class)
  (:method
      :in db-layer ((class db-table-class))
    (with-slots (schema table require-columns mapped-by) class
      (let ((table-name (set-sql-name schema table))
	    (control (if mapped-by
			 "ARRAY[~~{(~{~a~^, ~})~~^, ~~}]::~a_type[]"
			 "ARRAY[(~{~a~^, ~})]::~a_type[]")))
	(list 
	 (format nil control
		 (loop
		   for slot in require-columns
		     collect "~a")
		 table-name)
	 require-columns)))))
