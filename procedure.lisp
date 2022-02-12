(in-package stw.db)


(define-layered-class procedure
  :in db-layer ()
  ((name :initarg :name :accessor name)
   (args :initarg :args :initform nil :accessor args)
   (p-values :initarg :values :initform nil :accessor p-values)
   (vars :initarg :vars :initform nil :accessor vars)
   (sql-list :initarg :sql-list :initform nil :accessor sql-list)
   (sql-statement :initarg :sql-statement :initform nil :reader sql-statement)))


(define-layered-method statement
  :in-layer db-interface-layer ((class procedure))
  (with-slots (name args vars sql-list sql-statement) class
    (setf sql-statement
	  (format nil "CREATE OR REPLACE PROCEDURE ~a (~a)~%LANGUAGE plpgsql~%AS $BODY$~%~a~%BEGIN~%~a~%END;~%$BODY$;"
		  name 
		  (format nil "~@[~{~{~a~@[ ~a~]~}~^, ~}~]" args)
		  (format nil "~@[~%~{~{DECLARE ~a ~a~@[ := ~a~];~}~%~}~]" vars)
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
    (with-slots (schema table require-columns) class
      (let ((table-name (set-sql-name schema table))
	    (slots))
	(list 
	 (format nil "ARRAY[(~{~a~^, ~})]::~a_type"
		 (loop for slot in require-columns
		       collect slot into slots%
		       collect "~a" into values
		       finally (setf slots slots%)
			       (return values))
		 table-name)
	 slots)))))

	  ;;(format nil "ARRAY[~{(~{~a~^, ~})~^, ~}]::~a_type"
