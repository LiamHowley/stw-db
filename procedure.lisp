(in-package stw.db)


(define-layered-class procedure
  :in db-layer ()
  ((schema :initarg :schema :accessor schema)
   (name :initarg :name :initform nil :accessor name)
   (args :initarg :args :initform nil :accessor args)
   (p-controls :initarg :controls :initform nil :accessor p-controls)
   (p-control :initarg :p-control :initform nil :accessor p-control)
   (relevant-slots :initarg :relevant-slots :initform nil :accessor relevant-slots)
   (vars :initarg :vars :initform nil :accessor vars)
   (sql-list :initarg :sql-list :initform nil :accessor sql-list)
   (sql-statement :initarg :sql-statement :initform nil :reader sql-statement)
   (table :initarg :table :reader table)))


(defmethod slot-unbound (class (instance procedure) (slot-name (eql 'p-values)))
  (setf (slot-value instance slot-name) nil))


(define-layered-method statement
  :in-layer db-layer ((class procedure))
  (with-slots (schema name args vars sql-list sql-statement) class
    (setf sql-statement
	  (format nil "CREATE OR REPLACE PROCEDURE ~a (~a)~%LANGUAGE plpgsql~%AS $BODY$~%~a~%BEGIN~%~a~%END;~%$BODY$;"
		  (set-sql-name schema name)
		  (format nil "~@[~{~{~a ~a~@[ ~a~]~}~^, ~}~]" args)
		  (format nil "~@[DECLARE~%~{~{~a ~a~@[ := ~a~];~}~%~}~]" vars)
		  (format nil "~{~a~^~%~}" sql-list)))
    class))

(define-layered-function call-statement (class)
  (:method 
      :in-layer db-layer ((class procedure))
    (with-slots (schema name p-values) class
      (format nil "CALL ~a.~a (~@[~{~a~^, ~}~])" schema name p-values))))


(defstruct component
  (sql nil :type string)
  (declarations () :type list)
  (params)
  (param-controls))

(defstruct var
  column var param)



(define-layered-function generate-component (class))

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


;;; generating a procedure for insert/delete ops

(define-layered-method generate-procedure
  :in-layer db-layer
  :around ((class serialize) component)
  (let ((procedure (call-next-method)))
    (set-control procedure)
    (statement procedure)
    procedure))


(define-layered-function set-control (procedure)
  (:method
      :in db-layer ((procedure procedure))
    (with-slots (schema name p-control p-controls) procedure
      (setf p-control (format nil "CALL ~a.~a (~@[~{~a~^, ~}~])"
			      schema name (mapcar #'(lambda (control)
						      (cond ((and control (car control))
							     (car control))
							    (control "~a")
							    (t "null")))
						  p-controls)))))
  (:method
      :in update-node ((procedure procedure))
    (with-slots (schema name p-control p-controls) procedure
      (setf p-control (format nil "CALL ~a.~a (~@[~{~a~^, ~}~])"
			      schema name (map-tree-depth-first #'stringp p-controls))))))


(define-layered-function generate-procedure (class component)

  (:method
      :in-layer db-table-layer ((class serialize) (component db-table-class))
    (with-slots (schema table require-columns referenced-columns) component
      (let ((procedure (make-instance 'procedure
				      :schema schema
				      :table class))
	    (returns))
	(with-slots (args vars sql-list p-controls relevant-slots) procedure
	  (let ((component (generate-component component)))
	    (with-slots (sql params param-controls declarations) component
	      (loop
		for declaration in declarations
		for var = (var-var declaration)
		collect (var-param declaration) into params%
		collect var into vars%
		collect (format nil "~a := ~a;" (var-column declaration) (car var)) into returns%
		finally (setf vars vars%
			      params (nconc params params%)
			      returns returns%))
	      (setf sql-list `(,(apply #'format nil sql
				       (loop
					 for i from 1 to (length params)
					 collect i))
			       ,@returns)
		    args params
		    p-controls param-controls
		    relevant-slots (get-relevant-slots class procedure)))))
	procedure))))


(define-layered-function get-relevant-slots (class procedure)
  (:method
      :in db-layer ((class serialize) (proc procedure))
    (with-slots (p-controls) proc
      (mapcar #'(lambda (control)
		  (when control
		    (if (consp (cadr control))
			(loop
			  for slot in (cadr control)
			  for mapped-by = (match-mapping-node (class-of class) slot)
			  if mapped-by
			    collect (slot-value mapped-by 'mapping-slot)
			  else
			    collect slot)
			(aif (slot-value (cadr control) 'mapped-by)
			     (loop
			       for mapping in self
			       when (typep class (slot-value mapping 'mapping-node))
				 do (return mapping))
			     (cadr control)))))
	      p-controls))))
