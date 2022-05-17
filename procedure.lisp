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


(defstruct component
  (sql "" :type string)
  (declarations () :type list)
  (params)
  (param-controls))

(defstruct var
  column var param)


(define-layered-function generate-components (class &key)
  (:documentation "Traverses a list of tables and calls generate-component.
Returns a hash table of table name => component."))

(define-layered-function generate-component (class function &key)
  (:documentation "Creates an instance of component. When function 
is non nill, a predicate is expected to determine the validity of 
a foreign key references within the context of the current
expressions."))


;;; pg composite arrays - used in passing values to insert procedure calls

(define-layered-function sql-typed-array (class)
  (:documentation "Control for postgres composite typed arrays, 
according to class.")

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
(define-layered-function generate-procedure (class component &rest rest &key)
  (:documentation "Generates and returns a procedure. If component is non nil
a procedure is generated for that component alone. In all contexts but update-node
a component is an instance of db-table-class or nil. When nil, the function
generate components is called which returns a hash-table of table name => components
which are subsequently parsed and aggregated.

When the layered context is update-node, a component is expected to be a cloned copy 
of class with updated values.")

  (:method
      :in-layer db-layer
      :around ((class serialize) component &rest rest &key)
    (declare (ignore rest))
    (let ((procedure (call-next-method)))
      (set-control procedure)
      (statement procedure)
      procedure))

  (:method
      :in-layer db-table-layer ((class serialize) (component db-table-class) &key)
    (declare (ignore rest))
    (with-slots (schema table require-columns referenced-columns) component
      (let ((procedure (make-instance 'procedure
				      :schema schema
				      :table class))
	    (returns)
	    (mapping-slot (mapping-slot (match-mapping-node class component))))
	(with-slots (args vars sql-list p-controls relevant-slots) procedure
	  (let ((component (generate-component component nil :mapping-column mapping-slot)))
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





(define-layered-function set-control (procedure)
  (:documentation "Returns a control string to be populated with values 
from an instance of serialize, with which to query a database.")
  
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
