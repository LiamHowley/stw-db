(in-package stw.db)


(define-layered-function insert (class)

  (:method
      :in-layer db-interface-layer ((class serialize))
    (let* ((schema (slot-value (class-of class) 'schema))
	   (procedure (make-instance 'procedure :schema schema))
	   (include-tables (include-tables class))
	   (dispatch-statement (insert-dispatch-statement class procedure include-tables)))
      (handler-case (exec-query *db* dispatch-statement)
	(database-error (err)
	  (let* ((base-class (class-of class))
		 (class-name (db-syntax-prep (class-name base-class))))
	    (flet ((respond (proc-name &rest functions)
		     (multiple-value-bind (statement procedure)
			 (apply #'build-db-component base-class proc-name functions)
		       (exec-query *db* statement)
		       (exec-query *db* (call-statement procedure)))
		     (insert class)))
	      ;; in theory at least this should be safe from infinitely recursive
	      ;; loops as the dispatch statement and procedure are both generated
	      ;; by reference to the same object.
	      (scase (database-error-code err)
		     ("3F000"
		      ;; MISSING SCHEMA
		      ;; Response: Create schema and recurse
		      (exec-query *db* (create-schema schema))
		      (exec-query *db* (set-schema schema))
		      (insert class))
		     ("42704"
		      ;; MISSING TYPE
		      ;; Response: Create types for all relevant
		      ;; table inserts. 
		      (let ((proc-name (format nil "initialize_~a_types" class-name)))
			(respond proc-name #'create-pg-composite)))
		     ("42P01"
		      ;; MISSING TABLE OR TYPE IN DB.
		      ;; Response: rebuild database component and
		      ;; (re)initialize database.
		      ;; Note: An effective way to build a database is to
		      ;; let an insert fail and thus invoke this response.
		      (let ((proc-name (format nil "initialize_~a_relations" class-name)))
			(respond proc-name #'create-statement #'foreign-keys-statements #'index-statement)))
		     ("42883"
		      ;; MISSING INSERT PROCEDURE
		      ;; Response: Make procedure and recurse.
		      (insert-statement class procedure include-tables)
		      (exec-query *db* (sql-statement procedure))
		      (insert class))
		     (t err)))))))))
      


(define-layered-function insert-dispatch-statement (class procedure include-tables)

  (:method
      :in-layer db-interface-layer ((class serialize) (procedure procedure) include-tables)
    (let* ((base-class (class-of class))
	   (components (insert-components base-class))
	   (tables (or include-tables (include-tables class))))
      (with-slots (name p-values) procedure
	(setf name (format nil "~(~a~)_insert" (class-name base-class))
	      p-values
	      (loop
		for table in tables
		for param-controls = (slot-value (gethash table components) 'param-controls)
		for mapped-table = (slot-value (find-class table) 'mapped-by)
		when (or param-controls mapped-table)
		  collect (process-values class param-controls mapped-table)))
	(call-statement procedure)))))


(define-layered-function process-values (class controls mapped)
  (:documentation "Values are collated, prepared and passed as args
to be formatted. Mapped slots are refer to the mapping slot for value
acquisition. Returns pg array string.")

  (:method
      :in-layer db-interface-layer ((class serialize) (controls cons) mapped)
    (destructuring-bind (control slots) controls
      (let ((parenthesize (when (eql (length slots) 1) t)))
	(apply #'format nil control
	       (if mapped
		   ;; one to many 
		   (loop
		     for mapping in mapped
		     for mapping-node = (mapping-node mapping)
		     for mapping-slot-name = (slot-definition-name (mapping-slot mapping))
		     when (eq (class-of class) mapping-node)
		       collect (loop for value in (slot-value class mapping-slot-name)
				     collect (prepare-value (col-type (mapped-column mapping))
							    value
							    parenthesize)))
		   ;; one to one 
		   (loop
		     for slot in slots
		     for slot-name = (slot-definition-name slot)
		     collect (prepare-value (col-type slot)
					    (slot-value class slot-name) parenthesize))))))))


(define-layered-function insert-statement (class procedure include-tables)

  (:method
      :in-layer db-interface-layer ((class serialize) (procedure procedure) include-tables)
    (let* ((base-class (class-of class))
	   (components (insert-components base-class))
	   (tables (or include-tables (include-tables class))))
      (with-slots (args vars sql-list sql-statement) procedure
	(loop
	  with num = 0
	  for table in tables
	  do (with-slots (sql declarations params)
		 (gethash table components)
	       (loop
		 for declaration in declarations
		 do (push declaration vars))
	       (cond (params
		      (push params args)
		      (push (format nil sql (incf num)) sql-list))
		     (t (push sql sql-list)))))
	(setf args (nreverse args)
	      sql-list (nreverse sql-list)
	      vars (nreverse vars))
	(statement procedure)))))



(define-layered-function include-tables (class)
  (:documentation "Included tables must be either:
1. have required-columns that are bound and not null, or
2. have no required columns.")

  (:method
      :in db-interface-layer ((class serialize))
    (let ((tables (slot-value (class-of class) 'tables)))
      (loop
	with num = 0
	for table in tables
	for required = (require-columns (find-class table))
	for include-table = (cond ((and required
					(loop
					  for slot in required
					  always (slot-to-go class slot)))
				   t)
				  (required nil)
				  (t t))
	when include-table
	  collect table))))
				


(define-layered-function slot-to-go (class slot)
  (:documentation "A column that is set to NOT NULL, has neither a DEFAULT set 
nor a derived value from a FOREIGN KEY, nor is of type serial, must have a supplied
value, otherwise an error will be thrown upon inserting. Should SLOT not
be present in CLASS, SLOT must be mapped and the mapping slot, must be present, bound
and not null. Returns all slots with their respective values or nil.")

  (:method
      :in db-interface-layer ((class serialize) (slot db-column-slot-definition))
    (let ((slot-name (slot-definition-name slot)))
      ;; is it a directly inherited slot or a mapped-slot
      (if (slot-exists-p class slot-name)
	  (when (slot-boundp class slot-name)
	    (or (slot-value class slot-name)
		(let ((default (slot-value slot 'default)))
		  (or default
		      (and default (eq (slot-value slot 'col-type) :boolean))))))
	  (awhen (slot-value slot 'mapped-by)
	    (loop
	      for mapping in self
		thereis (let ((slot-name (slot-definition-name (mapping-slot mapping))))
			  (and (eq (mapping-node mapping) (class-of class))
			       (slot-boundp class slot-name)
			       (slot-value class slot-name)))))))))



(defvar *components* (make-hash-table :test #'equal))

(clrhash *components*)

(declaim (notinline insert-components))

(unmemoize 'insert-components)

(define-layered-function insert-components (class)
  (:documentation "When specializing on the (sub)class DB-KEY-TABLE 
two values are returned: 1. the class-name and 2. an instance of
the structure class COMPONENT. When called with DB-INTERFACE-CLASS
each referenced table is subsequently called, a hash-table is returned
with the key value pair, class-name => COMPONENT. Results are cached.")

  (:method
      :in db-interface-layer ((class db-interface-class))
    (let* ((tables (tables class))
	   (components (make-hash-table :test #'eq :size (length tables))))
      (with-active-layers (db-table-layer)
	(loop
	  for table in tables
	  do (multiple-value-bind (table-class component)
		 (insert-components (find-class table))
	       (setf (gethash table-class components) component))))
      components))

  (:method
      :in db-table-layer ((class db-key-table))
    (with-slots (schema table) class
      (let* ((slot (car (filter-slots-by-type class 'db-column-slot-definition)))
	     (column (column-name slot))
	     (table-name (set-sql-name schema table))
	     (var (list (format nil "_~a_~a"
				(db-syntax-prep table) column)
			(let ((col-type (col-type slot)))
			     (if (eq col-type :serial) :integer col-type))
			     nil)))
	(values
	 (class-name class)
	 (make-component 
	  :sql (format nil "INSERT INTO ~a DEFAULT VALUES RETURNING ~a INTO ~a;"
		       table-name column (car var))
	  :declarations (list var))))))


  (:method :in-layer db-table-layer ((class db-table-class))
    (with-slots (schema table require-columns referenced-columns) class
      (let* ((vars (mapcar #'column-name require-columns))
	     (columns vars)
	     (declared-vars)
	     (into (mapcar #'(lambda (column)
			       (list 
				(format nil "_~a_~a" table (car column))
				(let ((col-type (cadr column)))
				  (if (eq col-type :serial) :integer col-type))
				nil))
			   referenced-columns)))
	(map nil #'(lambda (f-key)
		     (destructuring-bind (&key schema key table column no-join &allow-other-keys) f-key
		       (declare (ignore schema))
		       (unless no-join
			 (push key columns)
			 (let ((reference (format nil "_~a_~a"
						  (db-syntax-prep table)
						  (db-syntax-prep column))))
			   ;; If there are no vars as referenced in require-columns, all vars are
			   ;; referencing declared variables; i.e. returned results from table(s) insert op
			   ;; corresponding to the foreign-keys of the current table.
			   (cond (vars
				  (push reference vars)
				  (push reference declared-vars))
				 (t
				  (push reference declared-vars)))))))
	     (foreign-keys class))
	;; Returning values: The class-name of table and a structure object
	;; of type component, containing:
	;; 1. statement,
	;; 2. variables that need to be declared and
	;; 3. the pg reference for the argument array that needs to be set.
	;; 4. a control string to build the argument array.
	(values
	 (class-name class)
	 (make-component 
	  :sql (format nil
		       "INSERT INTO ~a (~{~a~^, ~}) ~a~@[ RETURNING ~{~a~^, ~} INTO ~{~a~^, ~}~];"
		       (set-sql-name schema table)
		       columns
		       (if vars
			   (format nil "SELECT ~{~a~^, ~} FROM UNNEST ($~~a)" vars)
			   (format nil "VALUES (~{~a~^, ~})" declared-vars))
		       (mapcar #'car referenced-columns)
		       (mapcar #'car into))
	  :declarations into
	  :params (when vars
		    (list (format nil "~a_type[]" (set-sql-name schema table)) nil))
	  :param-controls (when vars
			    (sql-typed-array class))))))))


(memoize 'insert-components :table *components*)
