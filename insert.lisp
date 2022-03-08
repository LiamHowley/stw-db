(in-package stw.db)


(define-layered-function read-row-to-class (class)

  (:method
      :in db-layer ((class serialize))
    (flet ((get-symbol-name (field)
	     (string-upcase (substitute #\- #\_ (string-left-trim '(#\_) field)))))
      (row-reader (fields)
	(let ((base-class (class-of class)))
	  (loop
	    while (next-row)
	    do (loop
		 for field across fields
		 for symbol-name = (get-symbol-name (field-name field))
		 for slot-name = (intern symbol-name (symbol-package (class-name base-class)))
		 when (find-slot-definition base-class slot-name 'db-column-slot-definition)
		   do (setf (slot-value class slot-name) (next-field field)))))))))


(define-layered-function insert (class component)

  (:method 
      :in-layer db-layer :around ((class serialize) component)
    (multiple-value-bind (statement procedure)
	(call-next-method)
      (handler-case (exec-query *db* statement (read-row-to-class class))
	(database-error (err)
	  (insert-error-handler class procedure err component))))
    class)

  (:method
      :in-layer db-interface-layer ((class serialize) component)
    (let* ((schema (slot-value (class-of class) 'schema))
	   (procedure (make-instance 'procedure :schema schema)))
      (values
       (insert-dispatch-statement class procedure)
       procedure)))

  (:method
      :in-layer db-table-layer ((class serialize) (component db-table-class))
    (let ((procedure (memoized-funcall #'generate-insert-component component)))
      (values
       (dispatch-statement class procedure)
       procedure))))


(define-layered-function insert-error-handler (class procedure err component)

  (:method
      :in db-layer ((class serialize) (procedure procedure) (err database-error) component)
    (let* ((base-class (class-of class))
	   (schema (slot-value base-class 'schema))
	   (class-name (db-syntax-prep (class-name base-class))))
      (flet ((respond (proc-name &rest functions)
	       (multiple-value-bind (statement procedure)
		   (apply #'build-db-component
			  (if component component base-class)
			  proc-name functions)
		 (exec-query *db* statement)
		 (exec-query *db* (call-statement procedure)))
	       (insert class component)))
	;; in theory at least this should be safe from infinitely recursive
	;; loops as the dispatch statement and procedure are both generated
	;; by reference to the same object.
	(scase (database-error-code err)
	       ("3F000"
		;; MISSING SCHEMA
		;; Response: Create schema and recurse
		(exec-query *db* (create-schema schema))
		(exec-query *db* (set-schema schema))
		(insert class component))
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
		(insert-statement class procedure)
		(exec-query *db* (sql-statement procedure))
		(insert class component))
	       (t (error err)))))))








(define-layered-function generate-insert-component (class)

  (:method
      :in-layer db-table-layer ((class db-table-class))
    (with-slots (schema table require-columns referenced-columns) class
      (let ((type-array (format nil "~a.~a_type[]" schema table))
	    (columns)
	    (select)
	    (procedure (make-instance 'table-proc
				      :schema schema
				      :table class
				      :name (format nil "~(~a~)_insert" (db-syntax-prep (class-name class)))))
	    (array-position))
	(with-slots (args sql-list p-controls) procedure
	  (loop
	    with i = 1
	    for slot in (filter-slots-by-type class 'db-column-slot-definition)
	    for column-name = (slot-value slot 'column-name)
	    for column-type = (slot-value slot 'col-type)
	    if (member slot require-columns :test #'eq)
	      collect column-name into required-columns
	    else
	      do (push (list :in column-name column-type) args)
	      and do (push (format nil "$~a" i) select)
	      and do (push `("NULL" ,slot) p-controls)
	      and do (incf i)
	    collect column-name into columns%
	    finally (setf array-position (format nil "$~a" i)
			  columns columns%)
		    (push (list :in type-array nil) args)
		    (push (sql-typed-array class) p-controls)
		    (setf args (nreverse args)
			  p-controls (nreverse p-controls)
			  select (nconc (nreverse select) required-columns)))
	  (setf sql-list
		(list
		 (format nil
			 "INSERT INTO ~a (~{~a~^, ~}) SELECT ~{~a~^, ~} from unnest(~a);"
			 (set-sql-name schema table)
			 columns
			 select
			 array-position))))
	(statement procedure))))
  
  (:method
      :in db-table-layer ((class db-key-table))
    (with-slots (schema table) class
      (let ((procedure
	      (make-instance 'table-proc
			     :schema schema
			     :table class
			     :name (format nil "~(~a~)_insert" (class-name class)))))
	(with-slots (sql-list args) procedure
	  (let* ((slot (car (filter-slots-by-type class 'db-column-slot-definition)))
		 (column-name (column-name slot))
		 (table-name (set-sql-name schema table))
		 (arg (format nil "_~a_~a" (db-syntax-prep table) column-name)))
	    (setf args (list :in arg (let ((col-type (col-type slot)))
				       (if (eq col-type :serial) :integer col-type))))
	    (setf sql-list
		  (list (format nil "INSERT INTO ~a DEFAULT VALUES RETURNING ~a INTO ~a;"
				table-name column-name arg)))
	    (statement procedure)))))))



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
be present in CLASS, SLOT must be mapped and the mapping slot must be present, bound
and not null. Returns a boolean.")

  (:method
      :in db-interface-layer ((class serialize) (slot db-column-slot-definition))
    (let ((slot-name (slot-definition-name slot)))
      ;; is it a directly inherited slot or a mapped-slot
      (if (slot-exists-p class slot-name)
	  (when (slot-boundp class slot-name)
	    (when (or (slot-value class slot-name)
		      (let ((default (slot-value slot 'default)))
			(or default
			    (and default (eq (slot-value slot 'col-type) :boolean)))))
	      t))
	  (awhen (slot-value slot 'mapped-by)
	    (loop
	      for mapping in self
		thereis (let ((slot-name (slot-definition-name (mapping-slot mapping))))
			  (and (eq (mapping-node mapping) (class-of class))
			       (slot-boundp class slot-name)
			       (slot-value class slot-name))))))))

  (:method
      :in db-interface-layer ((class serialize) (slot db-column-slot-definition))
    (with-slots (not-null default col-type) slot
      (cond ((eq col-type :serial)
	     t)
	    (not-null
	     (let* ((slot-name (slot-definition-name slot))
		    (slot-value (slot-value class slot-name)))
	       (when (or slot-value
			 (or default
			     (and default (eq col-type :boolean))))
		 t)))
	    (t t)))))



(define-layered-function insert-dispatch-statement (class procedure)
  (:method
      :in-layer db-interface-layer ((class serialize) (procedure procedure))
    (with-slots (p-values) procedure
      (setf schema (slot-value (class-of class) 'schema)
	    p-values (let* ((base-class (class-of class))
			    (components (generate-insert-components base-class))
			    (tables (include-tables class)))
		       (with-slots (schema) procedure
			 (setf name (format nil "~(~a~)_insert" (class-name base-class))))
		       (loop
			 with declared-vars = nil
			 for table in tables
			 for param-control = (slot-value (gethash table components) 'param-controls)
			 for declarations = (slot-value (gethash table components) 'declarations)
			 for mapped-table = (slot-value (find-class table) 'mapped-by)
			 when declarations
			   do (loop
				for declaration in declarations
				do (push "null" declared-vars))
			 when (or param-control Mapped-table)
			   collect (process-values class param-control mapped-table) into params
			 finally (return (nconc declared-vars params)))))
      (call-statement procedure))))


(define-layered-function insert-statement (class procedure)

  (:method
      :in-layer db-interface-layer ((class serialize) (procedure procedure))
    (let* ((base-class (class-of class))
	   (components (generate-insert-components base-class))
	   (tables (include-tables class)))
      (with-slots (schema args vars sql-list sql-statement) procedure
	(setf schema (slot-value base-class 'schema))
	(let ((out-params)
	      (returns))
	  (loop
	    with num = 0
	    for table in tables
	    do (with-slots (sql declarations params)
		   (gethash table components)
		 (loop
		   for declaration in declarations
		   for var = (var-var declaration)
		   do (push var vars)
		   do (pushnew (var-param declaration) params :test #'equal)
		   do (pushnew (format nil "~a := ~a;" (var-column declaration) (car var)) returns
			       :test #'equal))
		 (cond (params
			(typecase (car params)
			  (cons
			   (loop
			     for param in params
			     do (push param args))
			   (push (format nil sql (incf num)) sql-list))
			  (atom
			   (push params args)
			   (push (format nil sql (incf num)) sql-list))))
		       (t (push sql sql-list)))))
	  (setf args (nconc (nreverse args) out-params)
		sql-list (nconc (nreverse sql-list) returns)
		vars (nreverse vars)))
	(statement procedure))))

  (:method
      :in-layer db-table-layer ((class serialize) (procedure procedure))
    (declare (ignore class))
    procedure))




(defun declared-var (table column-name col-type)
  (let ((col-type (if (eq col-type :serial) :integer col-type))
	(column (format nil "_~a" column-name)))
    (make-var
     :column column
     :var (list (format nil "_~a_~a" (db-syntax-prep table) column-name)
		(if (eq col-type :serial) :integer col-type)
		nil)
     :param (list :out column col-type))))


(defvar *components* (make-hash-table :test #'equal))

(clrhash *components*)

(declaim (notinline generate-insert-components))

(unmemoize 'generate-insert-components)


(define-layered-function generate-insert-components (class)
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
		 (generate-insert-components (find-class table))
	       (setf (gethash table-class components) component))))
      components))

  (:method
      :in db-table-layer ((class db-key-table))
    (with-slots (schema table) class
      (let* ((slot (car (filter-slots-by-type class 'db-column-slot-definition)))
	     (column (column-name slot))
	     (table-name (set-sql-name schema table))
	     (declared-var (list (declared-var table column (slot-value slot 'col-type)))))
	(values
	 (class-name class)
	 (make-component 
	  :sql (format nil "INSERT INTO ~a DEFAULT VALUES RETURNING ~a INTO ~a;"
		       table-name (set-sql-name table column) (car (var (car declared-var))))
	  :declarations declared-var)))))

	     
  (:method :in-layer db-table-layer ((class db-table-class))
    (with-slots (schema table require-columns referenced-columns) class
      (let* ((columns (mapcar #'column-name require-columns))
	     (vars (mapcar #'(lambda (column)
			       (set-sql-name table column))
			   columns))
	     (reference-vars columns)
	     (declared-vars
	       (mapcar #'(lambda (column)
			   (declared-var table (car column) (cadr column)))
		       referenced-columns)))
	(map nil #'(lambda (f-key)
		     (with-slots (key column no-join) f-key
		       (unless no-join
			 (push (db-syntax-prep key) columns)
			 (let ((reference (format nil "_~a_~a"
						  (db-syntax-prep (slot-value f-key 'table))
						  (db-syntax-prep column))))
			   ;; If there are no vars as referenced in require-columns, all vars are
			   ;; referencing declared variables; i.e. returned results from table(s) insert op
			   ;; corresponding to the foreign-keys of the current table.
			   (cond (vars
				  (push reference vars)
				  (push reference reference-vars))
				 (t
				  (push reference reference-vars)))))))
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
			   (format nil "SELECT ~{~a~^, ~} FROM UNNEST ($~~a)" reference-vars)
			   (format nil "VALUES (~{~a~^, ~})" reference-vars))
		       (mapcar #'(lambda (column)
				   (set-sql-name table (car column)))
			       referenced-columns)
		       (mapcar #'(lambda (var)
				   (car (var var)))
			       declared-vars))
	  :declarations declared-vars
	  :params (when vars
		    (list :in (format nil "~a_type[]" (set-sql-name schema table)) nil))
	  :param-controls (when vars
			    (sql-typed-array class))))))))


(memoize 'generate-insert-components :table *components*)
