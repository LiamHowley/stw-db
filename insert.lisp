(in-package stw.db)


(define-layered-function insert-statement (class)

  (:method
      :in-layer db-interface-layer ((class serialize))
    (let* ((base-class (class-of class))
	   (relevant-args (object-to-plist class
					   :use-placeholders t
					   :recurse nil
					   :package (symbol-package (class-name base-class))))
	   (procedure (memoized-funcall #'insert-build-procedure base-class (cdr relevant-args))))
      (values
       (sql-statement procedure)
       (when (p-values procedure)
	 (call-insert-statement class procedure))))))


(define-layered-function call-insert-statement (class procedure)
  (:method
      :in db-layer ((class serialize) (procedure procedure))
    (with-slots (name p-values) procedure
      (format nil "CALL ~a (~@[~{~a~^, ~}~])"
	      name (mapcar #'(lambda (p-value)
			       (destructuring-bind (control slots) p-value
				 (apply #'format nil control
					(loop
					  for slot in slots
					  for slot-name = (slot-definition-name slot)
					  collect (prepare-value (col-type slot) (slot-value class slot-name))))))
			   p-values)))))


(define-layered-function slot-to-go (slot args)
  (:documentation "A column that is set to NOT NULL, has neither a DEFAULT set 
nor a derived value from a FOREIGN KEY, nor is of type serial, must have a supplied
value, otherwise an error will be thrown upon inserting. Returns all slots with
their respective values or nil.")
  (:method
      :in db-layer ((slot db-column-slot-definition) args)
    (or (getf args (car (slot-definition-initargs slot)))
	(let ((default (slot-value slot 'default)))
	  (or default
	      (and default (eq (slot-value slot 'col-type) :boolean)))))))


(define-layered-function insert-build-procedure (class relevant-args)

  (:method
      :in-layer db-interface-layer ((class db-interface-class) relevant-args)
    (let ((procedure (make-instance 'procedure
				    :name (format nil "~(~a~)_insert" (class-name class))))
	  (components (memoized-funcall #'insert-components class))
	  (tables (slot-value class 'tables)))
      (with-slots (args vars sql-list p-values sql-statement) procedure
	(loop
	  with num = 0
	  for table in tables
	  for required = (require-columns (find-class table))
	  for include-table = (cond ((and required
					  (loop
					    for slot in required
					    always (slot-to-go slot relevant-args)))
				     t)
				    (required nil)
				    (t t))
	  when include-table
	    do (destructuring-bind (sql declarations &optional array-type)
		   (gethash table components)
		 (loop
		   for declaration in declarations
		   do (push declaration vars))
		 (cond (array-type
			(push array-type args)
			(push (format nil sql (incf num)) sql-list)
			(push (sql-typed-array (find-class table)) p-values))
		       (t (push sql sql-list)))))
	(setf args (nreverse args)
	      sql-list (nreverse sql-list)
	      vars (nreverse vars)
	      p-values (nreverse p-values))
	(statement procedure)))))


(define-layered-function insert-components (class)

  (:method
      :in db-interface-layer ((class db-interface-class))
    (let* ((tables (tables class))
	   (components (make-hash-table :test #'eq :size (length tables))))
      (with-active-layers (db-table-layer)
	(loop
	  for table in tables
	  do (multiple-value-bind (table-class accessories)
		 (insert-components (find-class table))
	       (setf (gethash table-class components) accessories))))
      components))

  (:method
      :in db-table-layer ((class db-key-table))
    (with-slots (schema table) class
      (let* ((slot (car (filter-slots-by-type class 'db-column-slot-definition)))
	     (column (column-name slot))
	     (table-name (set-sql-name schema table))
	     (var (list (format nil "_~a_~a" (db-syntax-prep table) column) (col-type slot) nil)))
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
				(cadr column)
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
