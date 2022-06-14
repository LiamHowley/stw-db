(in-package stw.db)


(define-layered-method generate-procedure
  :in-layer insert-table
  ((class serialize) (component db-table-class) &rest rest &key)
  (declare (ignore rest))
  (let ((procedure (call-next-method)))
    (setf (slot-value procedure 'name)
	  (format nil "~(~a~)_insert" 
		  (db-syntax-prep (class-name component))))
    procedure))


(define-layered-method generate-procedure
  :in-layer insert-node ((class serialize) component &rest rest &key)
  (declare (ignore component rest))
  (let* ((base-class (class-of class))
	 (components (generate-components base-class))
	 (tables (include-tables class))
	 (schema (slot-value base-class 'schema))
	 (procedure (make-instance 'procedure
				   :schema schema
				   :name (format nil "~a_insert" (db-syntax-prep (class-name base-class))))))
    (with-slots (name args vars sql-list p-controls relevant-slots) procedure
      (setf schema (slot-value base-class 'schema))
      (let (returns)
	(loop
	  with num = 0
	  for table in tables
	  do (with-slots (sql declarations params param-controls)
		 (gethash table components)

	       ;; declared vars
	       (loop
		 for declaration in declarations
		 for var = (var-var declaration)
		 do (push var vars)
		 do (pushnew (var-param declaration) params :test #'equal)
		 do (pushnew (format nil "~a := ~a;" (var-column declaration) (car var)) returns
			     :test #'equal))
	       ;; controls
	       (loop
		 for param in param-controls
		 do (push param p-controls))

	       ;; params
	       (cond (params
			 (loop
			   for param in params
			   do (push param args)
			   do (incf num))
			 (push (format nil sql num) sql-list))
		     (t (push sql sql-list)))))
	(setf args (nreverse args)
	      p-controls (nreverse p-controls)
	      relevant-slots (get-relevant-slots class procedure)
	      sql-list (nconc (nreverse sql-list) returns)
	      vars (nreverse vars)))
      procedure)))


(define-layered-function include-tables (class))

(define-layered-method include-tables
  :in insert-node ((class serialize))
  "Included tables must either:
1. have required-columns that are bound and not null, or
2. have no required columns.
Returns a list of tables."
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
	collect table)))
				


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
	  (or (and (slot-boundp class slot-name)
		   (slot-value class slot-name))
	      (slot-value slot 'default)
	      (eq (slot-value slot 'col-type) :serial))
	  (awhen (slot-value slot 'mapped-by)
	    (loop
	      for mapping in self
		thereis (let ((slot-name (slot-definition-name (mapping-slot mapping))))
			  (and (eq (mapping-node mapping) (class-of class))
			       (slot-boundp class slot-name)
			       (slot-value class slot-name)))))))))



(defun declared-var (table column)
  (with-slots (col-type default column-name) column
    (when (or (eq col-type :serial)
	      default)
      (let ((col-type% (if (eq col-type :serial) :integer col-type))
	    (column-param (format nil "_~a" column-name)))
	(make-var
	 :column column-param
	 :var (list (format nil "_~a_~a" (db-syntax-prep table) column-name) col-type% nil)
	 :param (list :out column-param col-type%))))))



(define-layered-method generate-components
  :in insert-node ((class db-interface-class) &key)
  (let* ((tables (tables class))
	 (components (make-hash-table :test #'eq :size (length tables))))
    (loop
      for table in tables
      do (multiple-value-bind (table-class component)
	     (generate-component (find-class table)
				 #'(lambda (f-key)
				     (with-slots (ref-table no-join) f-key
				       (unless no-join
					 (member ref-table tables :test #'eq)))))
	   (setf (gethash table-class components) component)))
    components))


(define-layered-method generate-component
  :in insert-node ((class db-key-table) function &key)
  (declare (ignore function))
  (with-slots (schema table) class
    (let* ((slot (car (filter-slots-by-type class 'db-column-slot-definition)))
	   (column (column-name slot))
	   (table-name (set-sql-name schema table))
	   (declared-var (list (declared-var table slot))))
      (values
       (class-name class)
       (make-component 
	:sql (format nil "INSERT INTO ~a DEFAULT VALUES RETURNING ~a INTO ~a;"
		     table-name (set-sql-name table column) (car (var-var (car declared-var))))
	:declarations declared-var
	:param-controls (list nil))))))


(define-layered-method generate-component
  :in-layer insert-node ((class db-table-class) (f-key-p function) &key)
  (with-slots (schema table require-columns) class
    (let* ((columns (mapcar #'column-name require-columns))
	   (vars (mapcar #'(lambda (column)
			     (set-sql-name table column))
			 columns))
	   (reference-vars columns)
	   (returning-columns)
	   (declared-vars))

      ;; The slots that return values must either have
      ;; an autogenerated value as in those with col-type
      ;; :serial, or a default value.
      (loop
	for column in (filter-slots-by-type class 'db-column-slot-definition)
	for declared-var = (declared-var table column)
	when declared-var
	  collect declared-var into declared-vars%
	  and collect column into returning-columns%
	finally (setf declared-vars declared-vars%
		      returning-columns returning-columns%))

      (map nil #'(lambda (f-key)
		   (when (funcall f-key-p f-key)
		     (with-slots (key column) f-key
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
      ;; 4. control strings to build the argument array
      ;; 5. pass nil for each returning value. This will be used above to pass
      ;; 'null' for each out param.
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
				 (set-sql-name table (column-name column)))
			     returning-columns)
		     (mapcar #'(lambda (var)
				 (car (var-var var)))
			     declared-vars))
	:declarations declared-vars
	:params (when vars
		  `((:in ,(format nil "~a_type[]" (set-sql-name schema table)) nil)))
	:param-controls (cond (vars
			       (nconc
				(when returning-columns
				  (mapcar (constantly nil) returning-columns))
				(list (when vars
					(sql-typed-array class)))))
			      (returning-columns
			       (mapcar (constantly nil) returning-columns))
			      (t (list nil))))))))


(define-layered-method generate-component
  :in-layer insert-table ((class db-table-class) function &key mapping-node)
  (declare (ignore function))
  (with-slots (schema table require-columns) class
    (let ((typed-array-name (or (when mapping-node
				  (format nil "insert_~(~a~)" (slot-definition-name (mapping-slot mapping-node))))
				"insert_array"))
	  (type-array (format nil "~a.~a_type[]" schema table)))
      (loop
	for slot in (filter-slots-by-type class 'db-column-slot-definition)
	for column-name = (slot-value slot 'column-name)
	for column-type = (slot-value slot 'col-type)
	if (member slot require-columns :test #'equality)
	  collect column-name into required-columns
	else
	  collect (list :in (format nil "insert_~a" column-name) column-type) into args
	  and collect "$~a" into select
	  and collect `("~a" ,slot) into p-controls
	collect column-name into columns
	finally (return
		  (make-component
		   :sql (format nil
				"INSERT INTO ~a (~{~a~^, ~}) SELECT ~{~a~^, ~} from unnest($~~a);"
				(set-sql-name schema table)
				columns
				(nconc select required-columns))
		   :params `(,@args (:inout ,typed-array-name ,type-array))
		   :param-controls `(,@p-controls ,(sql-typed-array class))))))))




(define-layered-method generate-component
  :in insert-table ((class db-key-table) function &key)
  (declare (ignore function))
  (nth-value 1 (generate-components class)))
