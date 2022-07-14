(in-package stw.db)


(define-layered-method sync
  :in delete-node
  :around ((class serialize) component &rest rest &key)
  (declare (ignore rest))
  (let* ((root-table (car (slot-value (class-of class) 'tables)))
	(primary-keys (slot-value (find-class root-table) 'primary-keys)))
    (loop
      for slot in primary-keys
      for slot-name = (slot-definition-name slot)
      for slot-value-p = (slot-value class slot-name)
      unless slot-value-p
      do (restart-case (null-key-error slot-name class)
	   (not-an-error ()
	     :report (lambda (s)
		       (write-string "This is not an error. Please continue" s))
	     nil)))
    (call-next-method)))


(define-layered-method escape
  :in delete-node ((slots cons))
  nil)


(define-layered-method generate-procedure
  :in-layer delete-table
  ((class serialize) (component db-table-class) &rest rest &key)
  (declare (ignore rest))
  (let ((procedure (call-next-method)))
    (setf (slot-value procedure 'name)
	  (format nil "~(~a~)_delete" 
		  (db-syntax-prep (class-name component))))
    procedure))



(define-layered-method generate-procedure
  :in-layer delete-node ((class serialize) component &rest rest &key)
  "Deletes value in root table, and assumes a cascade. If foreign keys are
not set to cascade on deletion, then data will be orphaned."
  (declare (ignore component rest))
  (let* ((base-class (class-of class))
	 (root-table (find-class (car (slot-value base-class 'tables))))
	 (root-columns (slot-value root-table 'primary-keys))
	 (schema (slot-value base-class 'schema))
	 (procedure (make-instance 'procedure
				   :schema schema
				   :name (format nil "~a_delete" (db-syntax-prep (class-name base-class))))))
    (with-slots (schema sql-list args p-controls relevant-slots) procedure
      (if (every (lambda (slot)
		   (let ((slot-name (slot-definition-name slot)))
			  (slot-value class slot-name)))
		 root-columns)
	  (loop
	    for column in root-columns
	    collect (list (set-sql-name schema (slot-value column 'domain))) into args%
	    collect `("~a" ,column) into p-controls%
	    finally (setf sql-list (list (with-active-layers (delete-table)
					   (delete-node-component root-table root-columns)))
			  args args%
			  p-controls p-controls%
			  relevant-slots (get-relevant-slots class procedure)))
	  (let* ((ignore-tables (ignore-tables class))
		 (select-proc
		   (with-active-layers (retrieve-node)
		     (generate-procedure class nil
					 :select-columns root-columns
					 :ignore-tables ignore-tables)))
		 (table-name (table root-table))
		 (column-names (mapcar #'column-name root-columns)))
	    (setf sql-list (list (format nil "DELETE FROM ~a.~a WHERE (~{~a~^, ~}) IN (~a);"
					 schema table-name
					 column-names
					 (string-right-trim '(#\;) (sql-query select-proc))))
		  args (args select-proc)
		  p-controls (p-controls select-proc)
		  relevant-slots (get-relevant-slots class procedure)))))
      procedure))


(define-layered-function ignore-tables (class)
  (:documentation "Returns list of tables where required columns
have no value.")

  (:method
      :in delete-node ((class serialize))
    (let ((tables%)
	  (tables (slot-value (class-of class) 'tables)))
      (labels ((recurse-slots (slots)
		 (when slots
		   (let* ((slot (car slots))
			  (slot-name (slot-definition-name slot)))
		     (when (or (slot-value class slot-name)
			       (eq (slot-value slot 'col-type) :boolean))
		       (pushnew (class-name
				 (typecase slot
				   (db-column-slot-definition
				    (slot-value slot 'table-class)) 
				   (db-aggregate-slot-definition
				    (mapped-table (slot-value slot 'maps)))))
				tables%
				:test #'eq)))
		   (recurse-slots (cdr slots)))))
	(recurse-slots (filter-slots-by-type (class-of class) 'db-base-column-definition)))
      (set-difference (cdr tables) tables% :test #'eq))))



(define-layered-function delete-node-component (table columns)

  (:method
      :in delete-table ((table db-table-class) (columns cons))
    (with-slots (schema table) table
      (let ((table-name (set-sql-name schema table))
	    (columns% (loop
		       for count from 1
		       for column in columns
		       collect (list (column-name column) count))))
	(format nil "DELETE FROM ~a WHERE ~{~{~a = $~a~}~^ AND~};"
		table-name columns%)))))



(define-layered-method generate-component
  :in delete-table ((class db-table-class) (slot-to-go-p function) &key)
  (with-slots (schema table require-columns primary-keys) class
    (let ((table-name (set-sql-name schema table))
	  (single-row (loop
			for slot in primary-keys
			always (funcall slot-to-go-p slot))))
      (loop 
	for column in (filter-slots-by-type class 'db-column-slot-definition)
	for column-name = (slot-value column 'column-name)
	for domain = (slot-value column 'domain)
	for declared-var = (when (and single-row
				      (member column require-columns :test #'equality))
			     (declared-var (as-prefix table) column "delete"))
	for slot-value-p = (funcall slot-to-go-p column)
	when (and slot-value-p declared-var)
	  collect declared-var into declared-vars
	  and collect (set-sql-name table (column-name column)) into returning-columns
	  and collect (car (var-var declared-var)) into vars
	  and collect (var-param declared-var) into out-args
	  and collect nil into out-values
	when slot-value-p
	  collect (list (set-sql-name schema domain)) into args
	  and collect `("~a" ,column) into p-controls
	  and collect (format nil "~a = $~~a" column-name) into where
	finally (return
		  (when where
		    (make-component
		     :sql (format nil "DELETE FROM ~a WHERE ~{~a~^ AND~% ~}~@[ RETURNING ~{~a~^, ~} INTO ~{~a~^, ~}~];"
				  table-name where returning-columns vars)
		     :declarations declared-vars
		     :params `(,@args ,@out-args)
		     :param-controls `(,@p-controls ,@out-values))))))))




(define-layered-method generate-component
  :in delete-table ((map slot-mapping) (slot-to-go-p function)  &key)
  (let ((class (mapped-table map))
	(mapped-column (slot-definition-name (mapped-column map)))
	(typed-array-name (format nil "delete_~(~a~)" (slot-definition-name (mapping-slot map)))))
    (with-slots (schema table require-columns primary-keys) class
      (let ((table-name (set-sql-name schema table))
	    (type-array (format nil "~a.~a_type[]" schema table))
	    (single-row (loop
			  for slot in primary-keys
			  always (funcall slot-to-go-p slot))))
	(loop 
	  for column in (filter-slots-by-type class 'db-column-slot-definition)
	  for column-name = (slot-value column 'column-name)
	  for domain = (slot-value column 'domain)
	  for slot-value-p = (funcall slot-to-go-p column)
	  with require-column = nil
	  if (and (eq (slot-definition-name column) mapped-column)
		  slot-value-p)
	    do (setf require-column column-name)
	    and collect column-name into required-columns
	  else
	    if (and (member column require-columns :test #'equality)
		    slot-value-p)
	      collect column-name into required-columns
	  else
	    if slot-value-p
	      collect (list (set-sql-name schema domain)) into args
	      and collect `("~a" ,column) into p-controls
	      and collect column-name into where
	  finally (return
		    (when (or where require-column)
		      (make-component
		       :sql (format nil "DELETE FROM ~a WHERE ~{~a~^ AND~% ~};"
				    table-name
				    `(,@(mapcar #'(lambda (column-name)
						    (format nil "~a = $~~a" column-name))
						where)
				      ,(format nil "~a IN (SELECT ~{~a~^, ~} FROM UNNEST ($~~a))"
					       require-column required-columns)))
		       :params `(,@args (,(if single-row :inout :in) ,typed-array-name ,type-array))
		       :param-controls `(,@p-controls ,(sql-typed-array class))))))))))
