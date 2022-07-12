(in-package stw.db)


(define-layered-method sync
  :in delete-node
  :around ((class serialize) component &rest rest &key)
  (call-next-method)
  (setf class nil))


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
		     (and (slot-boundp class slot-name)
			  (slot-value class slot-name))))
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
    (let ((tables (slot-value (class-of class) 'tables)))
      (loop
	for table in tables
	for required = (require-columns (find-class table))
	unless (or (typep (find-class table) 'db-key-table)
		   (and required
			(some #'(lambda (slot)
				  (let* ((slot (aif (match-mapping-node (class-of class) (find-class table))
						    (with-slots (mapping-slot mapped-column) self
						      (when (eq slot mapped-column)
							mapping-slot))
						    slot))
					 (slot-name (slot-definition-name slot)))
				    (when (slot-boundp class slot-name)
				      (slot-value class slot-name))))
			      required)))
	  collect table))))



(define-layered-function delete-node-component (table column)

  (:method
      :in delete-table ((table db-table-class) (column db-column-slot-definition))
    (with-slots (schema table) table
      (let ((table-name (set-sql-name schema table)))
	(format nil "DELETE FROM ~a WHERE ~a = $1;"
		table-name (column-name column))))))


(define-layered-method generate-component
  :in delete-table ((class db-table-class) function &key)
  (declare (ignore function))
  (with-slots (primary-keys schema table require-columns) class
    (let ((table-name (set-sql-name schema table)))
      (loop 
	for column in (filter-slots-by-type class 'db-column-slot-definition)
	for column-name = (slot-value column 'column-name)
	for domain = (slot-value column 'domain)
	for declared-var = (when (member column require-columns :test #'equality)
			     (declared-var (as-prefix table) column "delete"))
	when declared-var
	  collect declared-var into declared-vars
	  and collect (set-sql-name table (column-name column)) into returning-columns
	  and collect (car (var-var declared-var)) into vars
	  and collect (var-param declared-var) into out-args
	  and collect nil into out-values
	collect (list (set-sql-name schema domain)) into args
	collect `("~a" ,column) into p-controls
	collect (format nil "~a = $~~a" column-name) into where
	finally (return
		  (make-component
		   :sql (format nil "DELETE FROM ~a WHERE ~{~a~^ AND~% ~}~@[ RETURNING ~{~a~^, ~} INTO ~{~a~^, ~}~];"
				table-name where returning-columns vars)
		   :declarations declared-vars
		   :params `(,@args ,@out-args)
		   :param-controls `(,@p-controls ,@out-values)))))))




(define-layered-method generate-component
  :in delete-table ((map slot-mapping) function &key)
  (declare (ignore function))
  (let ((class (mapped-table map))
	(mapped-column (slot-definition-name (mapped-column map)))
	(typed-array-name (format nil "delete_~(~a~)" (slot-definition-name (mapping-slot map)))))
    (with-slots (primary-keys schema table require-columns) class
      (let ((table-name (set-sql-name schema table))
	    (type-array (format nil "~a.~a_type[]" schema table)))
	(loop 
	  for column in (filter-slots-by-type class 'db-column-slot-definition)
	  for column-name = (slot-value column 'column-name)
	  for domain = (slot-value column 'domain)
	  with require-column = nil
	  if (eq (slot-definition-name column) mapped-column)
	    do (setf require-column column-name)
	    and collect column-name into required-columns
	  else
	    if (member column require-columns :test #'equality)
	      collect column-name into required-columns
	  else
	    collect (list (set-sql-name schema domain)) into args
	    and collect `("~a" ,column) into p-controls
	    and collect column-name into where
	  finally (return
		    (make-component
		     :sql (format nil "DELETE FROM ~a WHERE ~{~a~^ AND~% ~};"
				  table-name
				  `(,@(mapcar #'(lambda (column-name)
						  (format nil "~a = $~~a" column-name))
					      where)
				    ,(format nil "~a IN (SELECT ~{~a~^, ~} FROM UNNEST ($~~a))"
					     require-column required-columns)))
		     :params `(,@args (:inout ,typed-array-name ,type-array))
		     :param-controls `(,@p-controls ,(sql-typed-array class)))))))))
