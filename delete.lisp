(in-package stw.db)



(define-layered-function delete-from (class component)

  (:method 
      :in-layer db-layer :around ((class serialize) component)
    (exec-query *db* (call-next-method))
    class)

  (:method
      :in-layer db-interface-layer ((class serialize) component)
    (let* ((schema (slot-value (class-of class) 'schema))
	   (procedure (make-instance 'procedure :schema schema)))
      (delete-dispatch-statement class procedure)))

  (:method
      :in-layer db-table-layer ((class serialize) (component db-table-class))
    (let ((procedure (memoized-funcall #'delete-table-statement component)))
      (dispatch-statement class procedure))))



(define-layered-function delete-dispatch-statement (class procedure)

  (:method
      :in-layer db-interface-layer ((class serialize) (procedure procedure))
    (with-slots (p-values) procedure
      (setf schema (slot-value (class-of class) 'schema)
	    p-values (let ((root-key (slot-value (class-of class) 'root-key)))
		       (let ((slot-name (slot-definition-name (slot-value root-key 'column))))
			 (list (slot-value class slot-name)))))
    (call-statement procedure))))



(define-layered-function delete-node-statement (class procedure &optional include-tables)
  (:documentation "Deletes value at root key, and assumes a cascade. If foreign keys are
not set to cascade on deletion, then the list include-tables exists to incorporate
tables as required. If orphaned data is desired, leave include-tables blank")

  (:method
      :in-layer db-interface-layer ((class db-interface-class) (procedure procedure) &optional include-tables)
    (with-slots (root-key) class
      (let* ((root-column (slot-value root-key 'column))
	     (col-type (let ((col-type (col-type root-column)))
			 (if (eq col-type :serial)
			     :integer
			     col-type))))
	(with-slots (schema sql-list args name) procedure
	  (setf schema (slot-value class 'schema)
		sql-list (delete-node-components class include-tables)
		args `((:in "root-key" ,col-type))
		name (if name name (format nil "~(~a~)_delete" (db-syntax-prep (class-name class)))))))
      (statement procedure))))
		


(define-layered-function delete-node-components (class &optional include-tables)

  (:method
      :in-layer db-interface-layer ((class db-interface-class) &optional include-tables)
    (with-slots (schema root-key) class
      (let ((key-table (slot-value root-key 'table))
	    (key-column (slot-value root-key 'column))
	    (statements))
	(with-active-layers (db-table-layer)
	  (when include-tables
	    (let ((manual-delete
		    (loop
		      for f-key in (slot-value key-table 'referenced-by)
		      for table = (ref-table f-key)
		      for on-delete = (on-delete f-key)
		      unless (eq on-delete :cascade)
			collect table)))
	      (when manual-delete
		(loop
		  for table in include-tables
		  for table-class = (find-class table)
		  do (push (delete-node-component table-class key-column) statements)))))
	  (push (delete-node-component key-table key-column) statements))
	statements))))


(define-layered-function delete-node-component (table column)

  (:method
      :in db-table-layer ((table db-table-class) (column db-column-slot-definition))
    (with-slots (schema table) table
      (let ((table-name (set-sql-name schema table)))
	(format nil "DELETE FROM ~a WHERE ~a = $1;"
		table-name (column-name column))))))


(define-layered-function delete-table-statement (table)

  (:method
      :in db-table-layer ((class db-table-class))
    (with-slots (primary-keys schema table) class
      (let ((table-name (set-sql-name schema table))
	    (columns)
	    (where)
	    (procedure (make-instance 'table-proc
				      :schema schema
				      :table class
				      :name (format nil "~(~a~)_delete" (db-syntax-prep (class-name class)))))
	    (array-position))
	(with-slots (args sql-list p-controls) procedure
	  (loop 
	    with i = 1
	    for column in primary-keys
	    for column-name = (slot-value column 'column-name)
	    for column-type = (slot-value column 'col-type)
	    for mapping-column = (slot-value column 'mapped-by)
	    if mapping-column
	      do (push (list :in column-name (format nil "~a[]" column-type)) args)
	      and do (push (format nil "~a IN SELECT ~a FROM UNNEST ($~a)"
				   column-name column-name i)
			   where)
		     and do (push (list "ARRAY[~{~a~^, ~}]" column) p-controls)
	    else
	      do (push (list :in column-name column-type) args)
	      and do (push `(nil ,column) p-controls)
	    and do (push (format nil "~a = $~a" column-name i) where)
	    do (incf i)
	    finally (setf args (nreverse args)
			  p-controls (nreverse p-controls)
			  where (nreverse where)))
	  (setf sql-list
		(list
		 (format nil "DELETE FROM ~a WHERE ~{~a~^ AND~% ~};" table-name where)))
	  (statement procedure))))))





;;  (:method
;;      :in db-table-layer ((table db-table-class) (columns cons))
;;    (with-slots (schema table) table
;;      (let ((table-name (set-sql-name schema table)))
;;	(format nil "DELETE FROM ~a WHERE ~{~a~^ AND~% ~};"
;;		table-name (loop
;;			     for column in columns
;;			     collect (delete-column-clause column)))))))
;;
;;
;;
;;(define-layered-function delete-using-primary-keys (column)
;;
;;  (:method
;;      :in db-table-layer ((column db-column-slot-definition))
;;    (format nil "~a = $~~a" (column-name column)))
;;
;;  (:method 
;;      :in db-table-layer ((column db-aggregate-slot-definition))
;;    (let* ((mapped-column (mapped-column (maps column)))
;;	   (col-name (column-name mapped-column)))
;;      (format nil "~a IN SELECT ~a FROM UNNEST ($~~a)" col-name col-name))))
