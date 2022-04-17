(in-package stw.db)

(define-layered-method proc-template
  :in-layer delete-from ((class serialize) component)
  (declare (ignore component))
  (gethash (class-name (class-of class)) (db-template-register)))

(define-layered-method (setf proc-template)
  :in-layer delete-from ((new-value procedure) (class serialize) component)
  (declare (ignore component))
  (let ((key (class-name (class-of class))))
    (setf (gethash key (db-template-register)) new-value)))


(define-layered-method read-row-to-class
  :in delete-from ((class serialize))
  (declare (ignore class))
  'ignore-row-reader)


(define-layered-method generate-procedure
  :in-layer delete-table
  ((class serialize) (component db-table-class))
  (let ((procedure (call-next-method)))
    (setf (slot-value procedure 'name)
	  (format nil "~(~a~)_delete" 
		  (db-syntax-prep (class-name component))))
    procedure))


(define-layered-method generate-procedure
  :in-layer delete-node ((class serialize) component)
  "Deletes value at root key, and assumes a cascade. If foreign keys are
not set to cascade on deletion, then the list include-tables exists to incorporate
tables as required. If orphaned data is desired, leave include-tables blank"
  (declare (ignore component))
  (let* ((base-class (class-of class))
	 (root-key (slot-value base-class 'root-key))
	 (root-table (slot-value root-key 'table))
	 (root-column (slot-value root-key 'column))
	 (schema (slot-value base-class 'schema))
	 (col-type (let ((col-type (col-type root-column)))
		     (if (eq col-type :serial)
			 :integer
			 col-type)))
	 (procedure (make-instance 'procedure
				   :schema schema
				   :name (format nil "~a_delete" (db-syntax-prep (class-name base-class))))))
    (with-slots (schema sql-list args p-controls relevant-slots) procedure
      (setf sql-list (list (with-active-layers (delete-table)
			     (delete-node-component root-table root-column)));; tables)
	    args `((:in "root_key" ,col-type))
	    p-controls `(("~a" ,root-column))
	    relevant-slots (get-relevant-slots class procedure)))
    procedure))


(define-layered-function delete-node-component (table column)

  (:method
      :in delete-table ((table db-table-class) (column db-column-slot-definition))
    (with-slots (schema table) table
      (let ((table-name (set-sql-name schema table)))
	(format nil "DELETE FROM ~a WHERE ~a = $1;"
		table-name (column-name column))))))


(define-layered-method generate-component
  :in delete-table ((class db-table-class))
  (with-slots (primary-keys schema table) class
    (let ((table-name (set-sql-name schema table)))
      (loop 
	for column in primary-keys
	for column-name = (slot-value column 'column-name)
	for column-type = (slot-value column 'col-type)
	for mapping-column = (slot-value column 'mapped-by)
	if mapping-column
	  collect (list :in
			(format nil "delete_~a" column-name)
			(format nil "~a[]" column-type))
	    into args
	    and collect (format nil "~a IN (SELECT UNNEST ($~~a))"
				column-name)
		  into where
		  and collect (list "ARRAY[~{~a~^, ~}]" column) into p-controls
	else
	  collect (list :in (format nil "_~a" column-name) column-type) into args
	  and collect `("~a" ,column) into p-controls
	  and collect (format nil "~a = $~~a" column-name) into where
	finally (return
		  (make-component
		   :sql (format nil "DELETE FROM ~a WHERE ~{~a~^ AND~% ~};" table-name where)
		   :params args
		   :param-controls p-controls))))))
