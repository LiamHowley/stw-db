(in-package stw.db)

(define-layered-method read-row-to-class
  :in delete-from ((class serialize))
  (declare (ignore class))
  'ignore-row-reader)

(define-layered-method error-handler
  :in delete-from ((class serialize) (procedure procedure) (err database-error) component)
  (scase (database-error-code err)
	 ("42883"
	  ;; MISSING DELETE PROCEDURE
	  ;; Response: Make procedure and recurse.
	  (make-sql-statement (class-of class) procedure)
	  (exec-query *db* (sql-statement procedure))
	  (delete-from class component))
	 (t (error err))))


(define-layered-method dispatch-statement 
  :in-layer delete-from :around ((class serialize) (procedure procedure))
  (declare (ignore class))
  (setf (slot-value procedure 'name)
	(format nil "~(~a~)_delete"
		(db-syntax-prep (class-name (class-of class)))))
  (call-next-method))

(define-layered-method dispatch-statement 
  :in-layer delete-node ((class serialize) (procedure procedure))
  (let ((root-key (slot-value (class-of class) 'root-key)))
    (let ((slot-name (slot-definition-name (slot-value root-key 'column))))
      (list (slot-value class slot-name)))))


(define-layered-method process-values
  :in-layer delete-table ((class serialize) (controls cons) mapped &optional parenthesize)
  (call-next-layered-method class controls mapped parenthesize))



(define-layered-method make-sql-statement 
  :in-layer delete-node ((class db-interface-class) (procedure procedure) &optional tables)
  "Deletes value at root key, and assumes a cascade. If foreign keys are
not set to cascade on deletion, then the list include-tables exists to incorporate
tables as required. If orphaned data is desired, leave include-tables blank"
  (with-slots (root-key) class
    (let* ((root-column (slot-value root-key 'column))
	   (col-type (let ((col-type (col-type root-column)))
		       (if (eq col-type :serial)
			   :integer
			   col-type))))
      (with-slots (schema sql-list args name) procedure
	(setf schema (slot-value class 'schema)
	      sql-list (delete-node-components class tables)
	      args `((:in "root_key" ,col-type))
	      name (if name name (format nil "~(~a~)_delete" (db-syntax-prep (class-name class)))))))
    (statement procedure)))

(define-layered-method make-sql-statement 
  :in-layer delete-table ((class db-interface-class) (procedure procedure) &optional tables)
  (declare (ignore class tables))
  procedure)
		


(define-layered-function delete-node-components (class &optional include-tables)

  (:method
      :in-layer delete-node ((class db-interface-class) &optional include-tables)
    (with-slots (schema root-key) class
      (let ((key-table (slot-value root-key 'table))
	    (key-column (slot-value root-key 'column))
	    (statements))
	(with-active-layers (delete-table)
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
      :in delete-table ((table db-table-class) (column db-column-slot-definition))
    (with-slots (schema table) table
      (let ((table-name (set-sql-name schema table)))
	(format nil "DELETE FROM ~a WHERE ~a = $1;"
		table-name (column-name column))))))


(define-layered-method generate-component
  :in delete-table ((class db-table-class))
  (with-slots (primary-keys schema table) class
    (let ((table-name (set-sql-name schema table))
	  (columns)
	  (where)
	  (procedure (make-instance 'table-proc
				    :schema schema
				    :table class
				    :name (format nil "~(~a~)_delete" (db-syntax-prep (class-name class))))))
      (with-slots (args sql-list p-controls) procedure
	(loop 
	  with i = 1
	  for column in primary-keys
	  for column-name = (slot-value column 'column-name)
	  for column-type = (slot-value column 'col-type)
	  for mapping-column = (slot-value column 'mapped-by)
	  if mapping-column
	    do (push (list :in
			   (format nil "_~a" column-name)
			   (format nil "~a[]" column-type))
		     args)
	    and do (push (format nil "~a IN (SELECT UNNEST ($~a))"
				 column-name i)
			 where)
	    and do (push (list "ARRAY[~{~a~^, ~}]" column) p-controls)
	  else
	    do (push (list :in (format nil "_~a" column-name) column-type) args)
	    and do (push `(nil ,column) p-controls)
	    and do (push (format nil "~a = $~a" column-name i) where)
	  do (incf i)
	  finally (setf args (nreverse args)
			p-controls (nreverse p-controls)
			where (nreverse where)))
	(setf sql-list
	      (list
	       (format nil "DELETE FROM ~a WHERE ~{~a~^ AND~% ~};" table-name where)))
	(statement procedure)))))
