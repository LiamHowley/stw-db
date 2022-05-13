(in-package stw.db)


(define-layered-method get-key
  :in update-node ((old serialize) (new serialize) &rest rest &key)
  (declare (ignore rest))
  (loop
    for slot in (filter-slots-by-type (class-of old) 'db-base-column-definition) 
    for slot-name = (slot-definition-name slot)
    for old-value = (when (slot-boundp old slot-name)
		      (slot-value old slot-name))
    for new-value = (when (slot-boundp new slot-name)
		      (slot-value new slot-name))
    when old-value
      collect slot-name into where 
    when new-value
      unless (equal new-value old-value)
	collect slot-name into set 
    finally (return (list set where))))
      


(define-layered-method read-row-to-class 
  :in-layer update-node ((class serialize))
  (flet ((process-fields (field next-field)
	   (let* ((base-class (class-of class))
		  (pos (search "_" field))
		  (symbol-name (string-upcase (substitute #\- #\_ (subseq field (1+ pos)))))
		  (slot-name (intern symbol-name (symbol-package (class-name base-class)))))
	     (scase (subseq field 0 pos)
		    ("set"
		     (when (find-slot-definition base-class slot-name 'db-column-slot-definition)
		       (setf (slot-value class slot-name) next-field)))
		    ("insert"
		     (awhen (find-slot-definition base-class slot-name 'db-aggregate-slot-definition)
		       (let ((list (explode-string next-field '("{(" "),(" ")}") :remove-separators t)))
			 (loop
			   for value in list
			   do (case (slot-definition-type self)
				(array
				 (vector-push-extend value (slot-value class slot-name)))
				(t
				 (push value (slot-value class slot-name))))))))
		    ("delete"
		     (awhen (find-slot-definition base-class slot-name 'db-aggregate-slot-definition)
		       (loop
			 for value across next-field
			 do (setf (slot-value class slot-name)
				  (remove value (slot-value class slot-name) :test #'equal)))))))))
    (row-reader (fields)
      (loop
	while (next-row)
	do (loop
	     for field across fields
	     do (process-fields (field-name field) (next-field field)))
	finally (return class)))))


(define-layered-method dispatch
  :in update-node ((old serialize) (new serialize) (procedure procedure))
  `(update-op-dispatch-statement ,old ,new ,procedure))


(define-layered-method generate-procedure
  :in-layer update-node ((old serialize) (new serialize) &rest rest &key)
  (declare (ignore rest))
  (let* ((base-class (class-of old))
	 (procedure (make-instance 'procedure
				   :schema (slot-value base-class 'schema)
				   :table (class-name base-class)
				   :name (format nil "~(~a~)_update" (db-syntax-prep (class-name base-class)))))
	 (root-key (slot-value base-class 'root-key))
	 (root-column (slot-definition-name (slot-value root-key 'column)))
	 (old-root-value (slot-value old root-column))
	 (new-root-value (slot-value new root-column)))
    (unless (equal old-root-value new-root-value)
      (error "The root-key ~a has values ~a and ~a that do not match. Root key column values must match for update to proceed." root-column old-root-value new-root-value))
    (multiple-value-bind (tables where set)
	(one-to-one-update-components old new)
      (let ((components (update-components tables (mapcar #'car where) (mapcar #'car set))))
	(multiple-value-bind (mapped-table components%)
	    (one-to-many-update-components old new)
	  (setf (gethash mapped-table components) components%)
	  (when mapped-table
	    (setf tables (nconc tables (list mapped-table))))
	  (with-slots (args vars sql-list p-controls sql-statement relevant-slots) procedure
	    (let* ((open 1)
		   (close open))
	      (labels ((build-procedure (params)
			 (loop
			   for param in params
			   do (incf close)
			   collect param into final-params
			   finally (setf args (nconc args final-params))))
		       (assign-slots (preface controls)
			 (loop
			   for (control slot) in controls
			   do (push (list preface slot) relevant-slots)))
		       (process-agg-component (preface component)
			 (with-slots (params sql param-controls) component
			   (build-procedure params)
			   (push (apply #'format nil sql (number-range open close)) sql-list)
			   (setf open close)
			   (push param-controls p-controls)
			   (map-tree-depth-first
			    #'(lambda (slot)
				(when (typep slot 'db-column-slot-definition)
				  (aif (match-mapping-node base-class slot)
				       (push (list preface (slot-value self 'mapping-slot)) relevant-slots)
				       (push (list old slot) relevant-slots))))
			    param-controls))))
		(loop
		  for table in tables
		  for component = (gethash table components)
		  for mapped-table = (match-mapping-node base-class table)
		  do (etypecase component
		       (update-component
			(with-slots (sql set-params where-params set-controls where-controls) component
			  (build-procedure set-params)
			  (build-procedure where-params)
			  (push set-controls p-controls)
			  (push where-controls p-controls)
			  (assign-slots :new set-controls) 
			  (assign-slots :old where-controls) 
			  (push (apply #'format nil sql (number-range open close)) sql-list)
			  (setf open close)))
		       (cons
			(destructuring-bind (insert delete) component
			  (process-agg-component :insert insert)
			  (process-agg-component :delete delete))))))
	      (setf sql-list (nreverse (push "RETURN;" sql-list))
		    p-controls (nreverse p-controls)
		    relevant-slots (nreverse relevant-slots)))))))
    procedure))



(define-layered-function one-to-one-update-components (old new)

  (:method
      :in update-node ((old serialize) (new serialize))
    (let* ((base-class (class-of old))
	   (root-key-column (column (slot-value base-class 'root-key))))
      (loop
	for slot in (filter-slots-by-type base-class 'db-column-slot-definition)
	for slot-name = (slot-definition-name slot)
	for old-value = (when (slot-boundp old slot-name)
			  (slot-value old slot-name))
	for new-value = (when (slot-boundp new slot-name)
			  (slot-value new slot-name))
	when old-value
	  collect (cons slot old-value) into where
	when new-value
	  unless (equal new-value old-value)
	    collect (slot-value slot 'table-class) into tables
	    and collect (cons slot new-value) into set
	finally (return (values tables where set))))))


(define-layered-function one-to-many-update-components (old new)
  (:method
      :in update-node ((old serialize) (new serialize))
    (loop
      for slot in (filter-slots-by-type (class-of old) 'db-aggregate-slot-definition)
      for slot-name = (slot-definition-name slot)
      for old-values = (when (slot-boundp old slot-name)
			 (slot-value old slot-name))
      for new-values = (when (slot-boundp new slot-name)
			 (slot-value new slot-name))
      for to-delete = (set-difference old-values new-values :test #'equal)
      for to-insert = (set-difference new-values old-values :test #'equal)
      for maps = (slot-value slot 'maps)
      for mapped-table = (mapped-table maps)
      when to-insert
	collect (with-active-layers (insert-table)
		  (generate-component mapped-table nil
				      :mapping-column (slot-definition-name slot)))
	  into components%
      when to-delete
	collect (with-active-layers (delete-table)
		  (generate-component mapped-table nil
				      :mapping-node maps))
	  into components%
      finally (when (or to-insert to-delete)
		(return (values mapped-table components%))))))


(define-layered-function update-op-dispatch-statement (old new procedure)

  (:method
      :in update-node ((old serialize) (new serialize) (procedure procedure))
    (with-slots (p-control relevant-slots) procedure
      (apply #'format nil p-control
	     (loop
	       for (symbol slot) in relevant-slots
	       for slot-name = (slot-definition-name slot)
	       if (typep slot 'db-column-slot-definition)
		 collect (prepare-value% slot (slot-value (if (eq symbol :old) old new) slot-name))
	       else 
		 collect (let* ((old-values (slot-value old slot-name))
				(new-values (slot-value new slot-name))
				(column (mapped-column (slot-value slot 'maps))))
			   (if (eq symbol :insert)
			       (loop
				 for value in (set-difference new-values old-values :test #'equal)
				 collect (prepare-value% column value t))
			       (loop
				 for value in (set-difference old-values new-values :test #'equal)
				 collect (prepare-value% column value)))))))))
				
		 
	       
(define-layered-function update-components (tables where set)

  (:method
      :in-layer update (tables where set)
    nil)

  (:method
      :in-layer update-node (tables where set)
    (let ((components (make-hash-table :test #'eq :size (length tables))))
      (loop
	for table in tables
	for slots = (filter-slots-by-type table 'db-base-column-definition)
	for component = (update-component table 
					  (remove-if-not #'(lambda (slot)
							     (eq table (slot-value slot 'table-class)))
							 where)
					  (remove-if-not #'(lambda (slot)
							     (eq table (slot-value slot 'table-class)))
							 set))
	when component
	  do (setf (gethash table components) component))
      components)))



(defstruct (update-component (:conc-name nil))
  (sql nil :type string)
  set-params
  where-params
  set-controls
  where-controls)



(define-layered-function update-component (class where set)

  (:method
      :in-layer update (table where set)
    nil)

  (:method
      :in-layer update ((class db-table-class) (where cons) (set cons))
    (with-slots (schema table) class
      (let* ((set-columns (mapcar #'column-name set))
	     (where-columns (mapcar #'column-name where)))
	(make-update-component 
	 :sql (format nil
		      "UPDATE ~a SET ~{~a = $~~a~^, ~} WHERE ~{~a = $~~a~^ AND ~};"
		      (set-sql-name schema table)
		      set-columns
		      where-columns)
	 :set-params (mapcar #'(lambda (slot)
				 (list :inout (format nil "set_~a" (column-name slot)) (set-sql-name schema (domain slot))))
			     set)
	 :where-params (mapcar #'(lambda (slot)
				   (list :in (format nil "where_~a" (column-name slot)) (set-sql-name schema (domain slot))))
			       where)
	 :set-controls (update-param-controls set)
	 :where-controls (update-param-controls where))))))


(defun update-param-controls (list)
  (loop
    for slot in list
    collect (list (format nil "~~a::~a"
			  (set-sql-name (slot-value slot 'schema) (slot-value slot 'domain)))
		  slot)))
