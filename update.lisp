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
		  (amended (substitute #\- #\_ field))
		  (symbol-name (string-upcase (or (when pos (subseq amended (1+ pos)))
						  amended)))
		  (slot-name (intern symbol-name (symbol-package (class-name base-class)))))
	     (scase (subseq field 0 pos)
		    ("insert"
		     (awhen (find-slot-definition base-class slot-name 'db-aggregate-slot-definition)
		       (let ((list (explode-string next-field '("{(" "),(" ")(" ")}" "),\"(\\\"" "\\\")\"}")
						   :remove-separators t)))
			 (loop
			   for value in list
			   do (case (slot-definition-type self)
				(array
				 (vector-push-extend value (slot-value class slot-name)))
				(t
				 (push value (slot-value class slot-name))))))))
		    ("delete"
		     (awhen (find-slot-definition base-class slot-name 'db-base-column-definition)
		       (etypecase self
			 (db-aggregate-slot-definition
			  (let ((list (explode-string next-field '("{(" "),(" ")(" ")}" "),\"(\\\"" "\\\")\"}")
						      :remove-separators t)))
			    (loop
			      for value in list
			      do (setf (slot-value class slot-name)
				       (remove value (slot-value class slot-name) :test #'equal)))))
			 (db-column-slot-definition
			  (when (equal next-field (slot-value class slot-name))
			    (setf (slot-value class slot-name) nil))))))
		    (t
		     (awhen (find-slot-definition base-class slot-name 'db-column-slot-definition)
		       (setf (slot-value class slot-name)
			     (if (stringp next-field)
				 (string-trim '(#\") next-field)
				 next-field))))))))
    (row-reader (fields)
      (loop
	while (next-row)
	do (loop
	     for field across fields
	     do (process-fields (field-name field) (next-field field)))
	finally (return class)))))



(define-layered-method dispatcher
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
    (multiple-value-bind (tables where set to-insert to-delete)
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
			   finally (setf args `(,@args ,@final-params))))
		       (assign-slots (prefix controls)
			 (loop
			   for (control slot) in controls
			   if (consp slot)
			     collect `(,prefix ,@slot) into relevant-slots%
			   else when slot
				  collect `(,prefix ,slot) into relevant-slots%
			   finally (setf relevant-slots `(,@relevant-slots ,@relevant-slots%))))
		       (process-many-to-one-component (prefix component)
			 (with-slots (params sql declarations param-controls) component
			   (build-procedure params)
			   (assign-slots prefix param-controls)
			   (push (apply #'format nil sql (number-range open close)) sql-list)
			   (setf open close
				 p-controls `(,@p-controls ,@param-controls))
			   (when declarations
			     (loop
			       for declaration in declarations
			       for var = (var-var declaration)
			       collect var into vars%
			       collect (format nil "~a := ~a;" (var-column declaration) (car var)) into returns
			       finally (setf vars `(,@vars ,@vars%)
					     sql-list (nconc returns sql-list)))))))
		(loop
		  for table in tables
		  for component = (gethash table components)
		  do (etypecase component
		       (update-component
			(with-slots (sql set-params where-params set-controls where-controls) component
			  (build-procedure set-params)
			  (build-procedure where-params)
			  (assign-slots :new set-controls) 
			  (assign-slots :old where-controls) 
			  (push (apply #'format nil sql (number-range open close)) sql-list)
			  (setf open close
				p-controls `(,@p-controls ,@set-controls ,@where-controls))))
		       (cons
			(loop
			  for (prefix compo) on component by #'cddr
			  do (process-many-to-one-component prefix compo)))))
		(when to-insert
		  (loop
		    for table in to-insert
		    for component = (with-active-layers (insert-table)
				      (generate-component table nil))
		    do (process-many-to-one-component :insert component)))
		(when to-delete
		  (loop
		    for table in to-delete
		    for component = (with-active-layers (delete-table)
				      (generate-component table nil))
		    do (process-many-to-one-component :delete component))))
	      (setf sql-list (nreverse (push "RETURN;" sql-list))))))))
    procedure))



(define-layered-function one-to-one-update-components (old new)
  (:documentation "Build components based on slots of type 
db-column-slot-definition.")

  (:method
      :in update-node ((old serialize) (new serialize))
    (let ((base-class (class-of old)))
      (loop
	with to-insert = nil
	with to-delete = nil
	for slot in (filter-slots-by-type base-class 'db-column-slot-definition)
	for slot-name = (slot-definition-name slot)
	for old-boundp = (unless (lock-value slot)
			   (slot-boundp old slot-name))
	for new-boundp = (unless (lock-value slot)
			   (slot-boundp new slot-name))
	for old-value = (when old-boundp
			  (slot-value old slot-name))
	for new-value = (when new-boundp
			    (slot-value new slot-name))
	for exceptionp = (lambda (slot)
			   (with-slots (col-type not-null) slot
			     (or (eq col-type :boolean)
				 (eq not-null nil))))
	when old-value
	  collect (cons slot old-value) into where
	  and unless (and new-boundp
			  (or new-value
			      (funcall exceptionp slot)))
	       do (pushnew (slot-value slot 'table-class) to-delete :test #'eq)
	when new-value
	  if (not old-boundp)
	       do (pushnew (slot-value slot 'table-class) to-insert :test #'eq)
	else if (and (null old-value)
		     (unless (funcall exceptionp slot)
		       t))
	       do (pushnew (slot-value slot 'table-class) to-insert :test #'eq)
	else
	  unless (equal new-value old-value)
	    collect (slot-value slot 'table-class) into tables
	    and collect (cons slot new-value) into set
	finally (return (values tables where set to-insert to-delete))))))


(define-layered-function one-to-many-update-components (old new)
  (:documentation "Build components based on slots of type 
db-aggregate-slot-definition.")

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
	collect :insert into components%
	and collect (with-active-layers (insert-table)
		      (generate-component maps nil))
	      into components%
      when to-delete
	collect :delete into components%
	and collect (with-active-layers (delete-table)
		      (generate-component maps nil))
	      into components%
      finally (when (or to-insert to-delete)
		(return (values mapped-table components%))))))


(define-layered-function update-op-dispatch-statement (old new procedure)
  (:documentation "Build dispatching statement from list of relevant-slots
and the control string p-control. Values are obtained from (old serialize) and 
it's clone (new serialize). Applies only to update-node context.")

  (:method
      :in update-node ((old serialize) (new serialize) (procedure procedure))
    (with-slots (p-control relevant-slots) procedure
      (apply #'format nil p-control
	     (loop
	       for (symbol slot) in relevant-slots
	       for mapped = (match-mapping-node (class-of old) slot)
	       if mapped
		 collect (let* ((slot-name (slot-definition-name (mapping-slot mapped)))
				(old-values (slot-value old slot-name))
				(new-values (slot-value new slot-name))
				(column (mapped-column mapped)))
			   (if (eq symbol :insert)
			       (loop
				 for value in (set-difference new-values old-values :test #'equal)
				 collect (prepare-value% column value t))
			       (loop
				 for value in (set-difference old-values new-values :test #'equal)
				 collect (prepare-value% column value))))
	       else 
		 collect (prepare-value% slot
					 (slot-value (if (or (eq symbol :old)
							     (eq symbol :delete))
							 old
							 new)
						     (slot-definition-name slot))
					 (when (eq symbol :insert)
					   t)))))))



(define-layered-function update-components (tables where set)

  (:method
      :in-layer update (tables where set)
    nil)

  (:method
      :in-layer update-node (tables where set)
    (let ((components (make-hash-table :test #'eq :size (length tables))))
      (loop
	for table in tables
	for component = (generate-component
			 table
			 #'(lambda (slot)
			     (let* ((slot-name (slot-definition-name slot))
				    (where (member slot-name (mapcar #'slot-definition-name where) :test #'eq))
				    (set (member slot-name (mapcar #'slot-definition-name set) :test #'eq)))
			       (cond ((and where set) t)
				     (where :where)
				     (set :set)))))
	when component
	  do (setf (gethash table components) component))
      components)))



(defstruct (update-component (:conc-name nil))
  (sql nil :type string)
  set-params
  where-params
  set-controls
  where-controls)


(define-layered-method generate-component
  :in-layer update-node ((class db-table-class) (allocate function) &key)
  (with-slots (schema table) class
    (loop
      for slot in (filter-slots-by-type class 'db-base-column-definition)
      for allocation = (funcall allocate slot)
      when (or (eq allocation :set)
	       (eq allocation t))
	collect (column-name slot) into set-columns
	and collect (update-param slot "set") into set-params
	and collect (update-param-control slot) into set-controls
      when (or (eq allocation :where)
	       (eq allocation t))
	collect (column-name slot) into where-columns
	and collect (update-param slot "where") into where-params
	and collect (update-param-control slot) into where-controls
      finally (when set-columns
		(return
		  (make-update-component 
		   :sql (format nil
				"UPDATE ~a SET ~{~a = $~~a~^, ~} WHERE ~{~a = $~~a~^ AND ~};"
				(set-sql-name schema table) set-columns where-columns)
		   :set-params set-params
		   :where-params where-params
		   :set-controls set-controls
		   :where-controls where-controls))))))



(define-layered-function update-param (slot prefix)
  (:method
      :in update ((slot db-column-slot-definition) prefix)
    (with-slots (schema column-name domain) slot
      (if (string= prefix "set")
	  (list :inout (format nil "~a_~a" prefix column-name) (set-sql-name schema domain))
	  (list (set-sql-name schema domain))))))


(define-layered-function update-param-control (slot)
  (:method
      :in update ((slot db-column-slot-definition))
    (list (format nil "~~a::~a"
		  (set-sql-name (slot-value slot 'schema) (slot-value slot 'domain)))
	  slot)))
