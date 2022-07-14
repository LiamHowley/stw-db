(in-package stw.db)


(define-layered-method get-key
  :in update-node ((old serialize) (new serialize) &rest rest &key)
  (declare (ignore rest))
  (loop
    for slot in (filter-slots-by-type (class-of old) 'db-base-column-definition) 
    for slot-name = (slot-definition-name slot)
    for old-value = (slot-value old slot-name)
    for new-value = (slot-value new slot-name)
    when old-value
      collect slot-name into where 
    when new-value
      collect slot-name into set 
    finally (return (list set where))))


(define-layered-method sync
  :in update-node ((old serialize) (new serialize) &rest rest)

  ;; Primary keys of the primary
  ;; table in tables must match.
  (or (match-primary-keys old new)
      (call-next-method)))


(define-layered-function match-primary-keys (old new)
  (:documentation "Selects the primary keys of the first table in tables 
and matches the values of each primary key in both old and new. A nil value
or primary keys not matching will invoke an error.")

  (:method
      :in-layer update ((old serialize) (new serialize))
    (let* ((key-table (car (slot-value (class-of old) 'tables)))
	   (primary-keys (slot-value (find-class key-table) 'primary-keys)))
      (loop
	for slot in primary-keys
	for slot-name = (slot-definition-name slot)
	for new-value = (slot-value new slot-name)
	do (aif (slot-value old slot-name)
		(flet ((update-value-error ()
			 (restart-case (update-key-value-error self new-value)
			   (use-expected-value ()
			     :report (lambda (s)
				       (format s "Use expected value: ~a" self))
			     (setf (slot-value new slot-name) self)))))
		  (if new-value
		      (unless (equal self new-value)
			(update-value-error))
		      (update-value-error)))
		(restart-case (null-key-error slot-name old)
		  (not-an-error ()
		    :report (lambda (s)
			      (write-string "This is not an error. Please continue" s))
		    :test (lambda (c)
			    (declare (ignore c))
			    (unless new-value
			      t))
		    nil)))))))



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
		       (let ((list (explode-string next-field
						   '("{\"(\\\"" "\\\")\",(" "\\\")" "{(" "),(" ")(" ")}" "),\"(\\\"" "\\\")\"}")
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
			  (let ((list (explode-string next-field
						      '("{\"(\\\"" "\\\")\",(" "\\\")" "{(" "),(" ")(" ")}" "),\"(\\\"" "\\\")\"}")
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
				   :name (format nil "~(~a~)_update" (db-syntax-prep (class-name base-class))))))
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
		(flet ((op (prefix table)
			 (awhen (generate-component table
						    #'(lambda (slot)
							(slot-to-go new slot)))
			   (process-many-to-one-component prefix self))))
		  (when to-insert
		    (loop
		      for table in to-insert
		      do (with-active-layers (insert-table)
			   (op :insert table))))
		  (when to-delete
		    (aif (member-if #'(lambda (class)
					(eq (class-name class)
					    (car (slot-value base-class 'tables))))
				    to-delete)
			 (with-active-layers (delete-table)
			   (op :delete (car self)))
			 (loop
			   for table in to-delete
			   do (with-active-layers (delete-table)
				(op :delete table)))))))
	      (setf sql-list (nreverse (push "RETURN;" sql-list))))))))
    procedure))



;;; Delete and update operations being procedures do not
;;; support multiple return values. Thus, any delete operation where
;;; the primary key values are absent must have no return values or
;;; PG will return an error.

;;; TODO
;;; Something could be done about this, maybe return the number of
;;; rows affected. Better yet(?), upstream, when the error is invoked,
;;; add a restart viewing the affected nodes before deleting/updating.

;;; Reason for not using a function??? Rollback support to be added
;;; in the near future?

;;; Should the object OLD be destroyed automatically or leave that
;;; to the discretion of the user?

(define-layered-function collate-tables (old new)
  (:method
      :in update-node ((old serialize) (new serialize))
    (loop
      with where = nil
      with set = nil
      for slot in (filter-slots-by-type (class-of old) 'db-column-slot-definition) 
      for slot-name = (slot-definition-name slot)
      for old-value = (slot-value old slot-name)
      for new-value = (slot-value new slot-name)
      for table = (slot-value slot 'table-class)
      when old-value
	do (pushnew table where :test #'eq)
      when new-value
	do (pushnew table set :test #'eq)
      finally (return (values set where)))))



(define-layered-function one-to-one-update-components (old new)
  (:documentation "Build components based on slots of type 
db-column-slot-definition.")

  (:method
      :in update-node ((old serialize) (new serialize))
    (let ((base-class (class-of old))
	  (to-insert)
	  (to-update)
	  (to-delete)
	  (set)
	  (where))
      (multiple-value-bind (table-set table-where)
	  (collate-tables old new)
	(labels ((parse-slots (slots)
		   (when slots
		     (let* ((slot (car slots))
			    (slot-name (slot-definition-name slot))
			    (old-value (slot-value old slot-name))
			    (new-value (unless (lock-value slot)
					 (slot-value new slot-name)))
			    (exceptionp (lambda (slot value)
					  (with-slots (col-type not-null) slot
					    (and (null value)
						 (or (eq col-type :boolean)
						     (eq not-null nil))))))
			    (table (slot-value slot 'table-class)))
		       (when old-value
			 (push (cons slot old-value) where)
			 (unless (and (or new-value
					  (funcall exceptionp slot new-value))
				      (member table table-set :test #'eq))
			   (pushnew table to-delete :test #'eq)))
		       (when (or new-value
				 (funcall exceptionp slot new-value))
			 (if (and (or old-value
				      (funcall exceptionp slot old-value))
				  (member table table-where :test #'eq))
			     (unless (or (equal new-value old-value)
					 (lock-value slot))
			       (pushnew table to-update :test #'eq)
			       (push (cons slot new-value) set))
			     (pushnew table to-insert :test #'eq)))
		       (parse-slots (cdr slots))))))
	  (parse-slots (filter-slots-by-type base-class 'db-column-slot-definition)))
	(values to-update where set to-insert to-delete)))))


(define-layered-function one-to-many-update-components (old new)
  (:documentation "Build components based on slots of type 
db-aggregate-slot-definition.")

  (:method
      :in update-node ((old serialize) (new serialize))
    (loop
      for slot in (filter-slots-by-type (class-of old) 'db-aggregate-slot-definition)
      for slot-name = (slot-definition-name slot)
      for old-values = (slot-value old slot-name)
      for new-values = (slot-value new slot-name)
      for to-delete = (set-difference old-values new-values :test #'equal)
      for to-insert = (set-difference new-values old-values :test #'equal)
      for maps = (slot-value slot 'maps)
      for mapped-table = (mapped-table maps)
      for insertion = (when to-insert
			(with-active-layers (insert-table)
			  (generate-component maps (lambda (slot%)
						     (slot-to-go new slot%)))))
      for deletion = (when to-delete
		       (with-active-layers (delete-table)
			 (generate-component maps (lambda (slot%)
						    (slot-to-go old slot%)))))
      when insertion
	collect :insert into components%
	and collect insertion into components%
      when deletion
	collect :delete into components%
	and collect deletion into components%
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
				 collect (prepare-value% column value))
			       (loop
				 for value in (set-difference old-values new-values :test #'equal)
				 collect (prepare-value% column value))))
	       else 
		 collect (prepare-value% slot
					 (slot-value (if (or (eq symbol :old)
							     (eq symbol :delete))
							 old
							 new)
						     (slot-definition-name slot))))))))



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
  (with-slots (schema table primary-keys) class
    (let ((single-row (loop
			for slot in primary-keys
			always (let ((allocation (funcall allocate slot)))
				 (or (eq allocation :where)
				     (eq allocation t))))))
      (loop
	for slot in (filter-slots-by-type class 'db-base-column-definition)
	for allocation = (funcall allocate slot)
	when (or (eq allocation :set)
		 (eq allocation t))
	  collect (column-name slot) into set-columns
	  and collect (update-param slot "set" single-row) into set-params
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
		     :where-controls where-controls)))))))



(define-layered-function update-param (slot prefix &optional single-row)
  (:method
      :in update ((slot db-column-slot-definition) prefix &optional (single-row t))
    (with-slots (schema column-name domain) slot
      (if (string= prefix "set")
	  (list (if single-row :inout :in)
		(format nil "~a_~a" prefix column-name)
		(set-sql-name schema domain))
	  (list (set-sql-name schema domain))))))


(define-layered-function update-param-control (slot)
  (:method
      :in update ((slot db-column-slot-definition))
    (list (format nil "~~a::~a"
		  (set-sql-name (slot-value slot 'schema) (slot-value slot 'domain)))
	  slot)))
