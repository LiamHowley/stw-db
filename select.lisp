(in-package stw.db)


(defstruct select-component
  alias
  columns
  from
  join
  where)

(define-layered-class select 
  :in db-layer ()
  ((col-names :initarg :col-names :initform nil :reader col-names)
   (aggregate :initarg :aggregate :initform nil :reader aggregate)
   (from :initarg :from :initform nil)
   (joins :initarg :joins :initform nil)
   (where :initarg :where :initform nil :reader where)
   (having% :initarg :having :initform nil :reader having%)
   (order-by :initarg :order-by :initform nil :reader order-by)
   (group-by :initarg :group-by :initform nil :reader group-by)
   (limit :initarg :limit :initform nil)))


(define-layered-class agg
  :in db-layer (select)
  ((alias :initarg :alias :initform nil)))


(define-layered-class array-agg
  :in db-layer (agg)
  ())


(define-layered-class json-agg
  :in db-layer (agg)
  ())


(define-layered-class optional-select
  :in db-layer (select)
  ())


(define-layered-class join
  :in db-layer ()
  ((table :initarg :table :initform nil :reader table)
   (join-type :initarg :join-type :initform :left :reader join-type)
   (on :initarg :on :initform nil :reader on)))


(define-layered-class union-query
  :in db-layer ()
  ((col-names :initarg :col-names :initform nil :reader col-names)
   (tables :initarg :tables :initform nil :reader tables)
   (queries :initarg :queries :initform nil :reader queries)
   (alias :initform :uw :reader alias)
   (where :initarg :where :initform nil :reader where)))


(define-layered-class union-all-query
  :in db-layer (union-query)
  ())


(define-layered-method read-row-to-class
  :in retrieve-node ((class serialize))
  (call-next-method))


(define-layered-method get-key
  :in retrieve-node
  ((class serialize) component
   &rest rest &key &allow-other-keys)
  (let ((slots (nth-value 1 (slots-with-values class
					       :type 'db-base-column-definition
					       :filter-if #'(lambda (slot)
							      (when (typep slot 'db-column-slot-definition)
								(date/time-p (slot-value slot 'col-type))))))))
    (push slots rest)))


(define-layered-method sync
  :in retrieve-node
  ((class serialize) component &rest rest 
   &key optional-join union-queries union-all-queries having group-by (order-by (mapcar #'column-name (get-root-key (class-of class)))) limit)
  (apply #'call-next-layered-method class component rest))


(define-layered-method generate-procedure
  :in-layer retrieve-node
  ((class serialize) component &rest rest
   &key optional-join union-queries union-all-queries order-by limit select-columns ignore-tables where)
  (declare (ignore component))

  ;; During insert, delete and update operations, create procedure statements are
  ;; composed of parts/expressions derived during calls to GENERATE-COMPONENTS.
  ;; In a select/retrieval operation, the select statement is composed of
  ;; clauses/parts derived from GENERATE-COMPONENTS concatenated 
  ;; into a discrete expression and then composed into a function expression.

  ;; This method is awful clunky and could maybe be broken up into
  ;; constituent parts. It's basically a controller so relies on having
  ;; lots of information and passing said information on.
  (macrolet ((test-queries (query-type)
	       `(when (eql (length ,query-type) 1)
		  (warn "Ignoring ~a. Contains only one table." ',query-type)
		  (setf ,query-type nil))))
    (test-queries union-queries)
    (test-queries union-all-queries))

  (multiple-value-bind (slots-with-values slot-names)
      (slots-with-values class
			 :type 'db-base-column-definition
			 :filter-if #'(lambda (slot)
					(when (typep slot 'db-column-slot-definition)
					  (date/time-p (slot-value slot 'col-type)))))
    (let* ((base-class (class-of class))
	   (schema (slot-value base-class 'schema))
	   (tables (tables base-class))
	   (select (make-instance 'select
				  :order-by order-by
				  :limit limit))
	   (slot-value-p #'(lambda (slot)
			     (flet ((valuep (slot)
				      (when slot
					(awhen (position (slot-definition-name slot) slot-names)
					  (values (1+ self) slot)))))
			       (valuep
				(etypecase slot
				  (db-column-slot-definition
				   (aif (match-mapping-node base-class slot)
					(mapping-slot self)
					slot))
				  (db-aggregate-slot-definition
				   slot)))))))
      (multiple-value-bind (where% positions)
	  (infix-where-clause where
			      #'(lambda (slot-name)
				  (if (member slot-name slot-names :test #'eq) 
				      (let* ((position (position slot-name slot-names :test #'eq))
					     (slot (nth position slots-with-values)))
					(values (set-sql-name schema (column-name slot)) position))
				      slot-name)))
	(multiple-value-bind (components returns)
	    (apply #'generate-components base-class
		   :slot-value-p slot-value-p
		   rest)
	  (let ((db-function (make-instance 'db-function
					    :schema schema
					    :name (format nil "~a_retrieve_~a"
							  (db-syntax-prep (class-name base-class))
							  (proc-id `(,@slot-names ,@rest)))
					    :vars (if select-columns
						      (mapcar #'(lambda (column)
								  (return-var column))
							      select-columns)
						      returns)
					    :relevant-slots slots-with-values)))
	    (with-slots (args sql-query p-controls relevant-slots) db-function
	      (loop 
		for slot in slots-with-values
		collect (input-arg slot) into args%
		collect (input-control slot) into p-controls%
		finally (setf args args%
			      p-controls p-controls%))

	      ;; build select statement
	      (with-slots (col-names from joins having% order-by% group-by% where) select
		;; tables
		(flet ((process-component (component)
			 (macrolet ((nconc% (analogue part)
				      `(setf ,analogue (nconc ,analogue (ensure-list ,part)))))
			   (with-slots (columns join) component
			     (nconc% from (select-component-from component))
			     (nconc% joins join)
			     (awhen (select-component-where component)
			       (push self where))
			     (if select-columns
				 (setf col-names (mapcar
						  #'(lambda (column)
						      (let* ((table-class column)
							     (table (table table-class))
							     (column-name (column-name column)))
							(set-sql-name table column-name)))
						  select-columns))
				 (nconc% col-names columns))))))

		  (when union-queries
		    (process-component (gethash union-queries components)))
		  (when union-all-queries
		    (process-component (gethash union-all-queries components)))
		  (loop
		    for table in tables
		    for component = (gethash table components)
		    unless (or (member table optional-join :test #'eq)
			       (member table ignore-tables :test #'eq))
		      do (process-component component)
		    finally (loop
			      for optional in optional-join
			      for component = (gethash optional components)
			      do (process-component component))))

		;; where
		(flet ((slot-relevant-p (slot)
			 (and (typep slot 'db-column-slot-definition)
			      (notany #'(lambda (table)
					  (map-filtered-slots
					   (find-class table)
					   #'(lambda (slot)
					       (typep slot 'db-column-slot-definition))
					   #'(lambda (slot%)
					       (eq (slot-definition-name slot%)
						   (slot-definition-name slot)))))
				      (append union-queries union-all-queries)))))
		  (setf where (nconc where
				     (loop
				       for i from 0
				       for slot in slots-with-values
				       for clause = (when (slot-relevant-p slot)
						      (unless (member i positions)
							(wherep slot slot-value-p)))
				       when clause
					 collect clause into where%%
				       finally (return (if where%
							   `(,@where%% ,where%)
							   where%%))))
			sql-query (concatenate 'string (statement select) ";")))))
	    db-function))))))

;; (notany #'(lambda (table)
;;	     (map-filtered-slots
;;	      (find-class table)
;;	      #'(lambda (slot)
;;		  (typep slot 'db-column-slot-definition))
;;	      #'(lambda (slot%)
;;		  (or (eq slot% slot)
;;		      (with-aggregate-slot slot
;;			(or (eq slot% column)
;;			    (member slot% columns :test #'eq)))))))

(define-layered-function return-var (slot)

  (:method
      :in retrieve-node ((slot db-column-slot-definition))
    (let* ((col-type (slot-value slot 'col-type))
	   (col-type (if (eq col-type :serial) :integer col-type)))
      (list (column-name slot) col-type)))

  (:method
      :in retrieve-node ((slot db-aggregate-slot-definition))
    (with-aggregate-slot slot
      (list (db-syntax-prep (slot-definition-name slot))
	    (cond (column
		   (format nil "~a[]" (col-type column)))
		  (columns
		   "JSON")
		  (t (error "No mapped column(s) in slot ~a" (slot-definition-name slot))))))))


(define-layered-function input-arg (slot)

  (:method
      :in retrieve-node ((slot db-column-slot-definition))
    (let ((schema (slot-value slot 'schema)))
      (list :in (format nil "_~a" (column-name slot)) (set-sql-name schema (domain slot)))))

  (:method
      :in retrieve-node ((slot db-aggregate-slot-definition))
    (with-aggregate-slot slot
      (let ((table-class (mapped-table map)))
	(cond (column
	       (list :in
		     (format nil "_~a" (db-syntax-prep (slot-definition-name slot)))
		     (format nil "~a[]" (col-type column))))
	      (columns
	       (with-slots (schema table) table-class
		 (list :in
		       (format nil "_~a" (db-syntax-prep (slot-definition-name slot)))
		       (format nil "~a.~a_type[]" schema table))))
	      (t (error "No mapped column(s) in slot ~a" (slot-definition-name slot))))))))


(define-layered-function input-control (slot)

  (:method
      :in retrieve-node ((slot db-column-slot-definition))
    (with-slots (schema domain) slot
      (list (format nil "~~a::~a" (set-sql-name schema domain)) slot)))

  (:method
      :in retrieve-node ((slot db-aggregate-slot-definition))
    (with-aggregate-slot slot
      (cond (column
	     (list "ARRAY[~{~a~^, ~}]" column))
	    (columns
	     (list (sql-typed-array map)))
	    (t (error "No mapped column(s) in slot ~a" (slot-definition-name slot)))))))


(define-layered-function wherep (slot function)

  (:method
      :in db-layer ((slot db-column-slot-definition) (slot-value-p function))
    (awhen (funcall slot-value-p slot)
      (with-slots (schema table column-name) slot
	(format nil "~a = $~a" (set-sql-name schema table column-name) self))))

  (:method
      :in db-layer ((slot db-aggregate-slot-definition) (slot-value-p function))
    (with-aggregate-slot slot
      (let* ((table (mapped-table map))
	     (table-name (set-sql-name (schema table) (table table))))
	(awhen (funcall slot-value-p slot)
	  (cond (column
		 (format nil "~a IN (SELECT UNNEST ($~a))" (set-sql-name table-name (column-name column)) self))
		(columns
		 (let ((column-names (mapcar #'column-name columns))
		       (where (mapcar #'(lambda (column)
					  (let ((col-name (column-name column)))
					    (format nil "~a = ~a.~a" col-name table-name col-name)))
				      columns)))
		   (format nil "EXISTS (SELECT ~{~a~^, ~} FROM UNNEST ($~a) WHERE~{ ~a~^ AND~}"
			   column-names self where)))
		(t (error "No mapped column(s) in slot ~a" (slot-definition-name slot)))))))))



(define-layered-method generate-components
  :in-layer retrieve-node
  ((class db-interface-class) &key optional-join union-queries union-all-queries slot-value-p ignore-tables &allow-other-keys)
  (let* ((tables (tables class))
	 (schema (schema class))
	 (components (make-hash-table :test #'equal :size (length tables)))
	 (last)
	 (return-columns))

    (flet ((process-union (query-type tables)
	     (let* ((union (make-union (make-instance query-type :tables tables)
				       slot-value-p))
		    (component (generate-component union nil :join-to last)))
	       (setf (gethash tables components) component
		     last (lambda (slot)
			    (when (member (db-syntax-prep slot) (slot-value union 'col-names) :test #'string=)
			      (slot-value union 'alias))))))
	   (process-table (table join-type)
	     (multiple-value-bind (table-class component return-columns%)
		 (generate-component 
		  (or (match-mapping-node class (find-class table))
		      (find-class table))
		  #'(lambda (f-key)
		      (with-slots (ref-table no-join) f-key
			(unless no-join
			  (member ref-table tables :test #'eq))))
		  :join-to last
		  :join-type join-type
		  :slot-value-p #'(lambda (slot)
				    (unless (or (member table union-queries :test #'eq)
						(member table union-all-queries :test #'eq))
				      (funcall slot-value-p slot))))
	       (setf (gethash table-class components) component
		     return-columns (nconc return-columns return-columns%))
	       (unless (eq join-type :left)
		 (setf last (lambda (slot)
			      (when (find-slot-definition (find-class table) slot 'db-column-slot-definition)
				(or (slot-value component 'alias)
				    (set-sql-name schema table)))))))))

      (when union-queries
	(process-union 'union-query union-queries))
      (when union-all-queries
	(process-union 'union-all-query union-all-queries))

      (loop
	for table in tables
	unless (or (member table optional-join :test #'eq)
		   (member table ignore-tables :test #'eq))
	  do (process-table table :inner)
	finally (loop
		  for optional in optional-join
		  do (process-table optional :left))))

    (values components return-columns)))



(define-layered-function make-union (union function)
  (:method
      :in retrieve-node ((union union-query) (slot-value-p function))
    (with-slots (alias tables col-names queries) union
      (setf alias (gensym "UNION")
	    col-names (reduce
		       #'(lambda (list1 list2)
			   (intersection list1 list2 :test #'string-equal))
		       (loop
			 for table in tables
			 collect (mapcan #'(lambda (name)
					     (list (db-syntax-prep (slot-definition-name name))))
					 (filter-slots-by-type (find-class table) 'db-column-slot-definition))))
	    queries (loop
		      for table in tables
		      collect (union-select (find-class table) col-names slot-value-p)))
      union)))


(define-layered-function union-select (table column-names function)
  (:method
      :in retrieve-node ((table db-table-class) (column-names cons) (slot-value-p function))
    (with-slots (schema) table
      (let ((select (make-instance 'select))
	    (table-name (table table)))
	(with-slots (col-names from where) select 
	  (setf from (set-sql-name schema table-name)
		col-names (mapcar #'(lambda (column)
				      (set-sql-name table-name column))
				  column-names)
		where (loop
			for column in (filter-slots-by-type table 'db-column-slot-definition)
			for slot = (nth-value 1 (funcall slot-value-p column))
			when slot
			  collect (wherep slot slot-value-p))))
	(statement select)))))


(define-layered-method generate-component
  :in retrieve-node ((class union-query) function &key join-to)
  (declare (ignore function join-to))
  (with-slots (tables alias col-names) class
    (make-select-component
     :from (clause class)
     :alias alias)))


(define-layered-method generate-component
  :in retrieve-node ((class db-key-table) function &key join-to slot-value-p)
  (declare (ignore function))
  (with-slots (schema table) class
    (let* ((slot (car (filter-slots-by-type class 'db-column-slot-definition)))
	   (table-name (set-sql-name schema table))
	   (col-name (column-name slot)))
      (values
       (class-name class)
       (make-select-component
	:columns (list (set-sql-name table-name col-name))
	:from table-name
	:where (when join-to
		 (awhen (funcall join-to (slot-definition-name slot))
		   (list (set-sql-name self col-name) := (set-sql-name table col-name)))))
       (list (return-var slot))))))


(define-layered-method generate-component
  :in retrieve-node
  ((class slot-mapping) (f-key-p function)
   &key join-to (join-type :inner) slot-value-p)
  (let* ((join (make-instance 'join :join-type join-type))
	 (mapped-table (mapped-table class))
	 (mapped-column (mapped-column class))
	 (mapped-columns (mapped-columns class))
	 (mapping-slot% (mapping-slot class))
	 (mapping-slot (db-syntax-prep (slot-definition-name mapping-slot%)))
	 (schema (schema mapped-table))
	 (slots (filter-slots-by-type mapped-table 'db-column-slot-definition))
	 (table-name (set-sql-name schema (table mapped-table)))
	 (f-keys)
	 (columns (loop
		    with collected-slots = nil
		    for slot in slots
		    for f-key = (slot-value slot 'foreign-key)
		    for column = (cond ((eq slot mapped-column)
					mapping-slot)
				       ((member slot mapped-columns :test #'eq)
					(unless (member mapping-slot collected-slots :test #'eq)
					  (push mapping-slot collected-slots)
					  mapping-slot))
				       (t (column-name slot)))
		    if (and f-key (funcall f-key-p f-key))
		      do (push f-key f-keys)
		    else
		      when column
			collect (set-sql-name mapping-slot column)))
	 (last-key)
	 (table-clause))

    ;; When values are bound to mapping slot in class mapping-node 
    ;; and mapped-table is not invoked in a union subquery,
    ;; any retrieved records must validate against at least one of
    ;; of the supplied values. 
    (when (funcall slot-value-p mapping-slot%)
      (let ((group-by (mapcar #'(lambda (f-key)
				  (set-sql-name table-name (slot-value f-key 'key)))
			      f-keys)))
	(setf last-key (concatenate 'string (db-syntax-prep mapping-slot) "_key"))
	(with-slots (table on) join
	  (setf table (clause
		       (make-instance 'agg
				      :alias last-key
				      :from table-name
				      :group-by group-by
				      :where (wherep mapping-slot% slot-value-p)))
		on (loop
		     for f-key in f-keys
		     for key = (slot-value f-key 'key)
		     for self = (funcall join-to key)
		     when self
		       collect `(,(set-sql-name self key)
				 ,(set-sql-name last-key key)))
		table-clause (clause join)))))

    (with-slots (table on) join
      (setf table 
	    (clause
	     (cond (mapped-column
		    (make-instance 'array-agg
				   :alias mapping-slot
				   :from table-name
				   :col-names (ensure-list (set-sql-name table-name (slot-definition-name mapped-column)))
				   :group-by (mapcan #'(lambda (slot)
							 (unless (eq slot mapped-column)
							   (list (set-sql-name table-name (column-name slot)))))
						     slots)))
		   (mapped-columns
		    (make-instance 'json-agg
				   :alias mapping-slot
				   :from table-name
				   :col-names (mapcar #'(lambda (column)
							  (set-sql-name table-name (column-name column)))
						      mapped-columns)
				   :group-by (mapcan #'(lambda (slot)
							 (unless (member slot mapped-columns :test #'eq)
							   (list (set-sql-name table-name (column-name slot)))))
						     slots)))))
	    on (loop
		 for f-key in f-keys
		 for key = (slot-value f-key 'key)
		 for self = (if table-clause
				last-key
				(funcall join-to key))
		 collect `(,(set-sql-name self key)
			   ,(set-sql-name mapping-slot key))))
      (values
       (class-name mapped-table)
       (make-select-component
	:alias mapping-slot
	:columns columns
	:join (if table-clause
		  (concatenate 'string table-clause " " (clause join))
		  (clause join)))
       (list (return-var (mapping-slot class)))))))



(define-layered-method generate-component
  :in retrieve-node ((class db-table-class) (f-key-p function) &key (join-to (constantly nil)) (join-type :inner) slot-value-p)
  (declare (ignore slot-value-p))
  (with-slots (schema table) class
    (let* ((slots (filter-slots-by-type class 'db-column-slot-definition))
	   (table-name (set-sql-name schema table))
	   (f-keys)
	   (join (make-instance 'join :join-type join-type)))
      (multiple-value-bind (columns returns)
	  (loop
	    for slot in slots
	    for f-key = (slot-value slot 'foreign-key)
	    if (and f-key (funcall f-key-p f-key))
	      do (push f-key f-keys)
	    else
	      collect (set-sql-name table-name (column-name slot)) into columns%
	      and collect (return-var slot) into returns%
	    finally (return (values columns% returns%)))
	(with-slots (table on) join
	  (setf table table-name
		on (loop
		     for f-key in f-keys
		     for key = (slot-value f-key 'key)
		     for self = (funcall join-to key)
		     when self
		     collect `(,(set-sql-name self key)
			       ,(set-sql-name table-name key)))))
	(values
	 (class-name class)
	 (make-select-component
	  :columns columns
	  :join (clause join))
	 returns)))))


(define-layered-method clause
  :in retrieve-node ((this join))
  (with-slots (join-type table on) this
    (format nil "~a JOIN ~a~@[ ON (~{~{~a = ~a~}~^ AND ~})~]"
	    join-type table on)))


(define-layered-method clause
  :in retrieve-node ((this union-query))
  (with-slots (queries alias) this
    (setf queries (format nil "(~{(~a)~^ UNION ~})~@[ ~a~]"
			  queries alias))))


(define-layered-method clause
  :in retrieve-node ((this union-all-query))
  (with-slots (queries alias) this
    (setf queries (format nil "(~{(~a)~^ UNION ALL ~})~@[ ~a~]"
			  queries alias))))


(define-layered-method clause
  :in retrieve-node ((this agg))
  (with-slots (from group-by alias where) this
    (format nil "((SELECT ~{~a~^, ~} FROM ~a~@[ WHERE ~a~] GROUP BY ~{~a~^, ~})) ~a"
	    group-by from where group-by alias)))


(define-layered-method clause
  :in retrieve-node ((this array-agg))
  (with-slots (col-names from group-by alias where) this
    (format nil "((SELECT ARRAY_AGG(~a) AS ~a, ~{~a~^, ~} FROM ~a~@[ WHERE ~{~a~^ AND~}~] GROUP BY ~{~a~^, ~})) ~a"
	    col-names alias group-by from where group-by alias)))


(define-layered-method clause
  :in db-interface-layer ((this json-agg))
  (with-slots (col-names from group-by alias where) this
    (format nil "((SELECT JSON_AGG(JSON_BUILD_ARRAY(~{~a~^, ~})) AS ~a, ~{~a~^, ~} FROM ~{~a~^, ~}~@[ WHERE ~{~a~^ AND~}~] GROUP BY ~{~a~^, ~})) ~a"
	    col-names alias group-by (ensure-list from) where group-by alias)))


(define-layered-method statement
  :in retrieve-node ((this select))
  (with-slots (col-names from joins where group-by having% order-by limit) this
    (when (and col-names from)
      (format nil "SELECT ~{~a~^, ~} FROM ~{~a~^, ~}~@[ ~{~a~^ ~}~]~@[~{~a~^~}~]"
	      col-names
	      (ensure-list from)
	      joins
	      (list (format nil "~@[ GROUP BY ~{~a~^, ~}~]" (ensure-list group-by))
		    (format nil "~@[ WHERE ~{~a~^ AND ~}~]" where)
		    (format nil "~@[ ORDER BY ~{~a~^, ~}~]"
			    (loop for order in (ensure-list order-by)
				  collect (format nil (typecase order
							(atom
							 "~a")
							(cons
							 "~{~a ~a~}"))
						  order)))
		    (format nil "~@[ LIMIT ~a~]" limit))))))
