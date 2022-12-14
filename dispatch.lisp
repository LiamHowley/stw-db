(in-package stw.db)


(defmacro with-aggregate-slot (slot &body body)
  `(when (typep ,slot 'db-aggregate-slot-definition)
     (let* ((map (slot-value ,slot 'maps))
	    (column (mapped-column map))
	    (columns (mapped-columns map)))
       ,@body)))


(defun proc-id (list)
  (db-syntax-prep
   (write-to-string
    (make-v3-uuid +namespace-oid+ (write-to-string list)))))


(define-layered-function (setf proc-template) (new-value class component &rest rest &key &allow-other-keys)
  (:documentation "Cache procedure in hash-table. Hash table is context dependent and derived from
calling db-template-register.")

  (:method
      :in-layer db-op (new-value (class serialize) component &rest rest &key &allow-other-keys)
    (let ((key (apply #'get-key class component rest)))
      (setf (gethash key (db-template-register)) new-value))))


(define-layered-function proc-template (class component &rest rest &key &allow-other-keys)
  (:documentation "Retrieve procedure from cache. Cache (hash table) is context dependent and derived from
calling db-template-register.")

  (:method
      :in db-layer :around ((class serialize) component &rest rest &key &allow-other-keys)
    (multiple-value-bind (procedure existsp)
	(call-next-method)
      (when (and existsp procedure)
	procedure)))

  (:method
      :in-layer db-op ((class serialize) component &rest rest &key &allow-other-keys)
    (gethash (apply #'get-key class component rest) (db-template-register))))


(define-layered-function get-key (class component &rest rest &key &allow-other-keys)
  (:documentation "Get key for cache store. Key is context dependent.")

  (:method 
      :in db-op ((class serialize) component &rest rest &key)
    (let ((slots (nth-value 1 (slots-with-values class))))
      (push slots rest)))

  (:method
      :in db-op ((class serialize) (component db-table-class) &rest rest &key)
    (class-name component)))



(define-layered-function db-template-register ()
  (:documentation "Context dependent cache stores for procedures.")

  (:method
      :in-layer insert ()
    (template-register (contextl:find-layer 'insert)))

  (:method
      :in-layer delete-from ()
    (template-register (contextl:find-layer 'delete-from)))

  (:method
      :in-layer update ()
    (template-register (contextl:find-layer 'update)))

  (:method
      :in-layer retrieve ()
    (template-register (contextl:find-layer 'retrieve))))



(define-layered-function dispatch-statement (class procedure)
  (:documentation "Build dispatching statement from list of relevant-slots
and the control string p-control. Values are obtained from serialize.")

  (:method
      :in db-layer ((class serialize) (procedure procedure))
    (with-slots (p-control relevant-slots) procedure
      (apply #'format nil p-control
	     (nreverse
	      (labels ((walk (inner &optional acc)
			 (cond ((null inner)
				acc)
			       ((atom inner)
				(let* ((slot-name (slot-definition-name inner))
				       (value (slot-value class slot-name))
				       (result (prepare-value% inner value)))
				  (cond (result
					 (cons result acc))
					(t acc))))
			       (t (walk (cdr inner) (walk (car inner) acc))))))
		(walk relevant-slots)))))))


(define-layered-function escape (slots)
  (:method
      :in db-layer ((slots cons))
    (unless (> (length slots) 1)
      t))
  (:method
    :in retrieve-node ((slots cons))
    nil))


(define-layered-function read-row-to-class (class)
  (:documentation "Generate row-reader that assigns returned values
to class(es).")

  (:method
      :in db-layer ((class serialize))
    (flet ((get-symbol-name (field)
	     (string-upcase (substitute #\- #\_ (string-left-trim '(#\_) field)))))
      (row-reader (fields)
	(let ((base-class (class-of class)))
	  (loop
	    while (next-row)
	    for i from 0
	    for node = (if (eql i 0) class (make-instance (class-of class)))
	    do (loop
		 for field across fields
		 for symbol-name = (get-symbol-name (field-name field))
		 for slot-name = (intern symbol-name (symbol-package (class-name base-class)))
		 do (let ((next-field (next-field field)))
		      (unless (eq next-field :null)
			(awhen (find-slot-definition base-class slot-name 'db-base-column-definition)
			  (etypecase self
			    (db-column-slot-definition
			     (setf (slot-value node slot-name) next-field))
			    (db-aggregate-slot-definition
			     (with-aggregate-slot self
			       (cond (column
				      (let ((slot-type (slot-definition-type self)))
					(setf (slot-value node slot-name)
					      (if (or (eq slot-type 'list)
						      (eq slot-type 'cons))
						  (array-to-list next-field)
						  next-field))))
				     (columns
				      (let ((type (slot-value self 'express-as-type)))
					(setf (slot-value node slot-name)
					      (parse-result self columns type next-field))))))))))))
	    collect node into nodes
	    finally (return (if (eql i 0) node nodes))))))))


(define-layered-function parse-result (slot columns type result)
  (:documentation "PARSE-RESULT is invoked when a result of aggregated 
values is returned in json format.")

  (:method
      :in retrieve-node 
      :around ((slot db-aggregate-slot-definition) (columns cons) (type (eql :alist)) result)
    (let ((type (slot-definition-type slot))
	  (parsed-results (call-next-method)))
      (if (eq type 'array)
	  (make-array (length parsed-results) :initial-contents parsed-results :adjustable t :fill-pointer t)
	  parsed-results)))

  (:method
      :in db-layer ((slot db-aggregate-slot-definition) (columns cons) type result)
    ;; default to alist
    (declare (ignore type))
    (parse-result slot columns :alist result))

  (:method
      :in db-layer ((slot db-aggregate-slot-definition) (columns cons) (type (eql :alist)) result)
    (declare (ignore slot type))
    (let ((list (json-array-to-list result)))
      (loop
	for row in list
	collect (loop
		  for result in row
		  for column in columns
		  for slot-name = (slot-definition-name column)
		  when (and slot-name result)
		    collect (cons slot-name result)))))

  (:method
      :in db-layer ((slot db-aggregate-slot-definition) (columns cons) (type (eql :plist)) result)
    (declare (ignore slot type))
    (let ((list (json-array-to-list result)))
      (loop
	for row in list
	collect (loop
		  for result in row
		  for column in columns
		  for key = (car (slot-definition-initargs column))
		  when (and key result)
		    collect key 
		    and collect result))))

  (:method
      :in db-layer ((slot db-aggregate-slot-definition) (columns cons) (type (eql :list)) result)
    (declare (ignore slot columns type))
    (json-array-to-list result))

  (:method
      :in db-layer ((slot db-aggregate-slot-definition) (columns cons) (type (eql :array)) result)
    (declare (ignore slot columns type))
    (let ((list (json-array-to-list result)))
      (loop
	for row in list
	collect (make-array (length row) 
			    :initial-contents (loop
						for result in row
						when result
						  collect result))))))


(defun json-array-to-list (array)
  (loop
    for row in (explode-string array '("[[" "]]" "], [") :remove-separators t)
    collect (explode-string row '(", " #\") :remove-separators t)))
    


(define-layered-function db-error-handler (err class component procedure)
  (:documentation "Main handler function for database errors. Returns a closure that takes one
argument which is required to be function and is called after error resolution has occurred. 
Provides resolutions for missing schemas, types, tables,and constraints.")

  (:method
      :in db-layer ((err database-error) class component (procedure procedure))
    (let* ((base-class (class-of class))
	   (schema (slot-value base-class 'schema))
	   (class-name (db-syntax-prep (class-name base-class))))
      (labels ((exec (statement)
		 (exec-query *db* statement))
	       (respond (proc-name &rest functions)
		 (multiple-value-bind (statement procedure)
		     (apply #'build-db-component
			    (or (when (typep component 'db-table-class)
				  component)
				base-class)
			    proc-name functions)
		   (exec statement)
		   (exec (slot-value procedure 'p-control)))))
	#'(lambda (query-function)
	    (scase (database-error-code err)
		   ("0A" (unless (> (get-postgresql-version *db*) 13)
			   (error "Postgresql version 14 or higher required for STW-DB procedures.")))
		   ("3F000"
		    ;; MISSING SCHEMA
		    ;; Response: Create schema and recurse
		    (exec (create-schema schema))
		    (exec (set-schema schema))
		    (funcall query-function))
		   ("42704"
		    ;; MISSING TYPE
		    ;; Response: Create types for all relevant
		    ;; table inserts. 
		    (let ((proc-name (format nil "initialize_~(~a~)_types" class-name)))
		      (respond proc-name #'create-pg-composite #'create-typed-domain))
		    (funcall query-function))
		   ("42P01"
		    ;; MISSING TABLE OR TYPE IN DB.
		    ;; Response: rebuild database component and
		    ;; (re)initialize database.
		    ;; Note: An effective way to build a database is to
		    ;; let an insert fail and thus invoke this response.
		    (let ((proc-name (format nil "initialize_~(~a~)_relations" class-name)))
		      (respond proc-name #'create-table-statement #'foreign-keys-statements #'index-statement))
		    (funcall query-function))
		   ("42883"
		    ;; MISSING PROCEDURE
		    ;; Response: Make procedure and recurse.
		    (exec (sql-statement procedure))
		    (funcall query-function))
		   (t (error err))))))))


(define-layered-function sync (class component &rest rest &key &allow-other-keys)
  (:documentation "Generates and syncs a database query according to the layered context, 
class, table component and other args. Specifying a table component results in insert/delete 
queries that operate only on that table/component, otherwise, all tables are queried. 
To update, create a clone of class, update relevant values in the clone, and pass the 
clone as the component parameter. Procedures/functions are generated by calling on 
GENERATE-PROCEDURE, and the resulting procedure is cached. To update the cache, set 
the refresh-cache keyword to T. This will overwrite any existing stored procedure/function
of the same name and arg types. Database errors as they occur are passed to the DB-ERROR-HANDLER 
function, which returns a closure that accepts a function to be called when resolution of the error
has occurred. The database, types, tables and relations can be easily constructed by simply invoking
sync on an object in the INSERT-NODE context layer.")

  (:method
      :in-layer db-layer
      :around ((class serialize) component &rest rest &key)
    (multiple-value-bind (statement procedure)
	(call-next-method)
      (handler-case (exec-query *db* statement (read-row-to-class class))
	(database-error (err)
	  (funcall (db-error-handler err class component procedure)
		   #'(lambda () (apply #'sync class component rest)))))))
  
  (:method
      :in-layer db-layer ((class serialize) component &rest rest &key refresh-cache &allow-other-keys)
    (flet ((update-procedure ()
	     (let ((procedure (apply #'generate-procedure class component rest)))
	       (replace-procedure class component procedure)
	       (setf (apply #'proc-template class component rest) procedure))))
      (let* ((procedure (or (cond (refresh-cache
				   (remf rest :refresh-cache)
				   (update-procedure))
				  (t
				   (apply #'proc-template class component rest)))
			    (update-procedure)))
	     (dispatcher (dispatcher class component procedure)))
	(values (apply (car dispatcher) (cdr dispatcher))
		procedure)))))


(define-layered-function replace-procedure (class component procedure)
  (:method
      :in-layer db-layer ((class serialize) component (procedure procedure))
    (handler-case (exec-query *db* (sql-statement procedure))
      (database-error (err)
	(funcall (db-error-handler err class component procedure)
		 #'(lambda () (replace-procedure class component procedure)))))))



(define-layered-function dispatcher (class component procedure)
  (:documentation "Returns layered dispatching function and relevant args in a list.")

  (:method
      :in db-layer ((class serialize) component (procedure procedure))
    (declare (ignore component))
    `(dispatch-statement ,class ,procedure)))


(define-layered-function match-mapping-node (class table/slot)
  (:documentation "Confirms class maps table, and returns the 
relevant instance of SLOT-MAPPING.")

  (:method
    :in-layer db-layer ((class db-interface-class) table/slot)
    (loop
      for mapping in (slot-value table/slot 'mapped-by)
      for mapping-node = (mapping-node mapping)
      when (or (eq mapping-node class)
	       (find-class-precedent mapping-node (class-name class) 'db-interface-class))
	do (return mapping))))


(define-layered-function prepare-value% (slot value)
  (:documentation "Parse slot for col-type, and call prepare-value.")

  (:method
      :in db-layer ((slot db-column-slot-definition) value)
    (with-slots (col-type not-null) slot
      (let ((col-type (if (consp col-type)
			  (car col-type)
			  col-type)))
	(setf col-type
	      (case col-type
		((:text :varchar :char)
		 :text)
		(t col-type)))
	(prepare-value slot col-type value))))

  (:method
      :in db-layer ((slot db-aggregate-slot-definition) values)
    (with-aggregate-slot slot
      (with-slots (express-as-type) slot
	(map 'list #'(lambda (value)
		       (cond (column
			      (prepare-value% column value))
			     (columns
			      (prep-mapped-value columns express-as-type value))
			     (t (error "Mapped column(s) missing from aggregate slot ~a" (slot-definition-name slot)))))
	     values)))))
	 

(define-layered-function prep-mapped-value (columns as-type values)
  (:documentation "Finds values associated with columns, formats the values
for database queries and returns the values in a list.")

  (:method
      :in db-layer ((columns cons) (as-type (eql :alist)) values)
    "Column name mapped to value '((<name> . <value>))"
    (loop
      for column in columns
      for col-name = (slot-definition-name column)
      for result = (assoc col-name values :test #'eq)
      when result
	collect (prepare-value% column (cdr result))))

  (:method
      :in db-layer ((columns cons) (as-type (eql :list)) values)
    "List of values associated with columns."
    (loop
      for column in columns
      for value in values
      collect (prepare-value% column value)))

  (:method
      :in db-layer ((columns cons) (as-type (eql :array)) values)
    "Array of values associated with columns."
    (prep-mapped-value columns :list (array-to-list values))))



(define-layered-function prepare-value (slot col-type value)
  (:documentation "Prepare value for query. Specializes on keyword representation of col-type,
specifically text/character types. While all strings are by default escaped, the use of escape
indicates whether the string is wrapped in double quotes or whether the character E can be used
before the first single quote.")

  (:method
      :in db-layer ((slot db-column-slot-definition) (col-type (eql :text)) (value string))
    (concatenate 'string "E'" value "'"))

  ;;; the rest
  (:method
      :in db-layer ((slot db-column-slot-definition) col-type value)
    (declare (ignore col-type))
    (cond (value (to-sql-string value))
	  ((slot-boundp slot 'default)
	   (to-sql-string (slot-value slot 'default)))
	  (t "null"))))
